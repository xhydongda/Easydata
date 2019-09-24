using Arim.Encoding.Binary;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;

namespace Easydata.Engine
{
    ///<summary>
    /// TimeDecoder decodes a byte slice into timestamps.
    /// </summary>
    public class TimeDecoder : IDecoder<long>
    {
        long v;
        int i, n;
        List<ulong> ts;
        Simple8bDecoder dec;

        // The delta value for a run-length encoded byte slice
        long rleDelta;

        byte encoding;
        string err = null;
        public TimeDecoder()
        {
            ts = new List<ulong>();
            dec = new Simple8bDecoder();
        }

        // Init initializes the decoder with bytes to read from.
        public string SetBytes(byte[] b, int startindex, int len)
        {
            err = null;
            v = 0;
            i = 0;
            ts.Clear();
            if (b != null && b.Length > startindex + len)
            {
                // Encoding type is stored in the 4 high bits of the first byte
                encoding = (byte)(b[startindex] >> 4);
            }
            decode(b, startindex, len);
            return null;
        }

        // Next returns true if there are any timestamps remaining to be decoded.
        public bool Next()
        {
            if (err != null)
                return false;
            if (encoding == TimeEncoder.timeCompressedRLE)
            {
                if (i >= n)
                {
                    return false;
                }
                i++;
                v += rleDelta;
                return i < n;
            }

            if (i >= ts.Count)
            {
                return false;
            }
            v = (long)ts[i];
            i++;
            return true;
        }

        // Read returns the next timestamp from the decoder.
        public long Read()
        {
            return v;
        }

        public IClockValue Create(long clock, long value, int quality)
        {
            throw new NotSupportedException("TimeDecoder不支持的方法Create");
        }

        private void decode(byte[] b, int startindex, int len)
        {
            if (b == null || b.Length <= startindex + len)
            {
                return;
            }

            switch (encoding)
            {
                case TimeEncoder.timeUncompressed:
                    decodeRaw(b, startindex + 1, len - 1);
                    break;
                case TimeEncoder.timeCompressedRLE:
                    decodeRLE(b, startindex, len);
                    break;
                case TimeEncoder.timeCompressedPackedSimple:
                    decodePacked(b, startindex, len);
                    break;
                default:
                    err = string.Format("unknown encoding: {0}", encoding);
                    break;
            }
        }

        public void decodePacked(byte[] b, int startindex, int len)
        {
            if (len < 9)
            {
                err = "TimeDecoder: not enough data to decode packed timestamps";
                return;
            }
            ulong div = (ulong)Math.Pow(10, b[startindex] & 0xF);
            ulong first = BitConverter.ToUInt64(b, startindex + 1);

            dec.SetBytes(b, startindex + 9, len - 9);//d.dec.SetBytes(b[9:])

            i = 0;
            ulong[] deltas = ArrayPool<ulong>.Shared.Rent(Constants.DefaultMaxPointsPerBlock);
            int index = 0;
            deltas[index++] = first;

            while (dec.Next())
            {
                deltas[index++] = dec.Read();
            }

            // Compute the prefix sum and scale the deltas back up
            ulong last = deltas[0];
            if (div > 1)
            {
                for (int j = 1; j < index; j++)
                {
                    ulong dgap = deltas[j] * div;
                    deltas[j] = last + dgap;
                    last = deltas[j];
                }
            }
            else
            {
                for (int j = 1; j < index; j++)
                {
                    deltas[j] += last;
                    last = deltas[j];
                }
            }

            i = 0;
            ts.AddRange(deltas.Take(index));
            ArrayPool<ulong>.Shared.Return(deltas);
        }

        private void decodeRLE(byte[] b, int startindex, int len)
        {
            if (len < 9)
            {
                err = "TimeDecoder: not enough data for initial RLE timestamp";
                return;
            }

            int j = startindex, m;

            // Lower 4 bits hold the 10 based exponent so we can scale the values back up
            long mod = (long)Math.Pow(10, (b[j] & 0xF));
            j++;

            // Next 8 bytes is the starting timestamp
            ulong first = BitConverter.ToUInt64(b, j);
            j += 8;

            // Next 1-10 bytes is our (scaled down by factor of 10) run length values
            ulong value;
            m = Varint.Read(b, j, out value);
            if (m <= 0)
            {
                err = "TimeDecoder: invalid run length in decodeRLE";
                return;
            }

            // Scale the value back up
            value *= (ulong)mod;
            j += m;

            // Last 1-10 bytes is how many times the value repeats
            ulong count;
            m = Varint.Read(b, j, out count);
            if (m <= 0)
            {
                err = "TimeDecoder: invalid repeat value in decodeRLE";
                return;
            }

            v = (long)(first - value);
            rleDelta = (long)value;

            i = -1;
            n = (int)count;
        }

        private void decodeRaw(byte[] b, int startindex, int len)
        {
            i = 0;
            int tscount = len / 8;
            ts = new List<ulong>(tscount);
            for (int j = 0; j < tscount; j++)
            {
                ts.Add(BitConverter.ToUInt64(b, startindex + j * 8));

                ulong delta = ts[j];
                // Compute the prefix sum and scale the deltas back up
                if (j > 0)
                {
                    ts[j] = ts[j - 1] + delta;
                }
            }
        }

        public static int CountTimestamps(byte[] b, int startindex, int len)
        {
            if (b == null || b.Length < startindex + len)
            {
                return 0;
            }

            // Encoding type is stored in the 4 high bits of the first byte
            byte encoding = (byte)(b[startindex] >> 4);
            switch (encoding)
            {
                case TimeEncoder.timeUncompressed:
                    // Uncompressed timestamps are just 8 bytes each
                    return (len - 1) / 8;
                case TimeEncoder.timeCompressedRLE:
                    // First 9 bytes are the starting timestamp and scaling factor, skip over them
                    int j = startindex + 9;
                    // Next 1-10 bytes is our (scaled down by factor of 10) run length values
                    ulong value;
                    j += Varint.Read(b, j, out value);
                    // Last 1-10 bytes is how many times the value repeats
                    Varint.Read(b, j, out value);
                    return (int)value;
                case TimeEncoder.timeCompressedPackedSimple:
                    // First 9 bytes are the starting timestamp and scaling factor, skip over them
                    var count = Simple8bDecoder.CountBytes(b, startindex + 9, len - 9);
                    return count.Item1 + 1; // +1 is for the first uncompressed timestamp, starting timestamep in b[1:9]
                default:
                    return 0;
            }
        }

        public string Error()
        {
            return err;
        }
    }
}
