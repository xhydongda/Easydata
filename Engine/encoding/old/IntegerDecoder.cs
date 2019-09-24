using Arim.Encoding.Binary;
using System;

namespace Easydata.Engine
{
    ///<summary>
    /// IntegerDecoder decodes a byte slice into int64s.
    /// </summary>
    public class IntegerDecoder : IDecoder<long>
    {
        ulong[] values;
        byte[] bytes;
        int byteIndex;
        int i, n;
        long prev;
        bool first;
        // The first value for a run-length encoded byte slice
        ulong rleFirst;
        // The delta value for a run-length encoded byte slice
        ulong rleDelta;
        byte encoding;
        int endIndex;
        string err = null;
        public IntegerDecoder()
        {
            //240 is the maximum number of values that can be encoded into a single uint64 using simple8b
            values = new ulong[240];
        }

        // SetBytes sets the underlying byte slice of the decoder.
        public string SetBytes(byte[] b, int startindex, int len)
        {
            if (b != null && b.Length - startindex - len >= 0)
            {
                encoding = (byte)(b[startindex] >> 4);
                bytes = b;
                byteIndex = startindex + 1;//neglect first byte 
                endIndex = startindex + len;
            }
            else
            {
                encoding = 0;
                bytes = null;
                byteIndex = 0;
                endIndex = 0;
            }
            i = 0;
            n = 0;
            prev = 0;
            first = true;

            rleFirst = 0;
            rleDelta = 0;
            err = null;
            return null;
        }

        // Next returns true if there are any values remaining to be decoded.
        public bool Next()
        {
            if (i >= n && bytes == null)
            {
                return false;
            }

            i++;

            if (i >= n)
            {
                switch (encoding)
                {
                    case IntegerEncoder.intUncompressed:
                        decodeUncompressed();
                        break;
                    case IntegerEncoder.intCompressedSimple:
                        decodePacked();
                        break;
                    case IntegerEncoder.intCompressedRLE:
                        decodeRLE();
                        break;
                    default:
                        err = string.Format("unknown encoding {0}", encoding);
                        break;
                }
            }
            return i < n;
        }

        // Read returns the next value from the decoder.
        public long Read()
        {
            switch (encoding)
            {
                case IntegerEncoder.intCompressedRLE:
                    return Encoding.ZigZagDecode(rleFirst) + i * Encoding.ZigZagDecode(rleDelta);
                default:
                    long v = Encoding.ZigZagDecode(values[i]);
                    // v is the delta encoded value, we need to add the prior value to get the original
                    v = v + prev;
                    prev = v;
                    return v;
            }
        }

        public IClockValue Create(long clock, long value, int quality)
        {
            return new ClockInt64(clock, value, quality);
        }

        private void decodeRLE()
        {
            if (bytes == null || byteIndex >= endIndex)
            {
                return;
            }

            if (endIndex - byteIndex < 8)
            {
                err = "IntegerDecoder: not enough data to decode RLE starting value";
                return;
            }

            int j = byteIndex, m = 0;

            // Next 8 bytes is the starting value
            ulong firstvalue = BitConverter.ToUInt64(bytes, j);
            j += 8;

            // Next 1-10 bytes is the delta value
            ulong value;
            m = Varint.Read(bytes, j, out value);
            if (m <= 0)
            {
                err = "IntegerDecoder: invalid RLE delta value";
                return;
            }
            j += m;

            // Last 1-10 bytes is how many times the value repeats
            ulong count;
            m = Varint.Read(bytes, j, out count);
            if (m <= 0)
            {
                err = "IntegerDecoder: invalid RLE repeat value";
                return;
            }

            // Store the first value and delta value so we do not need to allocate
            // a large values slice.  We can compute the value at position d.i on
            // demand.
            rleFirst = firstvalue;
            rleDelta = value;
            n = (int)count + 1;
            i = 0;

            // We've process all the bytes
            bytes = null;
        }

        private void decodePacked()
        {
            if (bytes == null || byteIndex >= endIndex)
            {
                return;
            }

            if (endIndex - byteIndex < 8)
            {
                err = "IntegerDecoder: not enough data to decode packed value";
                return;
            }

            ulong v = BitConverter.ToUInt64(bytes, byteIndex);
            // The first value is always unencoded
            if (first)
            {
                first = false;
                n = 1;
                values[0] = v;
            }
            else
            {
                var m = Simple8bDecoder.Decode(values, v);
                if (m.error != null)
                {
                    // Should never happen, only error that could be returned is if the the value to be decoded was not
                    // actually encoded by simple8b encoder.
                    err = string.Format("failed to decode value {0}: {1}", v, m.error);
                }
                n = m.Item1;
            }
            i = 0;
            byteIndex += 8;
        }

        private void decodeUncompressed()
        {
            if (bytes == null || byteIndex >= endIndex)
            {
                return;
            }

            if (endIndex - byteIndex < 8)
            {
                err = "IntegerDecoder: not enough data to decode uncompressed value";
                return;
            }

            values[0] = BitConverter.ToUInt64(bytes, byteIndex);
            i = 0;
            n = 1;
            byteIndex += 8;
        }

        public string Error()
        {
            return err;
        }

    }
}
