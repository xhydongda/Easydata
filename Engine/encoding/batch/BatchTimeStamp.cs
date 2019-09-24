using Arim.Encoding.Binary;
using System;
using System.Buffers;

namespace Easydata.Engine
{
    public class BatchTimeStamp : IBatchCoder<long>
    {
        // timeUncompressed is a an uncompressed format using 8 bytes per timestamp
        public const byte timeUncompressed = 0;
        // timeCompressedPackedSimple is a bit-packed format using simple8b encoding
        public const byte timeCompressedPackedSimple = 1;
        // timeCompressedRLE is a run-length encoding format
        public const byte timeCompressedRLE = 2;
        public (ByteWriter, string) EncodingAll(Span<long> src)
        {
            int srclen = src.Length;
            if (srclen == 0)
            {
                return (null, null); // Nothing to do
            }
            ulong max = 0, div = (ulong)1e12;
            ulong[] deltaarray = ArrayPool<ulong>.Shared.Rent(srclen);
            Span<ulong> deltas = new Span<ulong>(deltaarray);
            deltas[0] = (ulong)src[0];
            for (int i = srclen - 1; i > 0; i--)
            {
                deltas[i] = (ulong)src[i] - (ulong)src[i - 1];
                if (deltas[i] > max)
                {
                    max = deltas[i];
                }
            }
            bool rle = true;
            for (int i = 2; i < srclen; i++)
            {
                if (deltas[1] != deltas[i])
                {
                    rle = false;
                    break;
                }
            }
            ByteWriter result;
            // Deltas are the same - encode with RLE
            if (rle)
            {
                // Large varints can take up to 10 bytes.  We're storing 3 + 1
                // type byte.
                result = new ByteWriter(31);
                // 4 high bits used for the encoding type
                result.Write((byte)(timeCompressedRLE << 4));
                // The first value
                result.Write(deltas[0]);
                // The first delta, checking the divisor
                // given all deltas are the same, we can do a single check for the divisor
                ulong v = deltas[1];
                while (div > 1 && v % div != 0)
                {
                    div /= 10;
                }
                if (div > 1)
                {
                    // 4 low bits are the log10 divisor
                    result.EndWrite()[0] |= (byte)Math.Log10(div);
                    result.Write(Varint.GetBytes(deltas[1] / div));
                }
                else
                {
                    result.Write(Varint.GetBytes(deltas[1]));
                }
                // The number of times the delta is repeated
                result.Write(Varint.GetBytes(srclen));
                ArrayPool<ulong>.Shared.Return(deltaarray);
                return (result, null);
            }
            // We can't compress this time-range, the deltas exceed 1 << 60
            if (max > Simple8bEncoder.MaxValue)
            {
                // Encode uncompressed.
                int sz = 1 + srclen * 8;
                result = new ByteWriter(sz);
                result.Write((byte)(timeUncompressed << 4));
                for (int i = 0; i < srclen; i++)
                {
                    result.Write(deltas[i]);
                }
                ArrayPool<ulong>.Shared.Return(deltaarray);
                return (result, null);
            }
            // find divisor only if we're compressing with simple8b
            for (int i = 1; i < srclen && div > 1; i++)
            {
                // If our value is divisible by 10, break.  Otherwise, try the next smallest divisor.
                var v = deltaarray[i];
                while (div > 1 && v % div != 0)
                {
                    div /= 10;
                }
            }
            // Only apply the divisor if it's greater than 1 since division is expensive.
            if (div > 1)
            {
                for (int i = 1; i < srclen; i++)
                {
                    deltas[i] /= div;
                }
            }
            result = new ByteWriter(1 + srclen * 8);
            result.Write((byte)((timeCompressedPackedSimple << 4) | (byte)Math.Log10(div)));
            // Write the first value since it's not part of the encoded values
            result.Write(deltas[0]);
            // Encode with simple8b - fist value is written unencoded using 8 bytes.
            var error = Simple8bEncoder.EncodeAll(deltaarray, 1, srclen, result);
            if (error != null)
            {
                result.Release();
                return (null, error);
            }
            return (result, null);
        }
        public (int, string) DecodeAll(Span<byte> b, Span<long> dst)
        {
            if (b.Length == 0) return (0, null);
            byte encoding = (byte)(b[0] >> 4);
            long div = b[0] & 0xF;
            b = b.Slice(1);
            if (encoding == timeUncompressed)
            {
                if ((b.Length & 0x7) != 0)
                {
                    return (0, "timeArrayDecodeAll: expected multiple of 8 bytes");
                }
                int count = b.Length / 8;
                ulong prev = 0;
                for (int i = 0; i < count; i++)
                {
                    prev += BitConverter.ToUInt64(b);
                    dst[i] = (long)prev;
                    b = b.Slice(8);
                }
                return (count, null);
            }//uncompressed
            else if (encoding == timeCompressedPackedSimple)
            {
                if (b.Length < 8)
                {
                    return (0, "timeArrayDecodeAll: not enough data to decode packed value");
                }
                (int count, string err) = Simple8bDecoder.CountBytes(b.Slice(8));
                if (err != null)
                {
                    return (0, err);
                }
                count++;
                ulong[] bufarray = ArrayPool<ulong>.Shared.Rent(count);
                Span<ulong> buf = new Span<ulong>(bufarray, 0, count);
                //first value
                dst[0] = BitConverter.ToInt64(b);
                var nerr = Simple8bDecoder.DecodeAll(buf.Slice(1), b.Slice(8));
                if (nerr.error != null)
                {
                    ArrayPool<ulong>.Shared.Return(bufarray);
                    return (0, err);
                }
                if (nerr.Item1 != count - 1)
                {
                    ArrayPool<ulong>.Shared.Return(bufarray);
                    return (0, String.Format("timeArrayDecodeAll: unexpected number of values decoded; got={0}, exp={1}", nerr.Item1, count - 1));
                }
                // Compute the prefix sum and scale the deltas back up
                var last = dst[0];
                if (div > 1)
                {
                    for (int i = 1; i < count; i++)
                    {
                        var dgap = (long)buf[i] * div;
                        dst[i] = last + dgap;
                        last = dst[i];
                    }
                }
                else
                {
                    for (int i = 1; i < count; i++)
                    {
                        dst[i] = (long)buf[i] + last;
                        last = dst[i];
                    }
                }
                ArrayPool<ulong>.Shared.Return(bufarray);
                return (count, null);
            }//simple.
            else if (encoding == timeCompressedRLE)
            {
                if (b.Length < 8)
                {
                    return (0, "timeArrayDecodeAll: not enough data to decode RLE starting value");
                }
                // Lower 4 bits hold the 10 based exponent so we can scale the values back up
                var mod = div;
                ulong first = BitConverter.ToUInt64(b);
                b = b.Slice(8);
                // Next 1-10 bytes is our (scaled down by factor of 10) run length delta
                int n = Varint.Read(b, out ulong delta);
                if (n <= 0)
                {
                    return (0, "timeArrayDecodeAll: invalid RLE delta value");
                }
                b = b.Slice(n);
                // Scale the delta back up
                delta *= (ulong)mod;
                // Last 1-10 bytes is how many times the value repeats
                n = Varint.Read(b, out int count);
                if (n <= 0)
                {
                    return (0, "timeDecoder: invalid repeat value in decodeRLE");
                }

                var acc = first;
                for (int i = 0; i < count; i++)
                {
                    dst[i] = (long)acc;
                    acc += delta;
                }
                return (count, null);
            }//rle
            else
            {
                return (0, string.Format("unknown encoding {0:X2}", encoding));
            }
        }
    }
}
