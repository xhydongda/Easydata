using Arim.Encoding.Binary;
using System;
using System.Buffers;

namespace Easydata.Engine
{
    public class BatchInt64 : IBatchCoder<long>
    {
        // intUncompressed is an uncompressed format using 8 bytes per point
        public const byte intUncompressed = 0;
        // intCompressedSimple is a bit-packed format using simple8b encoding
        public const byte intCompressedSimple = 1;
        // intCompressedRLE is a run-length encoding format
        public const byte intCompressedRLE = 2;
        public (ByteWriter, string) EncodingAll(Span<long> src)
        {
            int srclen = src.Length;
            ulong max = 0;
            ulong[] deltasarray = ArrayPool<ulong>.Shared.Rent(srclen);
            Span<ulong> deltas = new Span<ulong>(deltasarray);
            for (int i = srclen - 1; i > 0; i--)
            {
                long l = src[i] - src[i - 1];
                ulong delta = Encoding.ZigZagEncode(l);
                deltas[i] = delta;
                if (delta > max)
                {
                    max = delta;
                }
            }
            deltas[0] = Encoding.ZigZagEncode(src[0]);
            ByteWriter result;
            if (srclen > 2)
            {
                var rle = true;
                for (int i = 2; i < srclen; i++)
                {
                    if (deltas[1] != deltas[i])
                    {
                        rle = false;
                        break;
                    }
                }
                if (rle)
                {
                    // Large varints can take up to 10 bytes.  We're storing 3 + 1
                    // type byte.
                    result = new ByteWriter(31);
                    // 4 high bits used for the encoding type
                    result.Write((byte)(intCompressedRLE << 4));
                    // The first value
                    result.Write(deltas[0]);
                    // The first delta
                    result.Write(Varint.GetBytes(deltas[1]));
                    // The number of times the delta is repeated
                    result.Write(Varint.GetBytes(srclen - 1));
                    ArrayPool<ulong>.Shared.Return(deltasarray);
                    return (result, null);
                }
            }
            if (max > Simple8bEncoder.MaxValue)// There is an encoded value that's too big to simple8b encode.
            {
                // Encode uncompressed.
                int sz = 1 + srclen * 8;
                result = new ByteWriter(sz);
                result.Write((byte)(intUncompressed << 4));
                for (int i = 0; i < srclen; i++)
                {
                    result.Write(deltas[i]);
                }
                ArrayPool<ulong>.Shared.Return(deltasarray);
                return (result, null);
            }
            result = new ByteWriter(1 + srclen * 8);
            result.Write((byte)(intCompressedSimple << 4));
            // Write the first value since it's not part of the encoded values
            result.Write(deltas[0]);
            var error = Simple8bEncoder.EncodeAll(deltasarray, 1, srclen, result);
            ArrayPool<ulong>.Shared.Return(deltasarray);
            if (error != null)
            {
                result.Release();
                return (null, error);
            }
            return (result, null);
        }

        public (int, string) DecodeAll(Span<byte> b, Span<long> dst)
        {
            if (b.Length == 0)
            {
                return (0, null);
            }
            byte encoding = (byte)(b[0] >> 4);
            b = b.Slice(1);
            if (encoding == intUncompressed)
            {
                if ((b.Length & 0x7) != 0)
                {
                    return (0, "integerArrayDecodeAll: expected multiple of 8 bytes");
                }
                int count = b.Length / 8;
                long prev = 0;
                for (int i = 0; i < count; i++)
                {
                    prev += Encoding.ZigZagDecode(BitConverter.ToUInt64(b));
                    dst[i] = prev;
                    b = b.Slice(8);
                }
                return (count, null);
            }//uncompressed
            else if (encoding == intCompressedSimple)
            {
                if (b.Length < 8)
                {
                    return (0, "integerArrayDecodeAll: not enough data to decode packed value");
                }
                (int count, string err) = Simple8bDecoder.CountBytes(b.Slice(8));
                if (err != null)
                {
                    return (0, err);
                }
                count++;
                //first value
                dst[0] = Encoding.ZigZagDecode(BitConverter.ToUInt64(b));

                ulong[] bufarray = ArrayPool<ulong>.Shared.Rent(count);
                Span<ulong> buf = new Span<ulong>(bufarray, 0, count);
                var nerr = Simple8bDecoder.DecodeAll(buf.Slice(1), b.Slice(8));
                if (nerr.error != null)
                {
                    ArrayPool<ulong>.Shared.Return(bufarray);
                    return (0, err);
                }
                if (nerr.Item1 != count - 1)
                {
                    ArrayPool<ulong>.Shared.Return(bufarray);
                    return (0, String.Format("integerArrayDecodeAll: unexpected number of values decoded; got={0}, exp={1}", nerr.Item1, count - 1));
                }
                // calculate prefix sum
                var prev = dst[0];
                for (int i = 1; i < count; i++)
                {
                    prev += Encoding.ZigZagDecode(buf[i]);
                    dst[i] = prev;
                }
                ArrayPool<ulong>.Shared.Return(bufarray);
                return (count, null);
            }//simple.
            else if (encoding == intCompressedRLE)
            {
                if (b.Length < 8)
                {
                    return (0, "integerArrayDecodeAll: not enough data to decode RLE starting value");
                }
                long first = Encoding.ZigZagDecode(BitConverter.ToUInt64(b));
                b = b.Slice(8);
                // Next 1-10 bytes is the delta value
                int n = Varint.Read(b, out ulong value);
                if (n <= 0)
                {
                    return (0, "integerArrayDecodeAll: invalid RLE delta value");
                }
                b = b.Slice(n);
                long delta = Encoding.ZigZagDecode(value);
                // Last 1-10 bytes is how many times the value repeats
                n = Varint.Read(b, out int count);
                if (n <= 0)
                {
                    return (0, "integerArrayDecodeAll: invalid RLE repeat value");
                }
                count++;
                if (delta == 0)
                {
                    for (int i = 0; i < count; i++)
                    {
                        dst[i] = first;
                    }
                }
                else
                {
                    long acc = first;
                    for (int i = 0; i < count; i++)
                    {
                        dst[i] = acc;
                        acc += delta;
                    }
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
