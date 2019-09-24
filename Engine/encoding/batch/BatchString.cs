using Arim.Encoding.Binary;
using System;

namespace Easydata.Engine
{
    public class BatchString : IBatchCoder<string>
    {
        // stringCompressedSnappy is a compressed encoding using Snappy compression
        public const byte stringCompressedSnappy = 1;
        public (ByteWriter, string) EncodingAll(Span<string> src)
        {
            int srcSz = 2 + src.Length * Varint.MaxVarintLen32;// strings should't be longer than 64kb
            for (int i = 0; i < src.Length; i++)
            {
                srcSz += src[i].Length;
            }
            // determine the maximum possible length needed for the buffer, which
            // includes the compressed size
            var compressSz = 0;
            if (src.Length > 0)
            {
                compressSz = SnappyMaxEncodedLen(srcSz) + 1;//header
            }
            int totSz = srcSz + compressSz;
            ByteWriter result = new ByteWriter(totSz);
            // Shortcut to snappy encoding nothing.
            if (src.Length == 0)
            {
                result.Write((byte)(stringCompressedSnappy << 4));
                return (result, null);
            }
            for (int i = 0; i < src.Length; i++)
            {
                result.Write(Varint.GetBytes(src[i].Length));
                result.Write(System.Text.Encoding.Default.GetBytes(src[i]));
            }
            byte[] compressed = SnappyPI.SnappyCodec.Compress(result.EndWrite(), 0, result.Length);
            result.Length = 0;
            result.Write((byte)(stringCompressedSnappy << 4));
            result.Write(compressed);
            return (result, null);
        }


        // It will return a negative value if srcLen is too large to encode.
        public static int SnappyMaxEncodedLen(int srcLen)
        {
            ulong n = (ulong)srcLen;
            if (n > 0xffffffff)
            {
                return -1;
            }
            // Compressed data can be defined as:
            //    compressed := item* literal*
            //    item       := literal* copy
            //
            // The trailing literal sequence has a space blowup of at most 62/60
            // since a literal of length 60 needs one tag byte + one extra byte
            // for length information.
            //
            // Item blowup is trickier to measure. Suppose the "copy" op copies
            // 4 bytes of data. Because of a special check in the encoding code,
            // we produce a 4-byte copy only if the offset is < 65536. Therefore
            // the copy op takes 3 bytes to encode, and this type of item leads
            // to at most the 62/60 blowup for representing literals.
            //
            // Suppose the "copy" op copies 5 bytes of data. If the offset is big
            // enough, it will take 5 bytes to encode the copy op. Therefore the
            // worst case here is a one-byte literal followed by a five-byte copy.
            // That is, 6 bytes of input turn into 7 bytes of "compressed" data.
            //
            // This last factor dominates the blowup, so the final estimate is:
            n = 32 + n + n / 6;
            if (n > 0xffffffff)
            {
                return -1;
            }
            return (int)n;
        }

        //TODO performance 1: Uncompress using output; 2.string convert.
        public (int, string) DecodeAll(Span<byte> b, Span<string> dst)
        {
            if (b.Length == 0)
            {
                return (0, null);
            }
            // First byte stores the encoding type, only have snappy format
            // currently so ignore for now.
            b = b.Slice(1);
            byte[] uncompressed = SnappyPI.SnappyCodec.Uncompress(b.ToArray(), 0, b.Length);
            if (uncompressed == null)
            {
                return (0, null);
            }
            b = new Span<byte>(uncompressed);
            int j = 0;
            while (b.Length > 0)
            {
                int n = Varint.Read(b, out int length);
                if (n <= 0)
                {
                    return (0, "stringArrayDecodeAll: invalid encoded string length");
                }
                b = b.Slice(n);
                if (length < 0)
                {
                    return (0, "stringArrayDecodeAll: length overflow");
                }
                if (length > b.Length)
                {
                    return (0, "stringArrayDecodeAll: short buffer");
                }
                dst[j++] = System.Text.Encoding.Default.GetString(b.Slice(0, length));
                b = b.Slice(length);
            }
            return (j, null);
        }
    }
}
