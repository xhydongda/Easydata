using Arim.Encoding.Binary;
using System;

namespace Easydata.Engine
{
    /// <summary>
    /// boolean encoding uses 1 bit per value.  Each compressed byte slice contains a 1 byte header
    /// indicating the compression type, followed by a variable byte encoded length indicating
    /// how many booleans are packed in the slice.  The remaining bytes contains 1 byte for every
    /// 8 boolean values encoded.
    /// </summary>
    public class BatchBoolean : IBatchCoder<bool>
    {
        // booleanCompressedBitPacked is an bit packed format using 1 bit per boolean
        const byte booleanCompressedBitPacked = 1;
        public (ByteWriter, string) EncodingAll(Span<bool> src)
        {
            int srclen = src.Length;
            int sz = 1 + 8 + (srclen + 7) / 8;//header+num bools+bool data
            ByteWriter result = new ByteWriter(sz);
            // Store the encoding type in the 4 high bits of the first byte
            result.Write((byte)(booleanCompressedBitPacked << 4));
            // Encode the number of booleans written.
            result.Write(Varint.GetBytes(srclen));
            int n = result.Length * 8;// Current bit in current byte.
            Span<byte> b = new Span<byte>(result.EndWrite());//取出数组
            for (int i = 0; i < srclen; i++)
            {
                bool v = src[i];
                if (v)
                {
                    b[n >> 3] |= (byte)(128 >> (n & 7));
                }// Set current bit on current byte.
                else
                {
                    b[n >> 3] = (byte)(b[n >> 3] & ~(byte)(128 >> (n & 7)));
                }// Clear current bit on current byte.
                n++;
            }
            int length = n >> 3;
            if ((n & 7) > 0)
            {
                length++;// Add an extra byte to capture overflowing bits.
            }
            result.Length = length;
            return (result, null);
        }

        public (int, string) DecodeAll(Span<byte> b, Span<bool> dst)
        {
            if (b.Length == 0)
                return (0, null);
            // First byte stores the encoding type, only have 1 bit-packet format
            // currently ignore for now.
            b = b.Slice(1);
            int n = Varint.Read(b, out int count);
            if (n <= 0)
            {
                return (0, "booleanBatchDecoder: invalid count");
            }
            b = b.Slice(n);
            int j = 0;
            foreach (byte v in b)
            {
                for (byte i = 128; i > 0 && j < dst.Length; i >>= 1)
                {
                    dst[j++] = (v & i) != 0;
                }
            }
            return (j, null);
        }
    }
}
