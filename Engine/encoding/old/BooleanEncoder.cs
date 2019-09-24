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
    public class BooleanEncoder : IEncoder<bool>
    {
        // booleanUncompressed is an uncompressed boolean format.
        // Not yet implemented.
        const byte booleanUncompressed = 0;
        // booleanCompressedBitPacked is an bit packed format using 1 bit per boolean
        const byte booleanCompressedBitPacked = 1;
        // The encoded bytes
        byte[] bytes;
        int len;
        // The current byte being encoded
        byte byt;
        // The number of bools packed into byt
        int i;
        // The total number of bools written
        int n;
        public BooleanEncoder(int sz)
        {
            bytes = new byte[(Constants.DefaultMaxPointsPerBlock + 7) / 8];
        }

        public void Reset()
        {
            len = 0;
            byt = 0;
            i = 0;
            n = 0;
        }

        public void Write(bool b)
        {
            // If we have filled the current byte, flush it
            if (i >= 8)
            {
                flush();
            }

            // Use 1 bit for each boolean value, shift the current byte
            // by 1 and set the least signficant bit acordingly
            byt <<= 1;
            if (b)
            {
                byt |= 1;
            }

            // Increment the current boolean count
            i++;
            // Increment the total boolean count
            n++;
        }

        private void flush()
        {
            // Pad remaining byte w/ 0s
            while (i < 8)
            {
                byt <<= 1;
                i++;
            }

            // If we have bits set, append them to the byte slice
            if (i > 0)
            {
                bytes[len++] = byt;
                byt = 0;
                i = 0;
            }
        }

        // Flush is no-op
        public void Flush() { }

        // Bytes returns a new byte slice containing the encoded booleans from previous calls to Write.
        public (ByteWriter, string) Bytes()
        {
            // Ensure the current byte is flushed
            flush();

            ByteWriter result = new ByteWriter(1 + Varint.MaxVarintLen64 + len);

            // Store the encoding type in the 4 high bits of the first byte
            result.Write((byte)(booleanCompressedBitPacked << 4));
            // Encode the number of booleans written            
            result.Write(Varint.GetBytes((ulong)n));
            // Append the packed booleans
            result.Write(bytes, 0, len);

            return (result, null);
        }

        public (ByteWriter, string) EncodingAll(Span<bool> src_span)
        {
            int srclen = src_span.Length;
            int sz = 1 + 8 + (srclen + 7) / 8;//header+num bools+bool data
            ByteWriter result = new ByteWriter(sz);
            // Store the encoding type in the 4 high bits of the first byte
            result.Write((byte)(booleanCompressedBitPacked << 4));
            // Encode the number of booleans written.
            result.Write(Varint.GetBytes(srclen));
            int bitcount = result.Length * 8;// Current bit in current byte.
            Span<byte> result_span = new Span<byte>(result.EndWrite());//取出数组
            for (int i = 0; i < srclen; i++)
            {
                bool v = src_span[i];
                if (v)
                {
                    result_span[n >> 3] |= (byte)(128 >> (bitcount & 7));
                }// Set current bit on current byte.
                else
                {
                    result_span[n >> 3] = (byte)(result_span[n >> 3] & ~(byte)(128 >> (bitcount & 7)));
                }// Clear current bit on current byte.
                bitcount++;
            }
            int length = bitcount >> 3;
            if ((bitcount & 7) > 0)
            {
                length++;// Add an extra byte to capture overflowing bits.
            }
            result.Length = length;
            return (result, null);
        }
    }
}
