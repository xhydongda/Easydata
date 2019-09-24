using System.Collections.Generic;

namespace Easydata.Engine
{
    struct buf
    {
        public ulong v;//bit buffer
        public int n;// available bits
    }

    public class BitReader
    {
        IList<byte> data;
        int ridx = 0;
        int endidx = 0;
        buf buf;
        public BitReader()
        {

        }
        public BitReader(IList<byte> data, int startindex, int len)
        {
            Reset(data, startindex, len);
        }

        public void Reset(IList<byte> data, int startindex, int len)
        {
            this.data = data;
            buf.v = 0;
            buf.n = 0;
            ridx = startindex;
            endidx = startindex + len;
            readBuf();
        }

        public bool CanReadBitFast()
        {
            return buf.n > 1;
        }

        public bool ReadBitFast()
        {
            bool v = (buf.v & ((ulong)1 << 63)) != 0;
            buf.v <<= 1;
            buf.n -= 1;
            return v;
        }

        public (bool, string error) ReadBit()
        {
            var t = ReadBits(1);
            return (t.Item1 != 0, t.error);
        }

        public (ulong, string error) ReadBits(int nbits)
        {
            // Return EOF if there is no more data.
            if (buf.n == 0)
            {
                return (0, "io.EOF");
            }
            // Return bits from buffer if less than available bits.
            if (nbits <= buf.n)
            {
                // Return all bits, if requested.
                if (nbits == 64)
                {
                    var v1 = buf.v;
                    buf.v = 0;
                    buf.n = 0;
                    readBuf();
                    return (v1, null);
                }
                // Otherwise mask returned bits.
                var v2 = buf.v >> (64 - nbits);
                buf.v <<= nbits;
                buf.n -= nbits;
                if (buf.n == 0)
                {
                    readBuf();
                }
                return (v2, null);
            }
            // Otherwise read all available bits in current buffer.
            var v = buf.v;
            var n = buf.n;
            // Read new buffer.
            buf.v = 0;
            buf.n = 0;
            readBuf();
            // Append new buffer to previous buffer and shift to remove unnecessary bits.
            v |= (buf.v >> n);
            v >>= 64 - nbits;
            // Remove used bits from new buffer.
            var bufN = nbits - n;
            if (bufN > buf.n)
            {
                bufN = buf.n;
            }
            buf.v <<= bufN;
            buf.n -= bufN;
            if (buf.n == 0)
            {
                readBuf();
            }
            return (v, null);
        }

        private void readBuf()
        {
            // Determine number of bytes to read to fill buffer.
            var byteN = 8 - (buf.n / 8);

            // Limit to the length of our data.
            var n = endidx - ridx;
            if (byteN > n)
            {
                byteN = n;
            }
            // Optimized 8-byte read.
            if (byteN == 8)
            {
                buf.v = data[ridx + 7] | (ulong)data[ridx + 6] << 8 |
                    (ulong)data[ridx + 5] << 16 | (ulong)data[ridx + 4] << 24 |
                    (ulong)data[ridx + 3] << 32 | (ulong)data[ridx + 2] << 40 |
                    (ulong)data[ridx + 1] << 48 | (ulong)data[ridx] << 56;
                buf.n = 64;
                ridx += 8;
                return;
            }

            // Otherwise append bytes to buffer.
            for (int i = 0; i < byteN; i++)
            {
                buf.n += 8;
                buf.v |= (ulong)data[ridx + i] << (64 - buf.n);
            }
            // Move data forward.
            ridx += byteN;
        }
    }
}
