namespace Easydata.Engine
{
    /// <summary>
    /// <seealso cref="https://github.com/dgryski/go-bitstream/blob/master/bitstream.go"/>
    /// </summary>
    public class BitWriter
    {
        byte[] w;

        public BitWriter(byte[] w)
        {
            this.w = w;
            Count = 8;
        }


        public byte Byt { get; private set; }

        public byte Count { get; private set; }

        public void Resume(byte data, byte count)
        {
            Byt = data;
            Count = count;
        }

        public void WriteBit(bool bit)
        {
            if (bit)
            {
                Byt |= (byte)(1 << (Count - 1));
            }
            Count--;
            if (Count == 0)
            {
                w[Len++] = Byt;
                Byt = 0;
                Count = 8;
            }
        }

        public void WriteByte(byte byt)
        {
            Byt |= (byte)(byt >> (8 - Count));
            w[Len++] = Byt;
            Byt = (byte)(byt << Count);
        }

        // Flush empties the currently in-process byte by filling it with 'bit'.
        public void Flush(bool bit)
        {
            while (Count != 8)
            {
                WriteBit(bit);
            }
        }

        public int Len { get; set; }

        // WriteBits writes the nbits least significant bits of u, most-significant-bit first.
        public void WriteBits(ulong u, int nbits)
        {
            u <<= (64 - nbits);
            while (nbits >= 8)
            {
                byte byt = (byte)(u >> 56);
                WriteByte(byt);
                u <<= 8;
                nbits -= 8;
            }
            while (nbits > 0)
            {
                WriteBit((u >> 63) == 1);
                u <<= 1;
                nbits--;
            }
        }
    }
}
