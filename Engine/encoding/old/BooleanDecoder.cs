using Arim.Encoding.Binary;
using System;

namespace Easydata.Engine
{
    /// <summary>
    /// BooleanDecoder decodes a series of booleans from an in-memory buffer.
    /// </summary>
    public class BooleanDecoder : IDecoder<bool>
    {
        // The encoded bytes
        byte[] bytes;
        int byteIndex;
        int i;
        // The total number of bools written
        int n;
        string err = null;
        // SetBytes initializes the decoder with a new set of bytes to read from.
        // This must be called before calling any other methods.
        public string SetBytes(byte[] b, int startindex, int len)
        {
            err = null;
            if (b == null || len <= 0 || b.Length - startindex - len < 0)
            {
                return null;
            }
            // First byte stores the encoding type, only have 1 bit-packet format
            // currently ignore for now.
            ulong boolcount;
            int bytecount = Varint.Read(b, startindex + 1, out boolcount);
            if (bytecount <= 0)
            {
                err = "BooleanDecoder: invalid count";
                return err;
            }
            byteIndex = bytecount + startindex + 1;
            i = -1;
            n = (int)boolcount;
            bytes = b;
            int min = len * 8;
            if (min < n)
            {
                // Shouldn't happen - TSM file was truncated/corrupted
                n = min;
            }
            return null;
        }


        // Next returns whether there are any bits remaining in the decoder.
        // It returns false if there was an error decoding.
        // The error is available on the Error method.
        public bool Next()
        {
            if (err != null)
                return false;
            i++;
            return i < n;
        }

        // Read returns the next bit from the decoder.
        public bool Read()
        {
            // Index into the byte slice
            int idx = byteIndex + i >> 3; // integer division by 8

            // Bit position
            int pos = 7 - (i & 0x7);

            // The mask to select the bit
            byte mask = (byte)(1 << pos);

            // The packed byte
            byte v = bytes[idx];

            // Returns true if the bit is set
            return (v & mask) == mask;
        }

        public IClockValue Create(long clock, bool value, int quality)
        {
            return new ClockBoolean(clock, value, quality);
        }

        public string Error()
        {
            return err;
        }

        public (int, string) DecodeAll(Span<byte> src, Span<bool> to)
        {
            if (src.Length == 0)
                return (0, null);
            // First byte stores the encoding type, only have 1 bit-packet format
            // currently ignore for now.
            src = src.Slice(1);
            int nn = Varint.Read(src, out int count);
            src = src.Slice(nn);
            if (count <= 0)
            {
                return (0, "booleanBatchDecoder: invalid count");
            }
            int j = 0;
            foreach (byte v in src)
            {
                for (byte i = 128; i > 0; i >>= 1)
                {
                    to[j++] = (v & i) != 0;
                }
            }
            return (j, null);
        }
    }
}
