using System;

namespace Easydata.Engine
{
    /// <summary>
    /// This code is originally from: https://github.com/dgryski/go-tsz and has been modified to remove
    /// the timestamp compression fuctionality.
    /// It implements the float compression as presented in: http://www.vldb.org/pvldb/vol8/p1816-teller.pdf.
    /// This implementation uses a sentinel value of NaN which means that float64 NaN cannot be stored using
    /// this version.
    /// </summary>
    public class FloatDecoder : IDecoder<double>
    {
        ulong val;
        ulong leading;
        ulong trailing;
        BitReader br;
        byte[] bytes;
        bool first;
        bool finished;

        string err = null;

        public FloatDecoder()
        {
            br = new BitReader();
        }

        // SetBytes initializes the decoder with b. Must call before calling Next().
        public string SetBytes(byte[] b, int startindex, int len)
        {
            ulong v;
            if (b == null || b.Length < startindex + len)
            {
                v = Constants.uvnan;
            }
            else
            {
                // first byte is the compression type.
                // we currently just have gorilla compression.
                br.Reset(b, startindex + 1, len - 1);
                (v, err) = br.ReadBits(64);
                if (err != null)
                {
                    return err;
                }
            }
            // Reset all fields.
            val = v;
            leading = 0;
            trailing = 0;
            bytes = b;
            first = true;
            finished = false;
            err = null;
            return null;
        }

        // Next returns true if there are remaining values to read.
        public bool Next()
        {
            if (finished || err != null)
                return false;
            if (first)
            {
                first = false;
                // mark as finished if there were no values.
                if (val == Constants.uvnan) // IsNaN
                {
                    finished = true;
                    return false;
                }
                return true;
            }

            // read compressed value
            bool bit;
            if (br.CanReadBitFast())
            {
                bit = br.ReadBitFast();
            }
            else
            {
                (bit, err) = br.ReadBit();
                if (err != null)
                {
                    return false;
                }
            }
            if (bit)
            {
                if (br.CanReadBitFast())
                {
                    bit = br.ReadBitFast();
                }
                else
                {
                    (bit, err) = br.ReadBit();
                    if (err != null)
                    {
                        return false;
                    }
                }
                if (bit)
                {
                    ulong bits;
                    (bits, err) = br.ReadBits(5);
                    if (err != null)
                    {
                        return false;
                    }
                    leading = bits;
                    (bits, err) = br.ReadBits(6);
                    if (err != null)
                    {
                        return false;
                    }
                    // 0 significant bits here means we overflowed and we actually need 64; see comment in encoder
                    if (bits == 0)
                    {
                        bits = 64;
                    }
                    trailing = 64 - leading - bits;
                }
                var mbits = 64 - leading - trailing;
                ulong bits1;
                (bits1, err) = br.ReadBits((int)mbits);
                if (err != null)
                {
                    return false;
                }
                var vbits1 = val;
                vbits1 ^= (bits1 << (int)trailing);
                if (vbits1 == Constants.uvnan)//IsNaN
                {
                    finished = true;
                    return false;
                }
                val = vbits1;
            }
            return true;
        }

        public double Read()
        {
            return Float64frombits(val);
        }

        public IClockValue Create(long clock, double value, int quality)
        {
            return new ClockDouble(clock, value, quality);
        }

        public static double Float64frombits(ulong v)
        {
            return BitConverter.ToDouble(BitConverter.GetBytes(v), 0);
        }

        public string Error()
        {
            return err;
        }

        static ulong[] bitMask;
        static FloatDecoder()
        {
            ulong v = 1;
            bitMask = new ulong[64];
            for (int i = 1; i < 64; i++)
            {
                bitMask[i & 0x3F] = v;
                v = (v << 1) | 1;
            }
        }
    }
}
