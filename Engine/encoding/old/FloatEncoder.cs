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
    public class FloatEncoder : IEncoder<double>
    {
        public const bool Zero = false;
        public const bool One = true;
        // floatCompressedGorilla is a compressed format using the gorilla paper encoding
        public const byte floatCompressedGorilla = 1;

        double val;
        int leading;
        int trailing;

        byte[] buf;
        int len;
        BitWriter bw;

        bool first;
        bool finished;
        string err;

        public FloatEncoder(int sz)
        {
            first = true;
            leading = -1;

            buf = new byte[Constants.DefaultMaxPointsPerBlock * 10];
            buf[0] = (floatCompressedGorilla << 4);
            len = 1;
            bw = new BitWriter(buf) { Len = len };
        }

        public void Reset()
        {
            val = 0;
            leading = -1;
            trailing = 0;
            buf[0] = (floatCompressedGorilla << 4);
            len = 1;
            bw.Resume(0, 8);
            bw.Len = len;
            finished = false;
            first = true;
            err = null;
        }
        public (ByteWriter, string) Bytes()
        {
            ByteWriter byteWriter = new ByteWriter(bw.Len);
            byteWriter.Write(buf, 0, bw.Len);
            return (byteWriter, err);
        }

        // Flush indicates there are no more values to encode.
        public void Flush()
        {
            if (!finished)
            {
                // write an end-of-stream record
                finished = true;
                Write(Double.NaN);//
                bw.Flush(Zero);
            }
        }

        public void Write(double d)
        {
            if (Double.IsNaN(d) && !finished)
            {
                err = "unsupported value: NaN";
                return;
            }
            if (first)
            {
                val = d;
                first = false;
                bw.WriteBits(Float64bits(d), 64);
                return;
            }
            ulong vDelta = Float64bits(d) ^ Float64bits(val);
            if (vDelta == 0)
            {
                bw.WriteBit(Zero);
            }
            else
            {
                bw.WriteBit(One);
                int clz = Encoding.Clz(vDelta);
                int ctz = Encoding.Ctz(vDelta);
                // Clamp number of leading zeros to avoid overflow when encoding
                clz &= 0x1F;
                if (clz >= 32)
                {
                    clz = 31;
                }

                // TODO(dgryski): check if it's 'cheaper' to reset the leading/trailing bits instead
                if (leading != -1 && clz >= leading && ctz >= trailing)
                {
                    bw.WriteBit(Zero);
                    bw.WriteBits(vDelta >> trailing, (64 - leading - trailing));
                }
                else
                {
                    leading = clz;
                    trailing = ctz;

                    bw.WriteBit(One);
                    bw.WriteBits((ulong)clz, 5);

                    // Note that if leading == trailing == 0, then sigbits == 64.  But that
                    // value doesn't actually fit into the 6 bits we have.
                    // Luckily, we never need to encode 0 significant bits, since that would
                    // put us in the other case (vdelta == 0).  So instead we write out a 0 and
                    // adjust it back to 64 on unpacking.
                    int sigbits = 64 - clz - ctz;
                    bw.WriteBits((ulong)sigbits, 6);
                    bw.WriteBits(vDelta >> ctz, sigbits);
                }
            }
            val = d;
        }

        public static ulong Float64bits(double v)
        {
            return BitConverter.ToUInt64(BitConverter.GetBytes(v), 0);
        }

        public string Error()
        {
            return err;
        }

    }
}
