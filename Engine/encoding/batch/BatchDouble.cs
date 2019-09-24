using Arim.Encoding.Binary;
using System;

namespace Easydata.Engine
{
    public class BatchDouble : IBatchCoder<double>
    {
        // floatCompressedGorilla is a compressed format using the gorilla paper encoding
        public const byte floatCompressedGorilla = 1;
        public static ulong Float64bits(double v)
        {
            unsafe
            {
                return *(ulong*)(&v);
            }
        }

        // FloatArrayEncodeAll encodes src into b, returning b and any error encountered.
        // The returned slice may be of a different length and capactity to b.
        //
        // Currently only the float compression scheme used in Facebook's Gorilla is
        // supported, so this method implements a batch oriented version of that.
        public (ByteWriter, string) EncodingAll(Span<double> src)
        {
            int length = src.Length;
            //9=Enough room for the header and one value.
            ByteWriter result = new ByteWriter(length * 10 + 9);
            byte[] bytes = result.EndWrite();
            Span<byte> b = new Span<byte>(bytes);
            b.Fill(0);
            b[0] = (floatCompressedGorilla << 4);
            double first;
            bool finished = false;
            if (length > 0 && Double.IsNaN(src[0]))
            {
                result.Release();
                return (null, "unsupported value: NaN");
            }
            else if (length == 0)
            {
                first = Double.NaN;// Write sentinal value to terminate batch.
                finished = true;
            }
            else
            {
                first = src[0];
                src = src.Slice(1);
            }

            int n = 8 + 64;//Number of bits written.
            ulong prev = Float64bits(first);
            // Write first value.
            ByteWriter.WriteBigEndian(bytes, 1, prev);

            int prevLeading = -1, prevTrailing = 0;
            int leading, trailing;
            ulong mask;
            double sum = 0;

            // Encode remaining values.
            for (int i = 0; !finished; i++)
            {
                double x;
                if (i < src.Length)
                {
                    x = src[i];
                    sum += x;
                }
                else
                {
                    // Encode sentinal value to terminate batch
                    x = double.NaN;
                    finished = true;
                }
                ulong cur = Float64bits(x);
                ulong vDelta = cur ^ prev;
                if (vDelta == 0)
                {
                    n++;// Write a zero bit. Nothing else to do.
                    prev = cur;
                    continue;
                }
                // n&7 - current bit in current byte.
                // n>>3 - the current byte.
                b[n >> 3] |= (byte)(128 >> (n & 7));// Sets the current bit of the current byte.
                n++;
                // Write the delta to b.
                // Determine the leading and trailing zeros.
                leading = Encoding.Clz(vDelta);
                trailing = Encoding.Ctz(vDelta);
                // Clamp number of leading zeros to avoid overflow when encoding
                leading &= 0x1F;
                if (leading >= 32)
                {
                    leading = 31;
                }
                if (prevLeading != -1 && leading >= prevLeading && trailing >= prevTrailing)
                {
                    n++;// Write a zero bit.
                    // Write the l least significant bits of vDelta to b, most significant
                    // bit first.
                    int L = 64 - prevLeading - prevTrailing;
                    // Full value to write.
                    ulong v = (vDelta >> prevTrailing) << (64 - L);// l least signifciant bits of v.
                    int m = n & 7;// Current bit in current byte.
                    int written = 0;
                    if (m > 0)// In this case the current byte is not full.
                    {
                        written = 8 - m;
                        if (L < written)
                        {
                            written = L;
                        }
                        mask = v >> 56;// Move 8 MSB to 8 LSB
                        b[n >> 3] |= (byte)(mask >> m);
                        n += written;
                        if (L == written)
                        {
                            prev = cur;
                            continue;
                        }
                    }
                    var vv = v << written;// Move written bits out of the way.
                    ByteWriter.WriteBigEndian(bytes, n >> 3, vv);
                    n += (L - written);
                }
                else
                {
                    prevLeading = leading;
                    prevTrailing = trailing;
                    // Set a single bit to indicate a value will follow.
                    b[n >> 3] |= (byte)(128 >> (n & 7));// Set current bit on current byte
                    n++;
                    int m = n & 7;
                    int L = 5;
                    ulong v = (ulong)leading << 59;// 5 LSB of leading.
                    mask = v >> 56;         // Move 5 MSB to 8 LSB
                    if (m <= 3)// 5 bits fit into current byte.
                    {
                        b[n >> 3] |= (byte)(mask >> m);
                        n = n + L;
                    }
                    else// In this case there are fewer than 5 bits available in current byte.
                    {
                        // First step is to fill current byte
                        int written = 8 - m;
                        b[n >> 3] |= (byte)(mask >> m);// Some of mask will get lost.
                        n += written;
                        // Second step is to write the lost part of mask into the next byte.
                        mask = v << written;// Move written bits in previous byte out of way.
                        mask >>= 56;
                        m = n & 7;//Recompute current bit.
                        b[n >> 3] |= (byte)(mask >> m);
                        n += (L - written);
                    }
                    // Note that if leading == trailing == 0, then sigbits == 64.  But that
                    // value doesn't actually fit into the 6 bits we have.
                    // Luckily, we never need to encode 0 significant bits, since that would
                    // put us in the other case (vdelta == 0).  So instead we write out a 0 and
                    // adjust it back to 64 on unpacking.
                    int sigbits = 64 - leading - trailing;
                    m = n & 7;
                    L = 6;
                    v = (ulong)sigbits << 58;// Move 6 LSB of sigbits to MSB
                    mask = v >> 56;   // Move 6 MSB to 8 LSB
                    if (m <= 2)
                    {
                        // The 6 bits fit into the current byte.
                        b[n >> 3] |= (byte)(mask >> m);
                        n += L;
                    }
                    else// In this case there are fewer than 6 bits available in current byte.
                    {
                        // First step is to fill the current byte.
                        int written = 8 - m;
                        b[n >> 3] |= (byte)(mask >> m);// Write to the current bit.
                        n += written;

                        // Second step is to write the lost part of mask into the next byte.
                        // Write l remaining bits into current byte.
                        mask = v << written;// Remove bits written in previous byte out of way.
                        mask >>= 56;
                        m = n & 7;// Recompute current bit.
                        b[n >> 3] |= (byte)(mask >> m);
                        n += L - written;
                    }
                    // Write final value.
                    m = n & 7;
                    L = sigbits;
                    v = (vDelta >> trailing) << (64 - L); // Move l LSB into MSB
                    int written2 = 0;
                    if (m > 0)// In this case the current byte is not full.
                    {
                        written2 = 8 - m;
                        if (L < written2)
                        {
                            written2 = L;
                        }
                        mask = v >> 56;//Move 8 MSB to 8 LSB
                        b[n >> 3] |= (byte)(mask >> m);
                        n += written2;
                        if (L - written2 == 0)
                        {
                            prev = cur;
                            continue;
                        }
                    }
                    // Shift remaining bits and write out in one go.
                    ulong vv = v << written2;// Remove bits written in previous byte.
                    ByteWriter.WriteBigEndian(bytes, n >> 3, vv);
                    n += (L - written2);
                }
                prev = cur;
            }
            if (Double.IsNaN(sum))
            {
                result.Release();
                return (null, "unsupported value: NaN");
            }
            int blength = n >> 3;
            if ((n & 7) > 0)
            {
                blength++; // Add an extra byte to capture overflowing bits.
            }
            result.Length = blength;
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


        public (int, string) DecodeAll(Span<byte> src_span, Span<double> to_span)
        {
            if (src_span.Length < 9) { return (0, null); }
            long to_len = to_span.Length;
            ulong val;          // current value
            byte trailingN = 0;    // trailing zero count
            byte meaningfulN = 64; // meaningful bit count

            // first byte is the compression type; always Gorilla
            src_span = src_span.Slice(1);//

            val = ByteWriter.ReadBigEndian(src_span);
            if (val == Constants.uvnan)
            {
                // special case: there were no values to decode
                return (0, null);
            }

            int result = 1;
            to_span[0] = Float64frombits(val);
            src_span = src_span.Slice(8);
            // The bit reader code uses brCachedVal to store up to the next 8 bytes
            // of MSB data read from b. brValidBits stores the number of remaining unread
            // bits starting from the MSB. Before N bits are read from brCachedVal,
            // they are left-rotated N bits, such that they end up in the left-most position.
            // Using bits.RotateLeft64 results in a single instruction on many CPU architectures.
            // This approach permits simple tests, such as for the two control bits:
            //
            //    brCachedVal&1 > 0
            //
            // The alternative was to leave brCachedValue alone and perform shifts and
            // masks to read specific bits. The original approach looked like the
            // following:
            //
            //    brCachedVal&(1<<(brValidBits&0x3f)) > 0
            //
            ulong brCachedVal = 0;// a buffer of up to the next 8 bytes read from b in MSB order
            byte brValidBits = 0;  // the number of unread bits remaining in brCachedVal
            // Refill brCachedVal, reading up to 8 bytes from b
            if (src_span.Length >= 8)
            {
                // fast path reads 8 bytes directly
                brCachedVal = ByteWriter.ReadBigEndian(src_span);
                brValidBits = 64;
                src_span = src_span.Slice(8);
            }
            else if (src_span.Length > 0)
            {
                brCachedVal = 0;
                brValidBits = (byte)(src_span.Length * 8);
                foreach (byte bi in src_span)
                {
                    brCachedVal = (brCachedVal << 8) | bi;
                }
                brCachedVal = RotateRight64(brCachedVal, brValidBits);
            }
            else
            {
                goto ERROR;
            }
            // The expected exit condition is for a uvnan to be decoded.
            // Any other error (EOF) indicates a truncated stream.
            while (result < to_len)
            {
                if (brValidBits > 0)
                {
                    // brValidBits > 0 is impossible to predict, so we place the
                    // most likely case inside the if and immediately jump, keeping
                    // the instruction pipeline consistently full.
                    // This is a similar approach to using the GCC __builtin_expect
                    // intrinsic, which modifies the order of branches such that the
                    // likely case follows the conditional jump.
                    goto READ0;
                }
                // Refill brCachedVal, reading up to 8 bytes from b
                if (src_span.Length >= 8)
                {
                    brCachedVal = ByteWriter.ReadBigEndian(src_span);
                    brValidBits = 64;
                    src_span = src_span.Slice(8);
                }
                else if (src_span.Length > 0)
                {
                    brCachedVal = 0;
                    brValidBits = (byte)(src_span.Length * 8);
                    foreach (byte bi in src_span)
                    {
                        brCachedVal = (brCachedVal << 8) | bi;
                    }
                    brCachedVal = RotateRight64(brCachedVal, brValidBits);
                }
                else
                {
                    goto ERROR;
                }
            READ0:
                brValidBits--;
                brCachedVal = RotateLeft64(brCachedVal, 1);
                if ((brCachedVal & 1) > 0)
                {
                    if (brValidBits > 0)
                    {
                        goto READ1;
                    }
                    // Refill brCachedVal, reading up to 8 bytes from b
                    if (src_span.Length >= 8)
                    {
                        brCachedVal = ByteWriter.ReadBigEndian(src_span);
                        brValidBits = 64;
                        src_span = src_span.Slice(8);
                    }
                    else if (src_span.Length > 0)
                    {
                        brCachedVal = 0;
                        brValidBits = (byte)(src_span.Length * 8);
                        foreach (byte bi in src_span)
                        {
                            brCachedVal = (brCachedVal << 8) | bi;
                        }
                        brCachedVal = RotateRight64(brCachedVal, brValidBits);
                    }
                    else
                    {
                        goto ERROR;
                    }

                READ1:
                    // read control bit 1
                    brValidBits--;
                    brCachedVal = RotateLeft64(brCachedVal, 1);
                    if ((brCachedVal & 1) > 0)
                    {
                        // read 5 bits for leading zero count and 6 bits for the meaningful data count
                        const int leadingTrailingBitCount = 11;
                        ulong lmBits = 0;// leading + meaningful data counts
                        if (brValidBits >= leadingTrailingBitCount)
                        {
                            // decode 5 bits leading + 6 bits meaningful for a total of 11 bits
                            brValidBits -= leadingTrailingBitCount;
                            brCachedVal = RotateLeft64(brCachedVal, leadingTrailingBitCount);
                            lmBits = brCachedVal;
                        }
                        else
                        {
                            byte bits01 = 11;
                            if (brValidBits > 0)
                            {
                                bits01 -= brValidBits;
                                lmBits = RotateLeft64(brCachedVal, 11);
                            }
                            // Refill brCachedVal, reading up to 8 bytes from b
                            if (src_span.Length >= 8)
                            {
                                brCachedVal = ByteWriter.ReadBigEndian(src_span);
                                brValidBits = 64;
                                src_span = src_span.Slice(8);
                            }
                            else if (src_span.Length > 0)
                            {
                                brCachedVal = 0;
                                brValidBits = (byte)(src_span.Length * 8);
                                foreach (byte bi in src_span)
                                {
                                    brCachedVal = (brCachedVal << 8) | bi;
                                }
                                brCachedVal = RotateRight64(brCachedVal, brValidBits);
                            }
                            else
                            {
                                goto ERROR;
                            }
                            brCachedVal = RotateLeft64(brCachedVal, bits01);
                            brValidBits -= bits01;
                            lmBits = lmBits & ~bitMask[bits01 & 0x3F];
                            lmBits |= brCachedVal & bitMask[bits01 & 0x3f];
                        }
                        lmBits &= 0x7FF;
                        byte leadingN = (byte)((lmBits >> 6) & 0x1F);//5 bits leading
                        meaningfulN = (byte)(lmBits & 0x3F);       // 6 bits meaningful
                        if (meaningfulN > 0)
                        {
                            trailingN = (byte)(64 - leadingN - meaningfulN);
                        }
                        else
                        {
                            // meaningfulN == 0 is a special case, such that all bits                                               
                            // are meaningful
                            trailingN = 0;
                            meaningfulN = 64;

                        }
                    }
                    ulong sBits = 0;// significant bits
                    if (brValidBits >= meaningfulN)
                    {
                        brValidBits -= meaningfulN;
                        brCachedVal = RotateLeft64(brCachedVal, meaningfulN);
                        sBits = brCachedVal;
                    }
                    else
                    {
                        byte mBits = meaningfulN;
                        if (brValidBits > 0)
                        {
                            mBits -= brValidBits;
                            sBits = RotateLeft64(brCachedVal, meaningfulN);
                        }
                        // Refill brCachedVal, reading up to 8 bytes from b
                        if (src_span.Length >= 8)
                        {
                            brCachedVal = ByteWriter.ReadBigEndian(src_span);
                            brValidBits = 64;
                            src_span = src_span.Slice(8);
                        }
                        else if (src_span.Length > 0)
                        {
                            brCachedVal = 0;
                            brValidBits = (byte)(src_span.Length * 8);
                            foreach (byte bi in src_span)
                            {
                                brCachedVal = (brCachedVal << 8) | bi;
                            }
                            brCachedVal = RotateRight64(brCachedVal, brValidBits);
                        }
                        else
                        {
                            goto ERROR;
                        }
                        brCachedVal = RotateLeft64(brCachedVal, mBits);
                        brValidBits -= mBits;
                        sBits = sBits & ~bitMask[mBits & 0x3F];
                        sBits |= brCachedVal & bitMask[mBits & 0x3F];
                    }
                    sBits &= bitMask[meaningfulN & 0x3F];
                    val ^= sBits << (trailingN & 0x3F);
                    if (val == Constants.uvnan)
                    {
                        // IsNaN, eof
                        break;
                    }
                }
                to_span[result++] = Float64frombits(val);
            }
            return (result, null);
        ERROR: return (0, "io.EOF");
        }

        public static ulong RotateLeft64(ulong original, int bits)
        {
            return (original << bits) | (original >> (64 - bits));
        }

        public static ulong RotateRight64(ulong original, int bits)
        {
            return (original >> bits) | (original << (64 - bits));
        }

        public static double Float64frombits(ulong v)
        {
            unsafe
            {
                return *(double*)&v;
            }
        }

        static ulong[] bitMask;
        static BatchDouble()
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
