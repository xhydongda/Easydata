using Arim.Encoding.Binary;
using System;

namespace Easydata.Engine
{
    ///<summary>
    /// Timestamp encoding is adaptive and based on structure of the timestamps that are encoded.  It
    /// uses a combination of delta encoding, scaling and compression using simple8b, run length encoding
    /// as well as falling back to no compression if needed.
    ///
    /// Timestamp values to be encoded should be sorted before encoding.  When encoded, the values are
    /// first delta-encoded.  The first value is the starting timestamp, subsequent values are the difference
    /// from the prior value.
    ///
    /// Timestamp resolution can also be in the nanosecond.  Many timestamps are monotonically increasing
    /// and fall on even boundaries of time such as every 10s.  When the timestamps have this structure,
    /// they are scaled by the largest common divisor that is also a factor of 10.  This has the effect
    /// of converting very large integer deltas into very small one that can be reversed by multiplying them
    /// by the scaling factor.
    ///
    /// Using these adjusted values, if all the deltas are the same, the time range is stored using run
    /// length encoding.  If run length encoding is not possible and all values are less than 1 << 60 - 1
    /// (~36.5 yrs in nanosecond resolution), then the timestamps are encoded using simple8b encoding.  If
    /// any value exceeds the maximum values, the deltas are stored uncompressed using 8b each.
    ///
    /// Each compressed byte slice has a 1 byte header indicating the compression type.  The 4 high bits
    /// indicate the encoding type.  The 4 low bits are used by the encoding type.
    ///
    /// For run-length encoding, the 4 low bits store the log10 of the scaling factor.  The next 8 bytes are
    /// the starting timestamp, next 1-10 bytes is the delta value using variable-length encoding, finally the
    /// next 1-10 bytes is the count of values.
    ///
    /// For simple8b encoding, the 4 low bits store the log10 of the scaling factor.  The next 8 bytes is the
    /// first delta value stored uncompressed, the remaining bytes are 64bit words containg compressed delta
    /// values.
    ///
    /// For uncompressed encoding, the delta values are stored using 8 bytes each.
    /// </summary>
    public class TimeEncoder : IEncoder<long>
    {
        // timeUncompressed is a an uncompressed format using 8 bytes per timestamp
        public const byte timeUncompressed = 0;
        // timeCompressedPackedSimple is a bit-packed format using simple8b encoding
        public const byte timeCompressedPackedSimple = 1;
        // timeCompressedRLE is a run-length encoding format
        public const byte timeCompressedRLE = 2;

        ulong[] ts;
        int len;
        Simple8bEncoder enc;
        public TimeEncoder(int sz)
        {
            ts = new ulong[Constants.DefaultMaxPointsPerBlock];
            enc = new Simple8bEncoder();
        }

        // Reset sets the encoder back to its initial state.
        public void Reset()
        {
            len = 0;
            enc.Reset();
        }

        public void Flush() { }

        // Write adds a timestamp to the compressed stream.
        public void Write(long t)
        {
            ts[len++] = (ulong)t;
        }

        private void reduce(out ulong max, out ulong divisor, out bool rle)
        {
            // Compute the deltas in place to avoid allocating another slice
            Span<ulong> deltas = new Span<ulong>(ts);
            // Starting values for a max and divisor
            max = 0;
            divisor = (ulong)1e12;

            // Indicates whether the the deltas can be run-length encoded
            rle = true;

            // Iterate in reverse so we can apply deltas in place
            for (int i = len - 1; i > 0; i--)
            {

                // First differential encode the values
                deltas[i] = deltas[i] - deltas[i - 1];

                // We also need to keep track of the max value and largest common divisor
                ulong v = deltas[i];

                if (v > max)
                {
                    max = v;
                }

                // If our value is divisible by 10, break.  Otherwise, try the next smallest divisor.
                while (divisor > 1 && v % divisor != 0)
                {
                    divisor /= 10;
                }

                // Skip the first value || see if prev = curr.  The deltas can be RLE if the are all equal.
                rle = i == len - 1 || rle && (deltas[i + 1] == deltas[i]);
            }
        }

        // Bytes returns the encoded bytes of all written times.
        public (ByteWriter, string) Bytes()
        {
            if (len == 0)
            {
                return (null, null);
            }

            // Maximum and largest common divisor.  rle is true if dts (the delta timestamps),
            // are all the same.
            reduce(out ulong max, out ulong div, out bool rle);

            // The deltas are all the same, so we can run-length encode them
            if (rle && len > 1)
            {
                return encodeRLE(ts[0], ts[1], div, len);
            }

            // We can't compress this time-range, the deltas exceed 1 << 60
            if (max > Simple8bEncoder.MaxValue)
            {
                return encodeRaw();
            }

            return encodePacked(div, ts);
        }


        private (ByteWriter, string error) encodePacked(ulong div, ulong[] dts)
        {
            // Only apply the divisor if it's greater than 1 since division is expensive.
            if (div > 1)
            {
                for (int i = 1; i < len; i++)
                {
                    ulong v = dts[i];
                    string error = enc.Write(v / div);
                    if (error != null)
                    {
                        return (null, error);
                    }
                }
            }
            else
            {
                for (int i = 1; i < len; i++)
                {
                    ulong v = dts[i];
                    string error = enc.Write(v);
                    if (error != null)
                    {
                        return (null, error);
                    }
                }
            }

            // The compressed deltas
            var deltas = enc.Bytes();
            if (deltas.error != null)
            {
                return (null, deltas.error);
            }
            int sz = 8 + 1 + deltas.len;

            ByteWriter b = new ByteWriter(sz);
            // 4 high bits used for the encoding type,4 low bits are the log10 divisor     
            b.Write((byte)((timeCompressedPackedSimple << 4) | (byte)Math.Log10(div)));

            // The first delta value
            b.Write(dts[0]);

            b.Write(deltas.Item1, 0, deltas.len);
            return (b, null);
        }

        private (ByteWriter, string error) encodeRaw()
        {
            int sz = 1 + len * 8;

            ByteWriter b = new ByteWriter(sz);

            b.Write((byte)(timeUncompressed << 4));
            for (int i = 0; i < len; i++)
            {
                b.Write(ts[i]);
            }
            return (b, null);
        }

        private (ByteWriter, string error) encodeRLE(ulong first, ulong delta, ulong div, int n)
        {
            // Large varints can take up to 10 bytes, we're encoding 3 + 1 byte type
            int sz = 31;
            ByteWriter b = new ByteWriter(sz);

            // 4 high bits used for the encoding type,4 low bits are the log10 divisor
            b.Write((byte)((timeCompressedRLE << 4) | (byte)Math.Log10(div)));

            // The first timestamp
            b.Write(first);
            // The first delta
            b.Write(Varint.GetBytes(delta / div));
            // The number of times the delta is repeated
            b.Write(Varint.GetBytes(n));

            return (b, null);
        }

    }
}
