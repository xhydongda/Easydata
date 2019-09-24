using Arim.Encoding.Binary;

namespace Easydata.Engine
{
    ///<summary>
    /// Integer encoding uses two different strategies depending on the range of values in
    /// the uncompressed data.  Encoded values are first encoding used zig zag encoding.
    /// This interleaves positive and negative integers across a range of positive integers.
    /// For example, [-2,-1,0,1] becomes [3,1,0,2]. See
    /// https://developers.google.com/protocol-buffers/docs/encoding?hl=en#signed-integers
    /// for more information.
    ///
    /// If all the zig zag encoded values are less than 1 << 60 - 1, they are compressed using
    /// simple8b encoding.  If any value is larger than 1 << 60 - 1, the values are stored uncompressed.
    ///
    /// Each encoded byte slice contains a 1 byte header followed by multiple 8 byte packed integers
    /// or 8 byte uncompressed integers.  The 4 high bits of the first byte indicate the encoding type
    /// for the remaining bytes.
    ///
    /// There are currently two encoding types that can be used with room for 16 total.  These additional
    /// encoding slots are reserved for future use.  One improvement to be made is to use a patched
    /// encoding such as PFOR if only a small number of values exceed the max compressed value range.  This
    /// should improve compression ratios with very large integers near the ends of the int64 range.
    /// </summary>
    public class IntegerEncoder : IEncoder<long>
    {
        // intUncompressed is an uncompressed format using 8 bytes per point
        public const byte intUncompressed = 0;
        // intCompressedSimple is a bit-packed format using simple8b encoding
        public const byte intCompressedSimple = 1;
        // intCompressedRLE is a run-length encoding format
        public const byte intCompressedRLE = 2;
        long prev;
        bool rle;
        ulong[] values;
        int len;
        public IntegerEncoder(int sz)
        {
            rle = true;
            values = new ulong[Constants.DefaultMaxPointsPerBlock];
        }

        // Flush is no-op
        public void Flush()
        { }

        // Reset sets the encoder back to its initial state.
        public void Reset()
        {
            prev = 0;
            rle = true;
            len = 0;
        }

        // Write encodes v to the underlying buffers.
        public void Write(long l)
        {
            // Delta-encode each value as it's written.  This happens before
            // ZigZagEncoding because the deltas could be negative.
            long delta = l - prev;
            prev = l;
            ulong enc = Encoding.ZigZagEncode(delta);
            if (len > 1)
            {
                rle &= (values[len - 1] == enc);
            }

            values[len++] = enc;
        }
        // Bytes returns a copy of the underlying buffer.
        public (ByteWriter, string) Bytes()
        {
            // Only run-length encode if it could reduce storage size.
            if (rle && len > 2)
            {
                return encodeRLE();
            }
            for (int i = 0; i < len; i++)
            {
                // Value is too large to encode using packed format
                if (values[i] > Simple8bEncoder.MaxValue)
                {
                    return encodeUncompressed();
                }
            }
            return encodePacked();
        }

        private (ByteWriter, string error) encodeRLE()
        {
            // Large varints can take up to 10 bytes.  We're storing 3 + 1
            // type byte.
            ByteWriter b = new ByteWriter(31);

            // 4 high bits used for the encoding type
            b.Write((byte)(intCompressedRLE << 4));
            // The first value
            b.Write(values[0]);
            // The first delta
            b.Write(Varint.GetBytes(values[1]));
            // The number of times the delta is repeated
            b.Write(Varint.GetBytes((ulong)(len - 1)));
            return (b, null);
        }

        private (ByteWriter, string error) encodePacked()
        {
            if (len == 0)
                return (null, null);
            // Encode all but the first value.  Fist value is written unencoded
            // using 8 bytes.
            ByteWriter b = new ByteWriter(1 + 8 * len);
            // 4 high bits of first byte store the encoding type for the block
            b.Write((byte)(intCompressedSimple << 4));
            // Write the first value since it's not part of the encoded values
            b.Write(values[0]);
            var error = Simple8bEncoder.EncodeAll(values, 1, len, b);
            if (error != null)
            {
                b.Release();
                return (null, error);
            }
            return (b, null);
        }

        private (ByteWriter, string error) encodeUncompressed()
        {
            if (len == 0)
                return (null, null);
            ByteWriter b = new ByteWriter(1 + len * 8);
            b.Write((byte)(intUncompressed << 4));
            for (int i = 0; i < len; i++)
            {
                b.Write(values[i]);
            }
            return (b, null);
        }

    }
}
