using Arim.Encoding.Binary;
using System;

namespace Easydata.Engine
{
    /// <summary>
    /// StringDecoder decodes a byte slice into strings.
    /// </summary>
    public class StringDecoder : IDecoder<string>
    {
        // The encoded bytes
        byte[] bytes;
        int byteslength = 0;
        int i;
        int l;
        string err = null;
        // SetBytes initializes the decoder with bytes to read from.
        // This must be called before calling any other method.        
        public string SetBytes(byte[] b, int startindex, int len)
        {
            err = null;
            // First byte stores the encoding type, only have snappy format
            // currently so ignore for now.
            byte[] data = null;
            if (b != null && b.Length >= startindex + len)
            {
                data = SnappyPI.SnappyCodec.Uncompress(b, startindex + 1, len - 1);
            }//+1 for result.Add( stringCompressedSnappy << 4);
            bytes = data;
            byteslength = (bytes == null ? 0 : bytes.Length);
            l = 0;
            i = 0;
            return null;
        }


        // Next returns true if there are any values remaining to be decoded.
        public bool Next()
        {
            i += l;
            return i < byteslength;
        }

        // Read returns the next value from the decoder.
        public string Read()
        {
            // Read the length of the string
            int length;
            int n = Varint.Read(bytes, i, out length);
            if (n <= 0)
            {
                err = "StringDecoder: invalid encoded string length";
                return String.Empty;
            }

            // The length of this string plus the length of the variable byte encoded length
            l = length + n;

            int lower = i + n;
            int upper = lower + length;
            if (upper < lower)
            {
                err = "StringDecoder: length overflow";
                return String.Empty;
            }
            if (upper > bytes.Length)
            {
                err = "StringDecoder: not enough data to represent encoded string";
                return String.Empty;
            }
            return System.Text.Encoding.Default.GetString(bytes, lower, length);
        }

        public IClockValue Create(long clock, string value, int quality)
        {
            return new ClockString(clock, value, quality);
        }

        public string Error()
        {
            return err;
        }

    }
}
