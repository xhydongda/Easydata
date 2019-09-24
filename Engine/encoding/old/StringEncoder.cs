using Arim.Encoding.Binary;
using System.Collections.Generic;

namespace Easydata.Engine
{
    /// <summary>
    /// boolean encoding uses 1 bit per value.  Each compressed byte slice contains a 1 byte header
    /// indicating the compression type, followed by a variable byte encoded length indicating
    /// how many booleans are packed in the slice.  The remaining bytes contains 1 byte for every
    /// 8 boolean values encoded.
    /// </summary>
    public class StringEncoder : IEncoder<string>
    {
        // stringUncompressed is a an uncompressed format encoding strings as raw bytes.
        // Not yet implemented.
        public const byte stringUncompressed = 0;
        // stringCompressedSnappy is a compressed encoding using Snappy compression
        public const byte stringCompressedSnappy = 1;
        // The encoded bytes
        List<byte> bytes;

        // NewStringEncoder returns a new StringEncoder with an initial buffer ready to hold sz bytes.
        public StringEncoder(int sz)
        {
            bytes = new List<byte>(sz);
        }

        // Flush is no-op
        public void Flush() { }

        // Reset sets the encoder back to its initial state.
        public void Reset()
        {
            bytes.Clear();
        }

        // Write encodes s to the underlying buffer.
        public void Write(string str)
        {
            // Append the length of the string using variable byte encoding
            byte[] strBytes = System.Text.Encoding.Default.GetBytes(str);
            bytes.AddRange(Varint.GetBytes(strBytes.Length));

            // Append the string bytes
            bytes.AddRange(strBytes);
        }

        // Bytes returns a copy of the underlying buffer.
        public (ByteWriter, string) Bytes()
        {
            // Compress the currently appended bytes using snappy and prefix with
            // a 1 byte header for future extension
            ByteWriter result = new ByteWriter(bytes.Count + 1);
            result.Write((byte)(stringCompressedSnappy << 4));
            //TODO: ToArray 多了一次拷贝，怎么避免？
            result.Write(SnappyPI.SnappyCodec.Compress(bytes.ToArray(), 0, bytes.Count));
            return (result, null);
        }

    }
}
