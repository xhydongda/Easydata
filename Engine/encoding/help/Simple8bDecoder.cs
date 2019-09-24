using System;

namespace Easydata.Engine
{
    /// <summary>
    /// Decoder converts a compressed byte slice to a stream of unsigned 64bit integers.
    /// <seealso cref="https://github.com/jwilder/encoding/blob/master/simple8b/encoding.go"/>
    /// </summary>
    public class Simple8bDecoder
    {
        // most recently written integers that have not been flushed
        ulong[] buf;
        // current bytes written and flushed
        byte[] bytes;
        int byteIndex;
        int i;
        int n;
        int endIndex;
        public Simple8bDecoder()
        {
            buf = new ulong[240];
        }

        // Next returns true if there are remaining values to be read.  Successive
        // calls to Next advance the current element pointer.
        public bool Next()
        {
            i++;

            if (i >= n)
            {
                read();
            }

            return endIndex - byteIndex >= 8 || (i >= 0 && i < n);
        }

        public void SetBytes(byte[] b, int startindex, int len)
        {
            bytes = b;
            i = 0;
            n = 0;
            byteIndex = startindex;
            endIndex = startindex + len;
        }

        // Read returns the current value.  Successive calls to Read return the same
        // value.
        public ulong Read()
        {
            return buf[i];
        }

        private void read()
        {
            if (endIndex - byteIndex < 8)
            {
                return;
            }
            ulong v = BitConverter.ToUInt64(bytes, byteIndex);
            byteIndex += 8;
            n = Decode(buf, v).Item1;
            i = 0;
        }

        // Count returns the number of integers encoded in the byte slice
        public static (int, string error) CountBytes(byte[] b, int startindex, int len)
        {
            int count = 0;
            int byteindex = startindex;
            int endindex = startindex + len;
            while (endindex - byteindex >= 8)
            {
                ulong v = BitConverter.ToUInt64(b, byteindex);
                byteindex += 8;
                var n = Count(v);
                if (n.error != null)
                    return (0, n.error);
                count += n.Item1;
            }

            if (endindex - byteindex > 0)
            {
                return (0, string.Format("invalid slice len remaining: {0}", endindex - byteindex));
            }
            return (count, null);
        }

        // Count returns the number of integers encoded in the byte slice
        public static (int, string error) CountBytes(ReadOnlySpan<byte> b)
        {
            int count = 0;
            while (b.Length >= 8)
            {
                ulong v = BitConverter.ToUInt64(b);
                b = b.Slice(8);
                var n = Count(v);
                if (n.error != null)
                    return (0, n.error);
                count += n.Item1;
            }
            if (b.Length > 0)
            {
                return (0, string.Format("invalid slice len remaining: {0}", b.Length));
            }
            return (count, null);
        }

        // Count returns the number of integers encoded within an uint64
        public static (int, string error) Count(ulong v)
        {
            ulong sel = v >> 60;
            if (sel >= 16)
            {
                return (0, string.Format("invalid selector value: {0}", sel));
            }
            return (Packing.All[sel].n, null);
        }

        public static (int, string error) Decode(ulong[] dst, ulong v)
        {
            int sel = (int)(v >> 60);
            if (sel >= 16)
            {
                return (0, string.Format("invalid selector value: {0}", sel));
            }
            Packing.All[sel].unpack(v, dst);
            return (Packing.All[sel].n, null);
        }

        // Decode writes the uncompressed values from src to dst.  It returns the number
        // of values written or an error.
        public static (int, string error) DecodeAll(Span<ulong> dst, ReadOnlySpan<byte> src)
        {
            if ((src.Length & 7) != 0)
            {
                return (0, "src length is not multiple of 8");
            }
            int j = 0;
            while (src.Length >= 8)
            {
                ulong v = BitConverter.ToUInt64(src);
                int sel = (int)((v >> 60) & 0xF);
                Packing.All[sel].unpack(v, dst);
                int n = Packing.All[sel].n;
                j += n;
                dst = dst.Slice(n);
                src = src.Slice(8);
            }
            return (j, null);
        }
    }
}
