using System;

namespace Arim.Encoding.Binary
{
    /// <summary>
    /// <seealso cref=">https://github.com/tabrath/BinaryEncoding"/>
    /// </summary>
    public static class Varint
    {
        public const int MaxVarintLen16 = 3;
        public const int MaxVarintLen32 = 5;
        public const int MaxVarintLen64 = 10;

        public static int GetByteCount(ulong value)
        {
            int i;
            for (i = 0; i < 9 && value >= 0x80; i++, value >>= 7) { }
            return i + 1;
        }

        public static byte[] GetBytes(int value) { return GetBytes((long)value); }
        public static byte[] GetBytes(long value)
        {
            var ux = (ulong)value << 1;
            if (value < 0)
                ux ^= ux;
            return GetBytes(ux);
        }

        public static byte[] GetBytes(ulong value)
        {
            var buffer = new byte[GetByteCount(value)];
            Write(buffer, 0, value);
            return buffer;
        }

        //与varint.go PutUvarint一致.
        public static int Write(byte[] buffer, int offset, ulong value)
        {
            int i = 0;
            while (value >= 0x80)
            {
                buffer[offset + i] = (byte)(value | 0x80);
                value >>= 7;
                i++;
            }
            buffer[offset + i] = (byte)value;
            return i + 1;
        }

        public static int Read(byte[] buffer, int offset, out ushort value)
        {
            ulong l;
            var n = Read(buffer, offset, out l);
            value = (ushort)l;
            return n;
        }

        public static int Read(byte[] buffer, int offset, out uint value)
        {
            ulong l;
            var n = Read(buffer, offset, out l);
            value = (uint)l;
            return n;
        }

        public static int Read(byte[] buffer, int offset, out ulong value)
        {
            value = 0;
            int s = 0;
            for (var i = 0; i < buffer.Length - offset; i++)
            {
                if (buffer[offset + i] < 0x80)
                {
                    if (i > 9 || i == 9 && buffer[offset + i] > 1)
                    {
                        value = 0;
                        return -(i + 1); // overflow
                    }
                    value |= (ulong)buffer[offset + i] << s;
                    return i + 1;
                }
                value |= (ulong)(buffer[offset + i] & 0x7f) << s;
                s += 7;
            }
            value = 0;
            return 0;
        }

        public static int Read(byte[] buffer, int offset, out short value)
        {
            long l;
            var n = Read(buffer, offset, out l);
            value = (short)l;
            return n;
        }

        public static int Read(byte[] buffer, int offset, out int value)
        {
            long l;
            var n = Read(buffer, offset, out l);
            value = (int)l;
            return n;
        }

        public static int Read(byte[] buffer, int offset, out long value)
        {
            ulong ux;
            int n = Read(buffer, offset, out ux);
            value = (long)(ux >> 1);
            if ((ux & 1) != 0)
                value ^= value;
            return n;
        }

        //与varint.go PutUvarint一致.
        public static int Write(Span<byte> buffer, ulong value)
        {
            int i = 0;
            while (value >= 0x80)
            {
                buffer[i] = (byte)(value | 0x80);
                value >>= 7;
                i++;
            }
            buffer[i] = (byte)value;
            return i + 1;
        }

        public static int Read(Span<byte> buffer, out ushort value)
        {
            ulong l;
            var n = Read(buffer, out l);
            value = (ushort)l;
            return n;
        }

        public static int Read(Span<byte> buffer, out uint value)
        {
            ulong l;
            var n = Read(buffer, out l);
            value = (uint)l;
            return n;
        }

        public static int Read(Span<byte> buffer, out ulong value)
        {
            value = 0;
            int s = 0;
            for (var i = 0; i < buffer.Length; i++)
            {
                if (buffer[i] < 0x80)
                {
                    if (i > 9 || i == 9 && buffer[i] > 1)
                    {
                        value = 0;
                        return -(i + 1); // overflow
                    }
                    value |= (ulong)buffer[i] << s;
                    return i + 1;
                }
                value |= (ulong)(buffer[i] & 0x7f) << s;
                s += 7;
            }
            value = 0;
            return 0;
        }

        public static int Read(Span<byte> buffer, out short value)
        {
            long l;
            var n = Read(buffer, out l);
            value = (short)l;
            return n;
        }

        public static int Read(Span<byte> buffer, out int value)
        {
            long l;
            var n = Read(buffer, out l);
            value = (int)l;
            return n;
        }

        public static int Read(Span<byte> buffer, out long value)
        {
            ulong ux;
            int n = Read(buffer, out ux);
            value = (long)(ux >> 1);
            if ((ux & 1) != 0)
                value ^= value;
            return n;
        }
    }
}
