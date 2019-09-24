using System;
using System.Buffers;
using System.Collections.Generic;

namespace Easydata.Engine
{
    public class ByteWriter
    {
        byte[] buffer;
        int i;
        public ByteWriter(int length)
        {
            buffer = ArrayPool<byte>.Shared.Rent(length);
            i = 0;
        }

        public int Length { get { return i; } set { i = value; } }

        public void Write(byte v)
        {
            buffer[i++] = v;
        }

        public void Write(bool v)
        {
            if (v) buffer[i++] = 0x01;
            else buffer[i++] = 0x00;
        }

        public void Write(ushort v)
        {
            buffer[i++] = (byte)v;
            buffer[i++] = (byte)(v >> 8);
        }

        public void Write(int v)
        {
            buffer[i++] = (byte)v;
            buffer[i++] = (byte)(v >> 8);
            buffer[i++] = (byte)(v >> 16);
            buffer[i++] = (byte)(v >> 24);
        }

        public void Write(uint v)
        {
            buffer[i++] = (byte)v;
            buffer[i++] = (byte)(v >> 8);
            buffer[i++] = (byte)(v >> 16);
            buffer[i++] = (byte)(v >> 24);
        }

        public void Write(long v)
        {
            buffer[i++] = (byte)v;
            buffer[i++] = (byte)(v >> 8);
            buffer[i++] = (byte)(v >> 16);
            buffer[i++] = (byte)(v >> 24);
            buffer[i++] = (byte)(v >> 32);
            buffer[i++] = (byte)(v >> 40);
            buffer[i++] = (byte)(v >> 48);
            buffer[i++] = (byte)(v >> 56);
        }

        public void Write(ulong v)
        {
            buffer[i++] = (byte)v;
            buffer[i++] = (byte)(v >> 8);
            buffer[i++] = (byte)(v >> 16);
            buffer[i++] = (byte)(v >> 24);
            buffer[i++] = (byte)(v >> 32);
            buffer[i++] = (byte)(v >> 40);
            buffer[i++] = (byte)(v >> 48);
            buffer[i++] = (byte)(v >> 56);
        }

        public void Write(double v)
        {
            Write(FloatEncoder.Float64bits(v));
        }

        public void Write(string v)
        {
            byte[] strBytes = System.Text.Encoding.Default.GetBytes(v);
            int len = strBytes.Length;
            Write(len);
            for (int j = 0; j < len; j++)
            {
                buffer[i++] = strBytes[j];
            }
        }

        public void Write(byte[] v)
        {
            unsafe
            {
                fixed (byte* pb = &buffer[i], pv = v)
                {
                    for (int j = 0; j < v.Length; j++)
                    {
                        *(pb+j) = *(pv+j);
                    }
                    i += v.Length;
                }
            }
        }
        public void Write(byte[] v, int start, int length)
        {
            for (int j = start; j < start + length; j++)
            {
                buffer[i++] = v[j];
            }
        }

        public void Write(IEnumerable<byte> v)
        {
            foreach (byte b in v)
            {
                buffer[i++] = b;
            }
        }
        public byte[] EndWrite()
        {
            return buffer;
        }
        public byte[] EndWriteCopy()
        {
            byte[] result = new byte[i];
            System.Array.Copy(buffer, result, i);
            return result;
        }

        public void Release()
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
        #region static
        public static int Write(byte[] buffer, int start, ulong v)
        {
            buffer[start++] = (byte)v;
            buffer[start++] = (byte)(v >> 8);
            buffer[start++] = (byte)(v >> 16);
            buffer[start++] = (byte)(v >> 24);
            buffer[start++] = (byte)(v >> 32);
            buffer[start++] = (byte)(v >> 40);
            buffer[start++] = (byte)(v >> 48);
            buffer[start++] = (byte)(v >> 56);
            return start;
        }
        public static int WriteBigEndian(byte[] buffer, int start, ulong v)
        {
            buffer[start++] = (byte)(v >> 56);
            buffer[start++] = (byte)(v >> 48);
            buffer[start++] = (byte)(v >> 40);
            buffer[start++] = (byte)(v >> 32);
            buffer[start++] = (byte)(v >> 24);
            buffer[start++] = (byte)(v >> 16);
            buffer[start++] = (byte)(v >> 8);
            buffer[start++] = (byte)v;
            return start;
        }

        public static ulong ReadBigEndian(Span<byte> buffer)
        {
            ulong result = 0;
            result |= (ulong)buffer[0] << 56;
            result |= (ulong)buffer[1] << 48;
            result |= (ulong)buffer[2] << 40;
            result |= (ulong)buffer[3] << 32;
            result |= (ulong)buffer[4] << 24;
            result |= (ulong)buffer[5] << 16;
            result |= (ulong)buffer[6] << 8;
            result |= buffer[7];
            return result;
        }
        #endregion
    }
}
