using System;
using System.Collections.Generic;

namespace Easydata.Engine
{
    /// <summary>
    /// Wal的子对象接口.
    /// </summary>
    public interface IWalEntry
    {
        /// <summary>
        /// 获取子对象的类型.
        /// </summary>
        /// <returns>写入 1、删除 2、范围删除 3</returns>
        byte Type();
        /// <summary>
        /// 将内容编码.
        /// </summary>
        /// <param name="err">错误信息</param>
        /// <returns>编码字节流</returns>
        ByteWriter Marshal(out string err);
        /// <summary>
        /// 解码得到子对象.
        /// </summary>
        /// <param name="b">字节数组</param>
        /// <param name="startindex">开始位置</param>
        /// <param name="endindex">截止位置</param>
        /// <returns>错误信息</returns>
        string UnmarshalBinary(byte[] b, int startindex, int endindex);
        /// <summary>
        /// 字节数.
        /// </summary>
        /// <returns>字节数</returns>
        int MarshalSize();
    }

    /// <summary>
    /// Wal的写入子对象.
    /// </summary>
    public class WriteWalEntry : IWalEntry
    {
        int sz;
        public WriteWalEntry()
        {
            Values = new Dictionary<ulong, ClockValues>();
        }

        public int MarshalSize()
        {
            if (sz > 0 || Values.Count == 0)
                return sz;
            int encLen = 5 * Values.Count;// // Type (1), and Count (4) for each sid

            // determine required length
            foreach (KeyValuePair<ulong, ClockValues> pair in Values)
            {
                ClockValues v = pair.Value;
                encLen += 8;//sid(8)
                int count = v.Count;
                if (count == 0)
                    return 0;
                encLen += 8 * count;//timestamps(8)
                switch (v[0].DataType)
                {
                    case DataTypeEnum.Double:
                    case DataTypeEnum.Integer:
                        encLen += 12 * count;
                        break;
                    case DataTypeEnum.Boolean:
                        encLen += 5 * count;
                        break;
                    case DataTypeEnum.String:
                        foreach (ClockString vv in v)
                        {
                            encLen += 4 + System.Text.Encoding.Default.GetByteCount(vv.Value) + 4;
                        }
                        break;
                    default:
                        return 0;
                }
            }
            sz = encLen;
            return sz;
        }

        // Encode converts the WriteWALEntry into a byte stream using dst if it
        // is large enough.  If dst is too small, the slice will be grown to fit the
        // encoded entry.
        public ByteWriter Marshal(out string err)
        {
            // The entries values are encode as follows:
            //
            // For each key and slice of values, first a 1 byte type for the []Values
            // slice is written.  Following the type, the length and key bytes are written.
            // Following the key, a 4 byte count followed by each value as a 8 byte time
            // and N byte value.  The value is dependent on the type being encoded.  float64,
            // int64, use 8 bytes, boolean uses 1 byte, and string is similar to the key encoding,
            // except that string values have a 4-byte length, and keys only use 2 bytes.
            //
            // This structure is then repeated for each key an value slices.
            //
            // ┌────────────────────────────────────────────────────────────────────┐
            // │                           WriteWALEntry                            │
            // ├──────┬─────────┬────────┬───────┬─────────┬─────────┬───┬──────┬───┤
            // │ Type │ Key Len │   Key  │ Count │  Time   │  Value  │...│ Type │...│
            // │1 byte│ 2 bytes │ N bytes│4 bytes│ 8 bytes │ N bytes │   │1 byte│   │
            // └──────┴─────────┴────────┴───────┴─────────┴─────────┴───┴──────┴───┘
            err = null;
            int encLen = MarshalSize();
            ByteWriter writer = new ByteWriter(encLen);
            // Finally, encode the entry
            byte curType = 0x00;
            foreach (KeyValuePair<ulong, ClockValues> pair in Values)
            {
                ClockValues v = pair.Value;
                curType = v[0].DataType;
                writer.Write(curType);
                writer.Write(pair.Key);
                writer.Write(v.Count);
                foreach (IClockValue vv in v)
                {
                    writer.Write(vv.Clock);
                    if (vv.DataType != curType)
                    {
                        err = string.Format("incorrect value found in {0} slice: {1}", curType, vv.OValue);
                        return null;
                    }
                    switch (vv.DataType)
                    {
                        case DataTypeEnum.Double:
                            writer.Write(((ClockDouble)vv).Value);
                            break;
                        case DataTypeEnum.Integer:
                            writer.Write(((ClockInt64)vv).Value);
                            break;
                        case DataTypeEnum.Boolean:
                            writer.Write(((ClockBoolean)vv).Value);
                            break;
                        case DataTypeEnum.String:
                            writer.Write(((ClockString)vv).Value);
                            break;
                        default:
                            err = string.Format("unsupported value found in {0} slice: {1}", curType, vv.OValue);
                            return null;
                    }
                    writer.Write(vv.Quality);
                }
            }
            return writer;
        }

        public string UnmarshalBinary(byte[] b, int startindex, int endindex)
        {
            Values.Clear();
            int i = startindex;
            while (i < endindex)
            {
                byte typ = b[i];
                i++;
                if (i + 8 > endindex)
                {
                    return Constants.ErrWALCorrupt;
                }
                ulong sid = BitConverter.ToUInt64(b, i);
                i += 8;
                if (i + 4 > endindex)
                {
                    return Constants.ErrWALCorrupt;
                }
                int nvals = BitConverter.ToInt32(b, i);
                i += 4;
                switch (typ)
                {
                    case DataTypeEnum.Double:
                        if (i + 16 * nvals > endindex)
                        {
                            return Constants.ErrWALCorrupt;
                        }
                        ClockValues values = new ClockValues(nvals);
                        for (int j = 0; j < nvals; j++)
                        {
                            long un = BitConverter.ToInt64(b, i);
                            i += 8;
                            double v = FloatDecoder.Float64frombits(BitConverter.ToUInt64(b, i));
                            i += 8;
                            int q = BitConverter.ToInt32(b, i);
                            i += 4;
                            values.Append(new ClockDouble(un, v, q));
                        }
                        Values[sid] = values;
                        break;
                    case DataTypeEnum.Integer:
                        if (i + 16 * nvals > endindex)
                        {
                            return Constants.ErrWALCorrupt;
                        }
                        values = new ClockValues(nvals);
                        for (int j = 0; j < nvals; j++)
                        {
                            long un = BitConverter.ToInt64(b, i);
                            i += 8;
                            long v = (long)BitConverter.ToUInt64(b, i);
                            i += 8;
                            int q = BitConverter.ToInt32(b, i);
                            i += 4;
                            values.Append(new ClockInt64(un, v, q));
                        }
                        Values[sid] = values;
                        break;
                    case DataTypeEnum.Boolean:
                        if (i + 9 * nvals > endindex)
                        {
                            return Constants.ErrWALCorrupt;
                        }
                        values = new ClockValues(nvals);
                        for (int j = 0; j < nvals; j++)
                        {
                            long un = BitConverter.ToInt64(b, i);
                            i += 8;
                            bool v = b[i] == 1 ? true : false;
                            i += 1;
                            int q = BitConverter.ToInt32(b, i);
                            i += 4;
                            values.Append(new ClockBoolean(un, v, q));
                        }
                        Values[sid] = values;
                        break;
                    case DataTypeEnum.String:
                        values = new ClockValues(nvals);
                        for (int j = 0; j < nvals; j++)
                        {
                            if (i + 12 > endindex)
                            {
                                return Constants.ErrWALCorrupt;
                            }
                            long un = BitConverter.ToInt64(b, i);
                            i += 8;
                            int length = BitConverter.ToInt32(b, i);
                            i += 4;
                            if (i + length > endindex)
                            {
                                return Constants.ErrWALCorrupt;
                            }
                            string v = System.Text.Encoding.Default.GetString(b, i, length);
                            i += length;
                            int q = BitConverter.ToInt32(b, i);
                            i += 4;
                            values.Append(new ClockString(un, v, q));
                        }
                        Values[sid] = values;
                        break;
                    default:
                        return string.Format("unsupported value type: {0}", typ);
                }
            }
            return null;
        }

        public byte Type()
        {
            return Constants.WriteWALEntryType;
        }

        public Dictionary<ulong, ClockValues> Values { get; set; }
    }

    /// <summary>
    /// WAL的删除子对象.
    /// </summary>
    public class DeleteWalEntry : IWalEntry
    {
        int sz;
        public DeleteWalEntry()
        {
            Sids = new List<ulong>();
        }

        public string UnmarshalBinary(byte[] b, int startindex, int endindex)
        {
            if (endindex - startindex < 8 || (endindex - startindex) % 8 != 0)
            {
                return Constants.ErrWALCorrupt;
            }
            Sids.Clear();
            int i = startindex;
            while (i < endindex)
            {
                Sids.Add(BitConverter.ToUInt64(b, i));
                i += 8;
            }
            return null;
        }

        public List<ulong> Sids { get; set; }

        public int MarshalSize()
        {
            sz = 8 * Sids.Count;
            return sz;
        }

        public ByteWriter Marshal(out string err)
        {
            err = null;
            int size = MarshalSize();
            ByteWriter writer = new ByteWriter(size);
            foreach (ulong sid in Sids)
            {
                writer.Write(sid);
            }
            return writer;
        }

        public byte Type()
        {
            return Constants.DeleteWALEntryType;
        }
    }

    /// <summary>
    /// WAL的范围删除子对象.
    /// </summary>
    public class DeleteRangeWalEntry : IWalEntry
    {
        int sz;
        public DeleteRangeWalEntry()
        {
            Sids = new List<ulong>();
        }

        public string UnmarshalBinary(byte[] b, int startindex, int endindex)
        {
            if (endindex - startindex < 16)
            {
                return Constants.ErrWALCorrupt;
            }
            int i = startindex;
            Min = BitConverter.ToInt64(b, i);
            Max = BitConverter.ToInt64(b, i + 8);
            i += 16;
            Sids.Clear();
            while (i < endindex)
            {
                if (i + 8 > endindex)
                {
                    return Constants.ErrWALCorrupt;
                }
                Sids.Add(BitConverter.ToUInt64(b, i));
                i += 8;
            }
            return null;
        }

        public List<ulong> Sids { get; set; }

        public long Min { get; set; }

        public long Max { get; set; }

        public int MarshalSize()
        {
            sz = 16 + 8 * Sids.Count;
            return sz;
        }

        public ByteWriter Marshal(out string err)
        {
            err = null;
            int size = MarshalSize();
            ByteWriter writer = new ByteWriter(size);
            writer.Write(Min);
            writer.Write(Max);
            foreach (ulong sid in Sids)
            {
                writer.Write(sid);
            }
            return writer;
        }

        public byte Type()
        {
            return Constants.DeleteRangeWALEntryType;
        }
    }

}
