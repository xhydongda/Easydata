using Arim.Encoding.Binary;
using System;
using System.Buffers;
using System.Collections.Generic;

namespace Easydata.Engine
{
    public class Encoding
    {
        public static ulong ZigZagEncode(long x)
        {
            return (((ulong)(x << 1)) ^ ((ulong)(x >> 63)));
        }

        public static long ZigZagDecode(ulong v)
        {
            return (long)((v >> 1) ^ ((ulong)((((long)(v & 1)) << 63) >> 63)));
        }

        // Clz counts leading zeroes
        public static int Clz(ulong x)
        {
            int n;

            n = 1;

            if ((x >> 32) == 0)
            {
                n = n + 32;
                x = x << 32;
            }
            if ((x >> (32 + 16)) == 0)
            {
                n = n + 16;
                x = x << 16;
            }
            if ((x >> (32 + 16 + 8)) == 0)
            {
                n = n + 8;
                x = x << 8;
            }
            if ((x >> (32 + 16 + 8 + 4)) == 0)
            {
                n = n + 4;
                x = x << 4;
            }
            if ((x >> (32 + 16 + 8 + 4 + 2)) == 0)
            {
                n = n + 2;
                x = x << 2;
            }
            n = n - (int)(x >> 63);
            return n;
        }

        // Ctz counts trailing zeroes
        public static int Ctz(ulong x)
        {
            if (x == 0)
            {
                return 64;
            }
            int n = 0;

            if ((x & 0x00000000FFFFFFFF) == 0)
            {
                n = n + 32;
                x = x >> 32;
            }
            if ((x & 0x000000000000FFFF) == 0)
            {
                n = n + 16;
                x = x >> 16;
            }
            if ((x & 0x00000000000000FF) == 0)
            {
                n = n + 8;
                x = x >> 8;
            }
            if ((x & 0x000000000000000F) == 0)
            {
                n = n + 4;
                x = x >> 4;
            }
            if ((x & 0x0000000000000003) == 0)
            {
                n = n + 2;
                x = x >> 2;
            }
            if ((x & 0x0000000000000001) == 0)
            {
                n = n + 1;
            }
            return n;
        }

        public static (ByteWriter, string error) Encode2(IList<IClockValue> values, int startIndex, int count)
        {
            if (values == null || values.Count == 0)
            {
                return (null, null);
            }
            IClockValue value0 = values[0];
            byte datatype = value0.DataType;
            TimeEncoder tenc = (TimeEncoder)EncoderFactory.Get(DataTypeEnum.DateTime, count);
            IntegerEncoder qenc = (IntegerEncoder)EncoderFactory.Get(DataTypeEnum.Integer, count);
            IEncoder venc = EncoderFactory.Get(datatype, count);
            if (datatype == DataTypeEnum.Double)
            {
                FloatEncoder encoder = (FloatEncoder)venc;
                for (int i = startIndex; i < startIndex + count; i++)
                {
                    ClockDouble value = (ClockDouble)values[i];
                    tenc.Write(value.Clock);
                    qenc.Write(value.Quality);
                    encoder.Write(value.Value);
                }
            }
            else if (datatype == DataTypeEnum.Boolean)
            {
                BooleanEncoder encoder = (BooleanEncoder)venc;
                for (int i = startIndex; i < startIndex + count; i++)
                {
                    ClockBoolean value = (ClockBoolean)values[i];
                    tenc.Write(value.Clock);
                    qenc.Write(value.Quality);
                    encoder.Write(value.Value);
                }
            }
            else if (datatype == DataTypeEnum.Integer)
            {
                IntegerEncoder encoder = (IntegerEncoder)venc;
                for (int i = startIndex; i < startIndex + count; i++)
                {
                    ClockInt64 value = (ClockInt64)values[i];
                    tenc.Write(value.Clock);
                    qenc.Write(value.Quality);
                    encoder.Write(value.Value);
                }
            }
            else if (datatype == DataTypeEnum.String)
            {
                StringEncoder encoder = (StringEncoder)venc;
                for (int i = startIndex; i < startIndex + count; i++)
                {
                    ClockString value = (ClockString)values[i];
                    tenc.Write(value.Clock);
                    qenc.Write(value.Quality);
                    encoder.Write(value.Value);
                }
            }
            var ts = tenc.Bytes();
            venc.Flush();//for floatencoder
            var qs = qenc.Bytes();
            var vs = venc.Bytes();
            EncoderFactory.Put(DataTypeEnum.DateTime, tenc);
            EncoderFactory.Put(DataTypeEnum.Integer, qenc);
            EncoderFactory.Put(datatype, venc);
            if (ts.Item2 != null)
            {
                return (null, ts.Item2);
            }
            if (vs.Item2 != null)
            {
                return (null, vs.Item2);
            }
            if (qs.Item2 != null)
            {
                return (null, qs.Item2);
            }
            ByteWriter tswriter = ts.Item1;
            ByteWriter vswriter = vs.Item1;
            ByteWriter qswriter = qs.Item1;
            ByteWriter result = new ByteWriter(1 + Varint.MaxVarintLen64 + tswriter.Length + vswriter.Length + qswriter.Length);
            result.Write(datatype); //first byte valuetype 
            result.Write(Varint.GetBytes(tswriter.Length));
            result.Write(tswriter.EndWrite(), 0, tswriter.Length);
            tswriter.Release();
            result.Write(Varint.GetBytes(vswriter.Length));
            result.Write(vswriter.EndWrite(), 0, vswriter.Length);
            vswriter.Release();
            result.Write(qswriter.EndWrite(), 0, qswriter.Length);
            qswriter.Release();
            return (result, null);
        }

        public static (ByteWriter, string error) Encode(IList<IClockValue> values, int startIndex, int count)
        {
            if (values == null || values.Count == 0)
            {
                return (null, null);
            }
            IClockValue value0 = values[0];
            byte datatype = value0.DataType;
            long[] ts = ArrayPool<long>.Shared.Rent(count);
            Span<long> ts_span = new Span<long>(ts, 0, count);
            long[] qs = ArrayPool<long>.Shared.Rent(count);
            Span<long> qs_span = new Span<long>(qs, 0, count);

            ByteWriter tswriter = null, vswriter = null, qswriter = null;
            string tserror = null, vserror = null, qserror = null;
            if (datatype == DataTypeEnum.Double)
            {
                double[] vs = ArrayPool<double>.Shared.Rent(count);
                Span<double> vs_span = new Span<double>(vs, 0, count);
                int j = 0;
                for (int i = startIndex; i < startIndex + count; i++, j++)
                {
                    ClockDouble value = (ClockDouble)values[i];
                    ts_span[j] = value.Clock;
                    vs_span[j] = value.Value;
                    qs_span[j] = value.Quality;
                }
                BatchDouble encoder = (BatchDouble)CoderFactory.Get(datatype);
                (vswriter, vserror) = encoder.EncodingAll(vs_span);
                ArrayPool<double>.Shared.Return(vs);
                CoderFactory.Put(datatype, encoder);
            }
            else if (datatype == DataTypeEnum.Boolean)
            {
                bool[] vs = ArrayPool<bool>.Shared.Rent(count);
                Span<bool> vs_span = new Span<bool>(vs, 0, count);
                int j = 0;
                for (int i = startIndex; i < startIndex + count; i++, j++)
                {
                    ClockBoolean value = (ClockBoolean)values[i];
                    ts_span[j] = value.Clock;
                    vs_span[j] = value.Value;
                    qs_span[j] = value.Quality;
                }
                BatchBoolean encoder = (BatchBoolean)CoderFactory.Get(datatype);
                (vswriter, vserror) = encoder.EncodingAll(vs_span);
                ArrayPool<bool>.Shared.Return(vs);
                CoderFactory.Put(datatype, encoder);
            }
            else if (datatype == DataTypeEnum.Integer)
            {
                long[] vs = ArrayPool<long>.Shared.Rent(count);
                Span<long> vs_span = new Span<long>(vs, 0, count);
                int j = 0;
                for (int i = startIndex; i < startIndex + count; i++, j++)
                {
                    ClockInt64 value = (ClockInt64)values[i];
                    ts_span[j] = value.Clock;
                    vs_span[j] = value.Value;
                    qs_span[j] = value.Quality;
                }
                BatchInt64 encoder = (BatchInt64)CoderFactory.Get(datatype);
                (vswriter, vserror) = encoder.EncodingAll(vs_span);
                ArrayPool<long>.Shared.Return(vs);
                CoderFactory.Put(datatype, encoder);
            }
            else if (datatype == DataTypeEnum.String)
            {
                string[] vs = ArrayPool<string>.Shared.Rent(count);
                Span<string> vs_span = new Span<string>(vs, 0, count);
                int j = 0;
                for (int i = startIndex; i < startIndex + count; i++, j++)
                {
                    ClockString value = (ClockString)values[i];
                    ts_span[j] = value.Clock;
                    vs_span[j] = value.Value;
                    qs_span[j] = value.Quality;
                }
                BatchString encoder = (BatchString)CoderFactory.Get(datatype);
                (vswriter, vserror) = encoder.EncodingAll(vs_span);
                ArrayPool<string>.Shared.Return(vs);
                CoderFactory.Put(datatype, encoder);
            }
            bool good = (vserror == null && vswriter != null);
            if (good)
            {
                BatchTimeStamp tenc = (BatchTimeStamp)CoderFactory.Get(DataTypeEnum.DateTime);
                (tswriter, tserror) = tenc.EncodingAll(ts_span);
                CoderFactory.Put(DataTypeEnum.DateTime, tenc);
                good = (tswriter != null && tserror == null);
                if (good)
                {
                    BatchInt64 qenc = (BatchInt64)CoderFactory.Get(DataTypeEnum.Integer);
                    (qswriter, qserror) = qenc.EncodingAll(qs_span);
                    CoderFactory.Put(DataTypeEnum.Integer, qenc);
                    good = (qswriter != null && qserror == null);
                }
            }
            ArrayPool<long>.Shared.Return(ts);
            ArrayPool<long>.Shared.Return(qs);
            if (good)
            {
                ByteWriter result = new ByteWriter(1 + Varint.MaxVarintLen64 + tswriter.Length + vswriter.Length + qswriter.Length);
                result.Write(datatype); //first byte valuetype 
                result.Write(Varint.GetBytes(tswriter.Length));
                result.Write(tswriter.EndWrite(), 0, tswriter.Length);
                result.Write(Varint.GetBytes(vswriter.Length));
                result.Write(vswriter.EndWrite(), 0, vswriter.Length);
                result.Write(Varint.GetBytes(qswriter.Length));
                result.Write(qswriter.EndWrite(), 0, qswriter.Length);
                return (result, null);
            }
            if (tswriter != null) tswriter.Release();
            if (vswriter != null) vswriter.Release();
            if (qswriter != null) qswriter.Release();
            return (null, vserror != null ? vserror : tserror != null ? tserror : qserror);
        }

        // BlockCount returns the number of timestamps encoded in block.
        public static int BlockCount(byte[] bytes)
        {
            int tsLen, index;
            index = 1 + Varint.Read(bytes, 1, out tsLen);
            return TimeDecoder.CountTimestamps(bytes, index, tsLen);
        }

        public static ClockValues Decode2(byte[] bytes, int startindex)
        {
            byte datatype = bytes[startindex];
            startindex++;
            int index;
            index = startindex + Varint.Read(bytes, startindex, out int tsLen);//时标段;
            TimeDecoder tdec = (TimeDecoder)DecoderFactory.Get(DataTypeEnum.DateTime);
            tdec.SetBytes(bytes, index, tsLen);
            index = index + tsLen;
            index = index + Varint.Read(bytes, index, out int vsLen);//数值段;
            IDecoder vdec = DecoderFactory.Get(datatype);
            vdec.SetBytes(bytes, index, vsLen);
            index = index + vsLen;
            index = index + Varint.Read(bytes, index, out int qsLen);//质量段;
            IntegerDecoder qdec = (IntegerDecoder)DecoderFactory.Get(DataTypeEnum.Integer);
            qdec.SetBytes(bytes, index, qsLen);
            ClockValues result = new ClockValues();
            if (datatype == DataTypeEnum.Double)
            {
                FloatDecoder decoder = (FloatDecoder)vdec;
                while (tdec.Next() && vdec.Next() && qdec.Next())
                {
                    result.Append(decoder.Create(tdec.Read(), decoder.Read(), (int)qdec.Read()));
                }
            }
            else if (datatype == DataTypeEnum.Boolean)
            {
                BooleanDecoder decoder = (BooleanDecoder)vdec;
                while (tdec.Next() && vdec.Next() && qdec.Next())
                {
                    result.Append(decoder.Create(tdec.Read(), decoder.Read(), (int)qdec.Read()));
                }
            }
            else if (datatype == DataTypeEnum.Integer)
            {
                IntegerDecoder decoder = (IntegerDecoder)vdec;
                while (tdec.Next() && vdec.Next() && qdec.Next())
                {
                    result.Append(decoder.Create(tdec.Read(), decoder.Read(), (int)qdec.Read()));
                }
            }
            else if (datatype == DataTypeEnum.String)
            {
                StringDecoder decoder = (StringDecoder)vdec;
                while (tdec.Next() && vdec.Next() && qdec.Next())
                {
                    result.Append(decoder.Create(tdec.Read(), decoder.Read(), (int)qdec.Read()));
                }
            }
            DecoderFactory.Put(DataTypeEnum.DateTime, tdec);
            DecoderFactory.Put(DataTypeEnum.Integer, qdec);
            DecoderFactory.Put(datatype, vdec);
            return result;
        }

        public static ClockValues Decode2(byte[] bytes, int startindex, long start, long end)
        {
            byte datatype = bytes[startindex];
            startindex++;
            int tsLen, index;
            index = startindex + Varint.Read(bytes, startindex, out tsLen);//时标段;
            TimeDecoder tdec = (TimeDecoder)DecoderFactory.Get(DataTypeEnum.DateTime);
            tdec.SetBytes(bytes, index, tsLen);
            int vsLen;
            index = index + tsLen + Varint.Read(bytes, index, out vsLen);//数值段;
            IDecoder vdec = DecoderFactory.Get(datatype);
            vdec.SetBytes(bytes, index, vsLen);
            index = index + vsLen;
            int qsLen = bytes.Length - index - vsLen;//质量段.
            IntegerDecoder qdec = (IntegerDecoder)DecoderFactory.Get(DataTypeEnum.Integer);
            qdec.SetBytes(bytes, index, qsLen);
            ClockValues result = new ClockValues();
            if (datatype == DataTypeEnum.Double)
            {
                FloatDecoder decoder = (FloatDecoder)vdec;
                while (tdec.Next() && vdec.Next() && qdec.Next())
                {
                    long clock = tdec.Read();
                    if (clock > end)
                    {
                        break;
                    }
                    if (clock >= start)
                    {
                        result.Append(decoder.Create(tdec.Read(), decoder.Read(), (int)qdec.Read()));
                    }
                }
            }
            else if (datatype == DataTypeEnum.Boolean)
            {
                BooleanDecoder decoder = (BooleanDecoder)vdec;
                while (tdec.Next() && vdec.Next() && qdec.Next())
                {
                    long clock = tdec.Read();
                    if (clock > end)
                    {
                        break;
                    }
                    if (clock >= start)
                    {
                        result.Append(decoder.Create(tdec.Read(), decoder.Read(), (int)qdec.Read()));
                    }
                }
            }
            else if (datatype == DataTypeEnum.Integer)
            {
                IntegerDecoder decoder = (IntegerDecoder)vdec;
                while (tdec.Next() && vdec.Next() && qdec.Next())
                {
                    long clock = tdec.Read();
                    if (clock > end)
                    {
                        break;
                    }
                    if (clock >= start)
                    {
                        result.Append(decoder.Create(tdec.Read(), decoder.Read(), (int)qdec.Read()));
                    }
                }
            }
            else if (datatype == DataTypeEnum.String)
            {
                StringDecoder decoder = (StringDecoder)vdec;
                while (tdec.Next() && vdec.Next() && qdec.Next())
                {
                    long clock = tdec.Read();
                    if (clock > end)
                    {
                        break;
                    }
                    if (clock >= start)
                    {
                        result.Append(decoder.Create(tdec.Read(), decoder.Read(), (int)qdec.Read()));
                    }
                }
            }
            DecoderFactory.Put(DataTypeEnum.DateTime, tdec);
            DecoderFactory.Put(DataTypeEnum.Integer, qdec);
            DecoderFactory.Put(datatype, vdec);
            return result;
        }

        public static ClockValues Decode(byte[] bytes, int startindex)
        {
            Span<byte> span = new Span<byte>(bytes);
            byte datatype = bytes[startindex];
            span = span.Slice(1);
            //解析时标段;
            int nn = Varint.Read(span, out int tsLen);
            Span<byte> ts_src_span = span.Slice(nn, tsLen);
            long[] ts = ArrayPool<long>.Shared.Rent(Constants.DefaultMaxPointsPerBlock);
            Span<long> ts_to_span = new Span<long>(ts, 0, Constants.DefaultMaxPointsPerBlock);
            BatchTimeStamp tdec = (BatchTimeStamp)CoderFactory.Get(DataTypeEnum.DateTime);
            (int tscount, string tserror) = tdec.DecodeAll(ts_src_span, ts_to_span);
            CoderFactory.Put(DataTypeEnum.DateTime, tdec);
            if (tscount == 0 || tserror != null)
            {
                ArrayPool<long>.Shared.Return(ts);
                return null;
            }
            span = span.Slice(nn + tsLen);
            //读取数值段.
            nn = Varint.Read(span, out int vsLen);
            Span<byte> vs_src_span = span.Slice(nn, vsLen);
            span = span.Slice(nn + vsLen);
            //解析质量段.
            nn = Varint.Read(span, out int qsLen);
            Span<byte> qs_src_span = span.Slice(nn, qsLen);
            BatchInt64 qdec = (BatchInt64)CoderFactory.Get(DataTypeEnum.Integer);
            long[] qs = ArrayPool<long>.Shared.Rent(tscount);
            Span<long> qs_to_span = new Span<long>(qs, 0, tscount);
            (int qscount, string qserror) = qdec.DecodeAll(qs_src_span, qs_to_span);
            CoderFactory.Put(DataTypeEnum.Integer, qdec);
            if (qscount != tscount || qserror != null)
            {
                ArrayPool<long>.Shared.Return(qs);
                return null;
            }
            //解析数据段.
            ClockValues result = null;
            if (datatype == DataTypeEnum.Double)
            {
                BatchDouble decoder = (BatchDouble)CoderFactory.Get(datatype);
                double[] vs = ArrayPool<double>.Shared.Rent(tscount + 1);
                Span<double> vs_to_span = new Span<double>(vs, 0, tscount + 1);
                (int vscount, string vserror) = decoder.DecodeAll(vs_src_span, vs_to_span);
                CoderFactory.Put(datatype, decoder);
                if (vscount >= tscount && vserror == null)
                {
                    result = new ClockValues(tscount);
                    for (int i = 0; i < tscount; i++)
                    {
                        result.Append(new ClockDouble(ts_to_span[i], vs_to_span[i], (int)qs_to_span[i]));
                    }
                }
                ArrayPool<double>.Shared.Return(vs);
            }
            else if (datatype == DataTypeEnum.Boolean)
            {
                BatchBoolean decoder = (BatchBoolean)CoderFactory.Get(datatype);
                bool[] vs = ArrayPool<bool>.Shared.Rent(tscount);
                Span<bool> vs_to_span = new Span<bool>(vs, 0, tscount);
                (int vscount, string vserror) = decoder.DecodeAll(vs_src_span, vs_to_span);
                CoderFactory.Put(datatype, decoder);
                if (vscount == tscount && vserror == null)
                {
                    result = new ClockValues(tscount);
                    for (int i = 0; i < tscount; i++)
                    {
                        result.Append(new ClockBoolean(ts_to_span[i], vs_to_span[i], (int)qs_to_span[i]));
                    }
                }
                ArrayPool<bool>.Shared.Return(vs);
            }
            else if (datatype == DataTypeEnum.Integer)
            {
                BatchInt64 decoder = (BatchInt64)CoderFactory.Get(datatype);
                long[] vs = ArrayPool<long>.Shared.Rent(tscount);
                Span<long> vs_to_span = new Span<long>(vs, 0, tscount);
                (int vscount, string vserror) = decoder.DecodeAll(vs_src_span, vs_to_span);
                CoderFactory.Put(datatype, decoder);
                if (vscount == tscount && vserror == null)
                {
                    result = new ClockValues(tscount);
                    for (int i = 0; i < tscount; i++)
                    {
                        result.Append(new ClockInt64(ts_to_span[i], vs_to_span[i], (int)qs_to_span[i]));
                    }
                }
                ArrayPool<long>.Shared.Return(vs);
            }
            else if (datatype == DataTypeEnum.String)
            {
                BatchString decoder = (BatchString)CoderFactory.Get(datatype);
                string[] vs = ArrayPool<string>.Shared.Rent(tscount);
                Span<string> vs_to_span = new Span<string>(vs, 0, tscount);
                (int vscount, string vserror) = decoder.DecodeAll(vs_src_span, vs_to_span);
                CoderFactory.Put(datatype, decoder);
                if (vscount == tscount && vserror == null)
                {
                    result = new ClockValues(tscount);
                    for (int i = 0; i < tscount; i++)
                    {
                        result.Append(new ClockString(ts_to_span[i], vs_to_span[i], (int)qs_to_span[i]));
                    }
                }
                ArrayPool<string>.Shared.Return(vs);
            }
            return result;
        }

        public static ClockValues Decode(byte[] bytes, int startindex, long start, long end)
        {
            Span<byte> span = new Span<byte>(bytes);
            byte datatype = bytes[startindex];
            span = span.Slice(1);
            //解析时标段;
            int nn = Varint.Read(span, out int tsLen);
            span = span.Slice(nn);
            Span<byte> ts_src_span = span.Slice(0, tsLen);
            long[] ts = ArrayPool<long>.Shared.Rent(Constants.DefaultMaxPointsPerBlock);
            Span<long> ts_to_span = new Span<long>(ts, 0, Constants.DefaultMaxPointsPerBlock);
            BatchTimeStamp tdec = (BatchTimeStamp)CoderFactory.Get(DataTypeEnum.DateTime);
            (int tscount, string tserror) = tdec.DecodeAll(ts_src_span, ts_to_span);
            CoderFactory.Put(DataTypeEnum.DateTime, tdec);
            if (tscount == 0 || tserror != null)
            {
                ArrayPool<long>.Shared.Return(ts);
                return null;
            }
            span = span.Slice(tsLen);
            //读取数值段.
            nn = Varint.Read(span, out int vsLen);
            Span<byte> vs_src_span = span.Slice(0, vsLen);
            span = span.Slice(vsLen);
            //解析质量段.
            nn = Varint.Read(span, out int qsLen);
            Span<byte> qs_src_span = span.Slice(0, qsLen);
            BatchInt64 qdec = (BatchInt64)CoderFactory.Get(DataTypeEnum.Integer);
            long[] qs = ArrayPool<long>.Shared.Rent(tscount);
            Span<long> qs_to_span = new Span<long>(qs, 0, tscount);
            (int qscount, string qserror) = qdec.DecodeAll(qs_src_span, qs_to_span);
            CoderFactory.Put(DataTypeEnum.Integer, qdec);
            if (qscount != tscount || qserror != null)
            {
                ArrayPool<long>.Shared.Return(qs);
                return null;
            }
            //解析数据段.
            ClockValues result = null;
            if (datatype == DataTypeEnum.Double)
            {
                BatchDouble decoder = (BatchDouble)CoderFactory.Get(datatype);
                double[] vs = ArrayPool<double>.Shared.Rent(tscount);
                Span<double> vs_to_span = new Span<double>(vs, 0, tscount);
                (int vscount, string vserror) = decoder.DecodeAll(vs_src_span, vs_to_span);
                CoderFactory.Put(datatype, decoder);
                if (vscount == tscount && vserror == null)
                {
                    result = new ClockValues(tscount);
                    for (int i = 0; i < tscount; i++)
                    {
                        long clock = ts_to_span[i];
                        if (clock > end)
                        {
                            break;
                        }
                        if (clock >= start)
                        {
                            result.Append(new ClockDouble(clock, vs_to_span[i], (int)qs_to_span[i]));
                        }
                    }
                }
                ArrayPool<double>.Shared.Return(vs);
            }
            else if (datatype == DataTypeEnum.Boolean)
            {
                BatchBoolean decoder = (BatchBoolean)CoderFactory.Get(datatype);
                bool[] vs = ArrayPool<bool>.Shared.Rent(tscount);
                Span<bool> vs_to_span = new Span<bool>(vs, 0, tscount);
                (int vscount, string vserror) = decoder.DecodeAll(vs_src_span, vs_to_span);
                CoderFactory.Put(datatype, decoder);
                if (vscount == tscount && vserror == null)
                {
                    result = new ClockValues(tscount);
                    for (int i = 0; i < tscount; i++)
                    {
                        long clock = ts_to_span[i];
                        if (clock > end)
                        {
                            break;
                        }
                        if (clock >= start)
                        {
                            result.Append(new ClockBoolean(clock, vs_to_span[i], (int)qs_to_span[i]));
                        }
                    }
                }
                ArrayPool<bool>.Shared.Return(vs);
            }
            else if (datatype == DataTypeEnum.Integer)
            {
                BatchInt64 decoder = (BatchInt64)CoderFactory.Get(datatype);
                long[] vs = ArrayPool<long>.Shared.Rent(tscount);
                Span<long> vs_to_span = new Span<long>(vs, 0, tscount);
                (int vscount, string vserror) = decoder.DecodeAll(vs_src_span, vs_to_span);
                CoderFactory.Put(datatype, decoder);
                if (vscount == tscount && vserror == null)
                {
                    result = new ClockValues(tscount);
                    for (int i = 0; i < tscount; i++)
                    {
                        long clock = ts_to_span[i];
                        if (clock > end)
                        {
                            break;
                        }
                        if (clock >= start)
                        {
                            result.Append(new ClockInt64(clock, vs_to_span[i], (int)qs_to_span[i]));
                        }
                    }
                }
                ArrayPool<long>.Shared.Return(vs);
            }
            else if (datatype == DataTypeEnum.String)
            {
                BatchString decoder = (BatchString)CoderFactory.Get(datatype);
                string[] vs = ArrayPool<string>.Shared.Rent(tscount);
                Span<string> vs_to_span = new Span<string>(vs, 0, tscount);
                (int vscount, string vserror) = decoder.DecodeAll(vs_src_span, vs_to_span);
                CoderFactory.Put(datatype, decoder);
                if (vscount == tscount && vserror == null)
                {
                    result = new ClockValues(tscount);
                    for (int i = 0; i < tscount; i++)
                    {
                        long clock = ts_to_span[i];
                        if (clock > end)
                        {
                            break;
                        }
                        if (clock >= start)
                        {
                            result.Append(new ClockString(clock, vs_to_span[i], (int)qs_to_span[i]));
                        }
                    }
                }
                ArrayPool<string>.Shared.Return(vs);
            }
            return result;
        }
    }
}
