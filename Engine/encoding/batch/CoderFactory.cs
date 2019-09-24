using System.Collections.Concurrent;

namespace Easydata.Engine
{
    public sealed class CoderFactory
    {
        static ConcurrentQueue<BatchBoolean> _boolcoders;
        static ConcurrentQueue<BatchDouble> _doublecoders;
        static ConcurrentQueue<BatchInt64> _longcoders;
        static ConcurrentQueue<BatchTimeStamp> _timecoders;
        static ConcurrentQueue<BatchString> _stringcoders;
        static CoderFactory()
        {
            _boolcoders = new ConcurrentQueue<BatchBoolean>();
            _doublecoders = new ConcurrentQueue<BatchDouble>();
            _longcoders = new ConcurrentQueue<BatchInt64>();
            _timecoders = new ConcurrentQueue<BatchTimeStamp>();
            _stringcoders = new ConcurrentQueue<BatchString>();
        }

        public static IBatchCoder Get(byte type)
        {
            switch (type)
            {
                case DataTypeEnum.Double:
                    if (_doublecoders.TryDequeue(out BatchDouble batchdouble))
                    {
                        return batchdouble;
                    }
                    return new BatchDouble();
                case DataTypeEnum.Integer:
                    if (_longcoders.TryDequeue(out BatchInt64 batchlong))
                    {
                        return batchlong;
                    }
                    return new BatchInt64();
                case DataTypeEnum.Boolean:
                    if (_boolcoders.TryDequeue(out BatchBoolean batchbool))
                    {
                        return batchbool;
                    }
                    return new BatchBoolean();
                case DataTypeEnum.String:
                    if (_stringcoders.TryDequeue(out BatchString batchstring))
                    {
                        return batchstring;
                    }
                    return new BatchString();
                case DataTypeEnum.DateTime:
                    if (_timecoders.TryDequeue(out BatchTimeStamp batchtime))
                    {
                        return batchtime;
                    }
                    return new BatchTimeStamp();
            }
            return null;
        }

        public static void Put(byte type, IBatchCoder coder)
        {
            switch (type)
            {
                case DataTypeEnum.Double:
                    _doublecoders.Enqueue((BatchDouble)coder);
                    break;
                case DataTypeEnum.Integer:
                    _longcoders.Enqueue((BatchInt64)coder);
                    break;
                case DataTypeEnum.Boolean:
                    _boolcoders.Enqueue((BatchBoolean)coder);
                    break;
                case DataTypeEnum.String:
                    _stringcoders.Enqueue((BatchString)coder);
                    break;
                case DataTypeEnum.DateTime:
                    _timecoders.Enqueue((BatchTimeStamp)coder);
                    break;
            }
        }
    }
}
