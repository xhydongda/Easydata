using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Easydata.Engine
{
    public sealed class EncoderFactory
    {
        static Dictionary<byte, ConcurrentQueue<IEncoder>> _Encoders;
        static EncoderFactory()
        {
            _Encoders = new Dictionary<byte, ConcurrentQueue<IEncoder>>();
            _Encoders.Add(DataTypeEnum.Double, new ConcurrentQueue<IEncoder>());
            _Encoders.Add(DataTypeEnum.Integer, new ConcurrentQueue<IEncoder>());
            _Encoders.Add(DataTypeEnum.Boolean, new ConcurrentQueue<IEncoder>());
            _Encoders.Add(DataTypeEnum.String, new ConcurrentQueue<IEncoder>());
            _Encoders.Add(DataTypeEnum.DateTime, new ConcurrentQueue<IEncoder>());
        }

        public static IEncoder Get(byte type, int sz)
        {
            ConcurrentQueue<IEncoder> typeencoders = _Encoders[type];
            IEncoder result;
            if (typeencoders.TryDequeue(out result))
            {
                return result;
            }
            else
            {
                switch (type)
                {
                    case DataTypeEnum.Double:
                        return new FloatEncoder(sz);
                    case DataTypeEnum.Integer:
                        return new IntegerEncoder(sz);
                    case DataTypeEnum.Boolean:
                        return new BooleanEncoder(sz);
                    case DataTypeEnum.String:
                        return new StringEncoder(sz);
                    case DataTypeEnum.DateTime:
                        return new TimeEncoder(sz);
                }
            }
            return null;
        }

        public static void Put(byte type, IEncoder encoder)
        {
            if (encoder != null)
            {
                encoder.Reset();
                _Encoders[type].Enqueue(encoder);
            }
        }
    }
}
