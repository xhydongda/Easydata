using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Easydata.Engine
{
    public sealed class DecoderFactory
    {
        static Dictionary<byte, ConcurrentQueue<IDecoder>> _Decoders;
        static DecoderFactory()
        {
            _Decoders = new Dictionary<byte, ConcurrentQueue<IDecoder>>();
            _Decoders.Add(DataTypeEnum.Double, new ConcurrentQueue<IDecoder>());
            _Decoders.Add(DataTypeEnum.Integer, new ConcurrentQueue<IDecoder>());
            _Decoders.Add(DataTypeEnum.Boolean, new ConcurrentQueue<IDecoder>());
            _Decoders.Add(DataTypeEnum.String, new ConcurrentQueue<IDecoder>());
            _Decoders.Add(DataTypeEnum.DateTime, new ConcurrentQueue<IDecoder>());
        }

        public static IDecoder Get(byte type)
        {
            ConcurrentQueue<IDecoder> typedecoders = _Decoders[type];
            IDecoder result;
            if (typedecoders.TryDequeue(out result))
            {
                return result;
            }
            else
            {
                switch (type)
                {
                    case DataTypeEnum.Double:
                        return new FloatDecoder();
                    case DataTypeEnum.Integer:
                        return new IntegerDecoder();
                    case DataTypeEnum.Boolean:
                        return new BooleanDecoder();
                    case DataTypeEnum.String:
                        return new StringDecoder();
                    case DataTypeEnum.DateTime:
                        return new TimeDecoder();
                }
            }
            return null;
        }

        public static void Put(byte type, IDecoder decoder)
        {
            if (decoder != null)
            {
                _Decoders[type].Enqueue(decoder);
            }
        }
    }
}
