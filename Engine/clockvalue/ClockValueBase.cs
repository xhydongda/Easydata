using System;

namespace Easydata.Engine
{
    /// <summary>
    /// 时标值类型，按照时标比较大小.
    /// </summary>
    public abstract class ClockValueBase : IClockValue
    {
        public ClockValueBase(long t, object v, int quality = 0)
        {
            Clock = t;
            OValue = v;
            Quality = quality;
        }

        /// <summary>
        /// 时标的ticks.
        /// </summary>
        public long Clock { get; set; }

        public int Quality { get; set; }

        object _OValue;
        protected virtual void setValue(object value)
        {
            _OValue = value;
            DataType = Easydata.Engine.DataTypeEnum.Empty;
            Size = 0;
        }

        /// <summary>
        /// 值.
        /// </summary>
        public object OValue
        {
            get
            {
                return _OValue;
            }
            set
            {
                setValue(value);
            }
        }

        /// <summary>
        /// 字节数,8位时标+类型长度（浮点数8，整型8，布尔1）.
        /// </summary>
        public int Size { get; protected set; }

        /// <summary>
        /// 根据值的系统类型判断其数据类型.
        /// </summary>
        public byte DataType { get; protected set; }

        /// <summary>
        /// 带毫秒和值的字符串.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.Format("{0:yyyy-MM-dd HH:mm:ss.fff} {1}", new DateTime(Clock), _OValue);
        }

        public int CompareTo(IClockValue other)
        {
            return Clock.CompareTo(other.Clock);
        }
    }
}
