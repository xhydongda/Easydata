using System;

namespace Easydata.Engine
{
    public interface IClockValue : IComparable<IClockValue>
    {
        long Clock { get; set; }
        int Size { get; }
        byte DataType { get; }
        int Quality { get; set; }
        object OValue { get; set; }
    }

    public interface IClockValue<T> : IClockValue
    {
        T Value { get; set; }
    }
}
