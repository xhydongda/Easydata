using System;

namespace Easydata.Engine
{
    public class ClockInt64 : ClockValueBase, IClockValue<long>
    {
        public ClockInt64(long clock, object value, int quality = 0) : base(clock, value, quality) { }

        protected override void setValue(object value)
        {
            base.setValue(value);
            if (value != null)
            {
                string type = value.GetType().Name;
                if (type == "Int32"
                   || type == "Int64"
                   || type == "Int16"
                   || type == "Int32"
                   || type == "UInt16"
                   || type == "UInt32"
                   || type == "UInt64")
                {
                    Value = Convert.ToInt64(value);
                    Size = 20;
                    DataType = Easydata.Engine.DataTypeEnum.Integer;
                }
            }
        }

        public long Value { get; set; }
    }
}
