using System;

namespace Easydata.Engine
{
    public class ClockDouble : ClockValueBase, IClockValue<double>
    {
        public ClockDouble(long clock, object value, int quality = 0) : base(clock, value, quality) { }

        protected override void setValue(object value)
        {
            base.setValue(value);
            if (value != null)
            {
                string type = value.GetType().Name;
                if (type == "Single"
                   || type == "Double"
                   || type == "Decimal")
                {
                    Value = Convert.ToDouble(value);
                    Size = 20;
                    DataType = Easydata.Engine.DataTypeEnum.Double;
                }
            }
        }

        public double Value { get; set; }
    }
}
