using System;

namespace Easydata.Engine
{
    public class ClockBoolean : ClockValueBase, IClockValue<bool>
    {
        public ClockBoolean(long clock, object value, int quality = 0) : base(clock, value, quality) { }

        protected override void setValue(object value)
        {
            base.setValue(value);
            if (value != null)
            {
                string type = value.GetType().Name;
                if (type == "Boolean")
                {
                    Value = Convert.ToBoolean(value);
                    Size = 13;
                    DataType = Easydata.Engine.DataTypeEnum.Boolean;
                }
            }
        }

        public bool Value { get; set; }
    }
}
