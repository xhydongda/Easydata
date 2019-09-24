using System;

namespace Easydata.Engine
{
    public class ClockString : ClockValueBase, IClockValue<string>
    {
        public ClockString(long clock, object value, int quality = 0) : base(clock, value, quality) { }

        protected override void setValue(object value)
        {
            base.setValue(value);
            if (value != null)
            {
                string type = value.GetType().Name;
                if (type == "String"
                    || type == "DateTime")
                {
                    Value = Convert.ToString(value);
                    Size = 12 + System.Text.Encoding.Default.GetBytes(Value).Length;
                    DataType = Easydata.Engine.DataTypeEnum.String;
                }
            }
        }

        public string Value { get; set; }
    }
}
