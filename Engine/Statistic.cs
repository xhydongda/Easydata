using System.Collections.Generic;

namespace Arim.Plat.Trend.Engine
{
    // Statistic is the representation of a statistic used by the monitoring service.
    public class Statistic
    {
        public Statistic()
        { 
        }

        public Statistic(string name)
        {
            this.Name = name;
            Tags = new Dictionary<string, string>();
            Values = new Dictionary<string, object>();
        }

        public string Name { get; set; }
        public Dictionary<string, string> Tags { get; set; }
        public Dictionary<string, object> Values { get; set; }

    }
}
