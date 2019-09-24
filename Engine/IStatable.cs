using System.Collections.Generic;

namespace Easydata.Engine
{
    public interface IStatable
    {
        string Name { get; }
        Dictionary<string, long> Stat();
    }
}
