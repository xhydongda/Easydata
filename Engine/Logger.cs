using Serilog;

namespace Easydata.Engine
{
    public class Logger
    {
        static Logger()
        {
            Log.Logger = new LoggerConfiguration()
            .WriteTo.Console()
            .WriteTo.File("logs/engine-.txt", rollingInterval: RollingInterval.Day)
            .CreateLogger();
        }
        public static void Write(object obj)
        {
            Info(obj==null?"null":obj.ToString());
        }

        public static void Write(string msg)
        {
            Info(msg);
        }

        public static void Info(string msg)
        {
            Log.Information(msg);
        }

        public static void Error(string msg)
        {
            Log.Error(msg);
        }
    }
}
