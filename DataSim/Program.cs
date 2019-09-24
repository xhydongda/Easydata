using Easydata.Engine;
using System;
using System.Collections.Generic;
using System.Threading;

namespace DataSim
{
    class Program
    {
        static void Main(string[] args)
        {
            TestWriteDouble();
        }

        static void TestWriteBoolean()
        {
            Store store = new Store(AppDomain.CurrentDomain.BaseDirectory);
            Console.WriteLine("请输入模拟点数量,默认50000");
            int n = 50000;
            try
            {
                n = Convert.ToInt32(Console.ReadLine());
            }
            catch { }
            Dictionary<ulong, ClockValues> data = new Dictionary<ulong, ClockValues>(n);
            for (int i = 1; i <= n; i++)
            {
                ClockValues cvs = new ClockValues();
                cvs.Append(new ClockBoolean(DateTime.Now.Ticks, 0));
                data.Add((ulong)i, cvs);
            }
            Console.WriteLine("请输入数据刷新频度,默认1000ms");
            int ms = 1000;
            try
            {
                ms = Convert.ToInt32(Console.ReadLine());
            }
            catch { }

            store.AddSids(new List<ulong>(data.Keys), 31);
            store.Open();
            Console.WriteLine("按任意键开始生成数据...");
            Console.ReadLine();
            DateTime time = DateTime.Now;
            int times = 0;
            Random random = new Random();
            double totalms = 0;
            while (true)
            {
                DateTime now = DateTime.Now;
                TimeSpan span = now - time;
                if (span.TotalMilliseconds >= ms)
                {
                    data.Clear();
                    for (int i = 1; i <= n; i++)
                    {
                        ClockValues cvs = new ClockValues();
                        cvs.Append(new ClockBoolean(now.Ticks, (random.Next(1) == 1)));
                        data.Add((ulong)i, cvs);
                    }
                    DateTime time1 = DateTime.Now;
                    int count = store.Write(data);
                    span = (DateTime.Now - time1);
                    //Logger.Write(span.TotalMilliseconds);
                    time = now;
                    times++;
                    totalms += span.TotalMilliseconds;
                    if (times % 60 == 0)
                    {
                        Logger.Write(String.Format("已写入{0}秒布尔，平均耗时{1}毫秒", times, totalms / times));
                    }
                }
                Thread.Sleep(1);
            }
        }

        static void TestWriteDouble()
        {
            Store store = new Store(AppDomain.CurrentDomain.BaseDirectory);
            Console.WriteLine("请输入模拟点数量,默认50000");
            int n = 50000;
            try
            {
                n = Convert.ToInt32(Console.ReadLine());
            }
            catch { }
            Dictionary<ulong, ClockValues> data = new Dictionary<ulong, ClockValues>(n);
            for (int i = 1; i <= n; i++)
            {
                ClockValues cvs = new ClockValues();
                cvs.Append(new ClockDouble(DateTime.Now.Ticks, 0));
                data.Add((ulong)i, cvs);
            }
            Console.WriteLine("请输入数据刷新频度,默认1000ms");
            int ms = 1000;
            try
            {
                ms = Convert.ToInt32(Console.ReadLine());
            }
            catch { }

            store.AddSids(new List<ulong>(data.Keys), 31);
            store.Open();
            Console.WriteLine("按任意键开始生成数据...");
            Console.ReadLine();
            DateTime time = DateTime.Now;
            int times = 0;
            Random random = new Random();
            double totalms = 0;
            while (true)
            {
                DateTime now = DateTime.Now;
                TimeSpan span = now - time;
                if (span.TotalMilliseconds >= ms)
                {
                    data.Clear();
                    for (int i = 1; i <= n; i++)
                    {
                        ClockValues cvs = new ClockValues();
                        cvs.Append(new ClockDouble(now.Ticks, (random.NextDouble())));
                        data.Add((ulong)i, cvs);
                    }
                    DateTime time1 = DateTime.Now;
                    int count = store.Write(data);
                    span = (DateTime.Now - time1);
                    //Logger.Write(span.TotalMilliseconds);
                    time = now;
                    times++;
                    totalms += span.TotalMilliseconds;
                    if (times % 60 == 0)
                    {
                        Logger.Write(String.Format("已写入{0}秒浮点数，平均耗时{1}毫秒", times, totalms / times));
                    }
                }
                Thread.Sleep(1);
            }
        }

        static void TestWriteInt()
        {
            Store store = new Store(AppDomain.CurrentDomain.BaseDirectory);
            Console.WriteLine("请输入模拟点数量,默认50000");
            int n = 50000;
            try
            {
                n = Convert.ToInt32(Console.ReadLine());
            }
            catch { }
            Dictionary<ulong, ClockValues> data = new Dictionary<ulong, ClockValues>(n);
            for (int i = 1; i <= n; i++)
            {
                ClockValues cvs = new ClockValues();
                cvs.Append(new ClockInt64(DateTime.Now.Ticks, 0));
                data.Add((ulong)i, cvs);
            }
            Console.WriteLine("请输入数据刷新频度,默认1000ms");
            int ms = 1000;
            try
            {
                ms = Convert.ToInt32(Console.ReadLine());
            }
            catch { }

            store.AddSids(new List<ulong>(data.Keys), 31);
            store.Open();
            Console.WriteLine("按任意键开始生成数据...");
            Console.ReadLine();
            DateTime time = DateTime.Now;
            int times = 0;
            Random random = new Random();
            double totalms = 0;
            while (true)
            {
                DateTime now = DateTime.Now;
                TimeSpan span = now - time;
                if (span.TotalMilliseconds >= ms)
                {
                    data.Clear();
                    for (int i = 1; i <= n; i++)
                    {
                        ClockValues cvs = new ClockValues();
                        cvs.Append(new ClockInt64(now.Ticks, (random.Next())));
                        data.Add((ulong)i, cvs);
                    }
                    DateTime time1 = DateTime.Now;
                    int count = store.Write(data);
                    span = (DateTime.Now - time1);
                    //Logger.Write(span.TotalMilliseconds);
                    time = now;
                    times++;
                    totalms += span.TotalMilliseconds;
                    if (times % 60 == 0)
                    {
                        Logger.Write(String.Format("已写入{0}秒整型，平均耗时{1}毫秒", times, totalms / times));
                    }
                }
                Thread.Sleep(1);
            }
        }

        static void TestRead()
        {
            Store store = new Store(AppDomain.CurrentDomain.BaseDirectory);
            store.Open();
            int s = 20;
            ulong c = 50000;
            for (int j = 1; j < 50; j++)
            {
                List<ulong> keys = new List<ulong>((int)c);
                for (ulong kk = 1; kk <= c; kk++)
                {
                    keys.Add(kk);
                }
                DateTime time2 = DateTime.Now;
                Dictionary<ulong, ClockValues> v = store.Read(keys, new DateTime(2019, 8, 23, 16, 10, 0).Ticks, new DateTime(2019, 8, 23, 16, 10+s / 60, s - 60 * (s / 60)).Ticks);
                Logger.Write(c + "," + (DateTime.Now - time2).TotalMilliseconds);
                c = c - 1000;
                s = 1000000 / (int)c;
            }
        }
    }
}
