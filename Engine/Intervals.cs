using System;
using System.Collections.Generic;
using System.Text;

namespace Easydata.Engine
{
    /// <summary>
    /// 周期变换类.
    /// </summary>
    [Serializable]
    public class Intervals
    {
        public const int Second = 1;
        public const int Minute = 60;
        public const int Hour = 3600;
        public const int Shift = 28800;
        public const int Day = 86400;
        public const int Week = 604800;
        public const int Month = 2628000;
        public const int Quarter = 7884000;
        public const int Year = 31536000;


        static SortedList<int, Interval> _Intervals;
        static Intervals()
        {
            _Intervals = new SortedList<int, Interval>();
            
            //注册预定义的周期
            string intervalsRecords = @"1,秒,2000/1/1,yyyy年MM月dd日 HH:mm:ss,秒,1,,,,,60
                                        60,分,2000/1/1,yyyy年MM月dd日 HH:mm,分,1,,,,1,3600
                                        900,刻,2000/1/1,yyyy年MM月dd日 HH:mm,分,15,,,,60,3600
                                        3600,时,2000/1/1,yyyy年MM月dd日 HH,时,1,,,,60,86400
                                        28800,班,2000/1/1,yyyy年MM月dd日,时,8,,,夜.白.中,3600,86400
                                        86400,天,2000/1/1,yyyy年MM月dd日,日,1,,,,3600,2628000
                                        604800,周,2000/1/3,yyyy年MM月dd日,周,1,,,,86400,
                                        2628000,月,2000/1/1,yyyy年MM月,月,1,,,,86400,31536000
                                        7884000,季度,2000/1/1,yyyy年,月,3,,,一季度.二季度.三季度.四季度,2628000,31536000
                                        15768000,半年,2000/1/1,yyyy年,月,6,,,上半年.下半年,2628000,31536000
                                        31536000,年,2000/1/1,yyyy年,年,1,,,,2628000,";
            string[] intervalStrings = intervalsRecords.Split('\n');
            foreach (string intervalString in intervalStrings)
            {
                Interval item = new Interval(intervalString);
                _Intervals.Add(item.Seconds, item);
            }
            foreach (Interval item in _Intervals.Values)
            {
                if (item.SmallerId != null && _Intervals.ContainsKey(item.SmallerId.Value))
                {
                    item.Smaller = _Intervals[item.SmallerId.Value];
                }
                if (item.BiggerId != null && _Intervals.ContainsKey(item.BiggerId.Value))
                {
                    item.Bigger = _Intervals[item.BiggerId.Value];
                }
            }
        }

        /// <summary>
        /// 根据时间值(秒数）获得一个周期计算对象
        /// </summary>
        /// <param name="seconds">秒数</param>
        /// <returns></returns>
        private static Interval get(int seconds)
        {
			lock(_Intervals)
			{				
				if (_Intervals.ContainsKey(seconds))
					return _Intervals[seconds];
				try
				{
					CustomInterval interval = new CustomInterval(seconds);
					interval.Smaller = _Intervals[interval.SmallerId.Value];
					interval.Bigger = _Intervals[interval.BiggerId.Value];
					_Intervals.Add(seconds, interval);
					return interval;
				}
				catch
				{
					return null;
				}
			}
        }

        public static DateTime Ceiling(int seconds, DateTime time, string option)
        {
            DateTime time2 = Round(seconds, time, option);
            return MoveN(seconds, time2, 1);
        }

        /// <summary>
        /// 获取一个周期的助记符
        /// </summary>
        /// <param name="seconds">周期的秒值</param>
        /// <returns>助记符</returns>
        public static string GetName(int seconds)
        {
            Interval obj = get(seconds);
            if (obj != null)
                return obj.Name;
            return seconds + "秒";
        }

        /// <summary>
        /// 获取在控件上显示时的格式
        /// </summary>
        /// <param name="seconds">周期的秒值</param>
        /// <returns>格式字符串</returns>
        public static string GetFormat(int seconds)
        {
            Interval obj = get(seconds);
            if (obj != null)
                return obj.Format;
            return "yyyy年MM月dd日 HH:mm:ss";
        }

        /// <summary>
        /// 附加时间可选项，适用于 季度、半年 等
        /// </summary>
        /// <param name="seconds">周期的秒值</param>
        /// <returns>附加时间可选项,没有返回null</returns>
        public static List<string> GetOptions(int seconds)
        {
            Interval obj = get(seconds);
            if (obj != null)
                return obj.Options;
            return null;
        }

        /// <summary>
        /// 对时间进行周期取整，结果小于等于time
        /// 如 time = 2009-02-01 20:00:01 option = 一季度 取整后的结果是 2009-01-01 00:00:00
        /// </summary>
        /// <param name="seconds">周期的秒值</param>
        /// <param name="time">被取整时间，取整后的结果落在紧邻该时间之前</param>
        /// <param name="option">附加时间选择，可以为空</param>
        /// <returns>取整后的时间</returns>
        public static DateTime Round(int seconds, DateTime time, string option)
        {
            Interval obj = get(seconds);
            if (obj != null)
                return obj.Round(time, option);
            return time;
        }

        /// <summary>
        /// 应用周期偏移.
        /// </summary>
        /// <param name="seconds">周期的秒值</param>
        /// <param name="time">原始时间，实际的物理时间</param>
        /// <returns>偏移后的时间，用于前端显示</returns>
        public static DateTime Offset(int seconds, DateTime time)
        {
            Interval obj = get(seconds);
            if (obj != null)
                return obj.Offset(time);
            return time;
        }

        /// <summary>
        /// 获取某时刻所对应的附加时间项
        /// </summary>
        /// <param name="seconds">周期的秒值</param>
        /// <param name="time">时间</param>
        /// <returns>附加时间项，没有返回null</returns>
        public static string GetOption(int seconds, DateTime time)
        {
            Interval obj = get(seconds);
            if (obj != null)
                return obj.GetOption(time);
            return null;
        }

        /// <summary>
        /// 周期跳转，在time 增加 times 个周期
        /// </summary>
        /// <param name="seconds">周期的秒值</param>
        /// <param name="time">时间</param>
        /// <param name="count">跳转的周期数</param>
        /// <returns></returns>
        public static DateTime MoveN(int seconds, DateTime time, int times)
        {
            Interval obj = get(seconds);
            if (obj != null)
                return obj.MoveN(time, times);
            return time.AddSeconds(seconds * times);
        }

        /// <summary>
        /// 获取临近小周期
        /// </summary>
        /// <param name="seconds">周期的秒值</param>
        /// <returns>临近小周期，没有返回-1</returns>
        public static int GetSmaller(int seconds)
        {
            Interval obj = get(seconds);
            if (obj != null && obj.Smaller != null)
                return obj.Smaller.Seconds;
            return -1;
        }

        /// <summary>
        /// 获取临近大周期
        /// </summary>
        /// <param name="seconds">周期的秒值</param>
        /// <returns>临近大周期，没有返回-1</returns>
        public static int GetBigger(int seconds)
        {
            Interval obj = get(seconds);
            if (obj != null && obj.Bigger != null)
                return obj.Bigger.Seconds;
            return -1;
        }

        /// <summary>
        /// 根据秒数获得最接近周期中开始时间到结束时间的时间差
        /// </summary>
        /// <param name="seconds">秒数</param>
        /// <param name="from">开始时间</param>
        /// <param name="to">结束时间</param>
        /// <returns></returns>
        public static double GetTotalBetween(int seconds, DateTime from, DateTime to)
        {
            Interval obj = get(seconds);
            if (obj != null)
                return obj.TotalBetween(from, to);
            return 1;
        }

        /// <summary>
        /// 获得所有周期信息包含的秒数值
        /// </summary>
        /// <returns></returns>
        public static List<int> GetAll()
        {
            lock (_Intervals)
            {
                return new List<int>(_Intervals.Keys);
            }
        }
    }

    [Serializable]
    public class Interval
    {
        static Dictionary<string, TickTypeRounder> _Rounders;
        protected static Dictionary<string, TickTypeRounder> Rounders
        {
            get
            {
                if (_Rounders == null)
                {
                    _Rounders = new Dictionary<string, TickTypeRounder>();
                    _Rounders.Add("年", new YearRounder());
                    _Rounders.Add("月", new MonthRounder());
                    _Rounders.Add("周", new WeekRounder());
                    _Rounders.Add("日", new DayRounder());
                    _Rounders.Add("时", new HourRounder());
                    _Rounders.Add("分", new MinuteRounder());
                    _Rounders.Add("秒", new SecondRounder());
                }
                return _Rounders;
            }
        }

        public Interval()
        { }

        protected TickTypeRounder _Rounder;
        public Interval(string intervalString)
        {
            string[] strs = intervalString.Split(',');
            int length = strs.Length;
            _Seconds = Convert.ToInt32(strs[0].Trim());
            _Name = strs[1].Trim();
            _FirstClock = Convert.ToDateTime(strs[2].Trim());
            _Format = strs[3].Trim();
            _TickType = strs[4].Trim();
            _Ticks = Convert.ToInt32(strs[5].Trim());
            string str = strs[6];
            if (str != null && str.Trim().Length > 0)
            {
                _OffsetTickType = str.Trim();
            }
            str = strs[7];
            if (str != null && str.Trim().Length > 0)
            {
                _OffsetTicks = Convert.ToInt32(str.Trim());
            }
            str = strs[8];
            if (str != null && str.Trim().Length > 0)
            {
                string[] subs = str.Split('.');
                _Options = new List<string>();
                foreach (string sub in subs)
                {
                    if (!string.IsNullOrEmpty(sub.Trim()))
                    {
                        _Options.Add(sub.Trim());
                    }
                }
            }
            if (strs.Length > 9)
            {
                str = strs[9];
                if (str != null && str.Trim().Length > 0)
                {
                    _SmallerId = Convert.ToInt32(str.Trim());
                }
            }
            if (strs.Length > 10)
            {
                str = strs[10];
                if (str != null && str.Trim().Length > 0)
                {
                    _BiggerId = Convert.ToInt32(str.Trim());
                }
            }
            _Rounder = Rounders[_TickType].Clone();
            _Rounder.Ticks = _Ticks;
            _Rounder.Options = _Options;
            _Rounder.FirstClock = _FirstClock;
        }

        protected int _Seconds = 1;
        /// <summary>
        /// 代表周期的秒值，与实际秒值可能不一致，例如月周期.
        /// </summary>
        public int Seconds { get { return _Seconds; } }

        protected string _Name;
        /// <summary>
        /// 周期的助记符.
        /// </summary>
        public virtual string Name { get { return _Name; } }

        protected string _Format;
        /// <summary>
        /// 在控件上显示时的格式.
        /// </summary>
        public virtual string Format { get { return _Format; } }

        protected DateTime _FirstClock;
        /// <summary>
        /// 第一个起始点.
        /// </summary>
        public DateTime FirstClock { get { return _FirstClock; } }

        List<string> _Options;
        /// <summary>
        /// 附加时间可选项，适用于 季度、半年 等
        /// </summary>
        public List<string> Options { get { return _Options; } }

        protected string _TickType;
        /// <summary>
        /// 月日时分秒类型.
        /// </summary>
        public string TickType { get { return _TickType; } }

        protected int _Ticks;
        /// <summary>
        /// 一个周期相当于多少月日时分秒.
        /// </summary>
        public int Ticks { get { return _Ticks; } }

        protected string _OffsetTickType;
        /// <summary>
        /// 月日时分秒类型.
        /// </summary>
        public string OffsetTickType { get { return _OffsetTickType; } }

        protected int _OffsetTicks = 0;
        /// <summary>
        /// 周期起始点物理始终与显示使用的偏差.
        /// </summary>
        public int OffsetTicks { get { return _OffsetTicks; } }

        protected int? _SmallerId;
        /// <summary>
        /// 小一级的周期的Id.
        /// </summary>
        public int? SmallerId { get { return _SmallerId; } }

        protected Interval _Smaller;
        /// <summary>
        /// 小一级的周期.
        /// </summary>
        public Interval Smaller { get { return _Smaller; } set { _Smaller = value; } }

        protected int? _BiggerId;
        /// <summary>
        /// 大一级的周期的Id.
        /// </summary>
        public int? BiggerId { get { return _BiggerId; } }

        protected Interval _Bigger;
        /// <summary>
        /// 大一级的周期.
        /// </summary>
        public Interval Bigger { get { return _Bigger; } set { _Bigger = value; } }

        public virtual DateTime Round(DateTime time, string option)
        {
            return _Rounder.Round(time, option);
        }

        public virtual DateTime MoveN(DateTime time, int count)
        {
            return _Rounder.MoveN(time, count);
        }

        public virtual string GetOption(DateTime time)
        {
            return _Rounder.GetOption(time);
        }

        public virtual double TotalBetween(DateTime from, DateTime to)
        {
            return _Rounder.TotalBetween(from, to);
        }

        public DateTime Offset(DateTime from)
        {
            if (_OffsetTicks == 0 || String.IsNullOrEmpty(_OffsetTickType))
                return from;
            TickTypeRounder offsetRounder = _Rounders[_OffsetTickType];
            return offsetRounder.MoveN(from, _OffsetTicks);
        }

        #region rounder

        protected abstract class TickTypeRounder
        {
            public TickTypeRounder()
            {
            }

            public TickTypeRounder(TickTypeRounder src)
            {
                this.FirstClock = src.FirstClock;
                this.Options = src.Options;
                this.Ticks = src.Ticks;
            }

            DateTime _FirstClock;
            public DateTime FirstClock { get { return _FirstClock; } set { _FirstClock = value; } }

            List<string> _Options;
            public List<string> Options { get { return _Options; } set { _Options = value; } }

            int _Ticks = 1;
            public int Ticks { get { return _Ticks; } set { _Ticks = value; } }

            public abstract DateTime Round(DateTime time, string option);

            protected DateTime toYearStart(DateTime time)
            {
                DateTime result = new DateTime(time.Year, FirstClock.Month, FirstClock.Day, FirstClock.Hour, FirstClock.Minute, FirstClock.Second);
                if (result > time)
                {
                    result = result.AddYears(-1);
                }
                return result;
            }

            public abstract DateTime MoveN(DateTime time, int count);

            public abstract string GetOption(DateTime time);

            public abstract double TotalBetween(DateTime from, DateTime to);

            public abstract TickTypeRounder Clone();
        }

        protected class YearRounder : TickTypeRounder
        {
            public YearRounder()
                : base()
            { }
            public YearRounder(YearRounder src)
                : base(src)
            { }

            public override DateTime Round(DateTime time, string option)
            {
                return toYearStart(time);
            }

            public override DateTime MoveN(DateTime time, int count)
            {
                return time.AddYears(count * Ticks);
            }

            public override string GetOption(DateTime time)
            {
                return null;
            }

            public override double TotalBetween(DateTime from, DateTime to)
            {
                return (to.Year - from.Year) / Ticks;
            }

            public override TickTypeRounder Clone()
            {
                return new YearRounder(this);
            }
        }

        protected class MonthRounder : TickTypeRounder
        {
            public MonthRounder()
                : base()
            { }
            public MonthRounder(MonthRounder src)
                : base(src)
            { }
            public override DateTime Round(DateTime time, string option)
            {
                DateTime yearStart = toYearStart(time);
                if (string.IsNullOrEmpty(option))
                {
                    int i = 0;
                    DateTime next = yearStart;
                    while (next <= time)
                    {
                        i++;
                        next = yearStart.AddMonths(i * Ticks);
                    }
                    return yearStart.AddMonths((i - 1) * Ticks);
                }
                else
                {
                    int optionticks = Ticks * Options.IndexOf(option);
                    int totalticks = Ticks * Options.Count;
                    DateTime result = yearStart.AddMonths(optionticks);
                    if (result > time)
                    {
                        result = yearStart.AddMonths(optionticks - totalticks);
                    }
                    return result;
                }//季度、半年 周期
            }

            public override DateTime MoveN(DateTime time, int count)
            {
                DateTime normaltime = Intervals.Offset(2628000, time);//到标准月
                TimeSpan span = normaltime - time;
                normaltime = normaltime.AddMonths(Ticks * count);
                return normaltime.AddTicks(-span.Ticks);
            }

            public override string GetOption(DateTime time)
            {
                if (Options == null || Options.Count == 0)
                    return null;
                int totalticks = Ticks * Options.Count;
                DateTime yearstart = toYearStart(time);
                for (int i = 0; i < Options.Count; i++)
                {
                    DateTime optionstart = yearstart.AddMonths(Ticks * i);
                    if (optionstart > time)
                    {
                        return Options[i - 1];
                    }
                }
                return Options[Options.Count - 1];
            }

            public override double TotalBetween(DateTime from, DateTime to)
            {
                return ((to.Year - from.Year) * 12 + (to.Month - from.Month)) / Ticks;
            }

            public override TickTypeRounder Clone()
            {
                return new MonthRounder(this);
            }
        }

        protected class WeekRounder : TickTypeRounder
        {
            public WeekRounder()
                : base()
            { }
            public WeekRounder(WeekRounder src)
                : base(src)
            { }
            public override TickTypeRounder Clone()
            {
                return new WeekRounder(this);
            }
            public override DateTime Round(DateTime time, string option)
            {
                TimeSpan span = time - FirstClock;
                int ticktimes = (int)(span.TotalDays / (7 * Ticks));
                return FirstClock.AddDays(ticktimes * Ticks * 7);
            }

            public override DateTime MoveN(DateTime time, int count)
            {
                return time.AddDays(7 * count * Ticks);
            }

            public override string GetOption(DateTime time)
            {
                return null;
            }

            public override double TotalBetween(DateTime from, DateTime to)
            {
                return (to - from).TotalDays / (7 * Ticks);
            }
        }

        protected class DayRounder : TickTypeRounder
        {
            public DayRounder()
                : base()
            { }
            public DayRounder(DayRounder src)
                : base(src)
            { }
            public override TickTypeRounder Clone()
            {
                return new DayRounder(this);
            }
            public override DateTime Round(DateTime time, string option)
            {
                TimeSpan span = time - FirstClock;
                int ticktimes = (int)(span.TotalDays / Ticks);
                return FirstClock.AddDays(Ticks * ticktimes);
            }

            public override DateTime MoveN(DateTime time, int count)
            {
                return time.AddDays(count * Ticks);
            }

            public override string GetOption(DateTime time)
            {
                return null;
            }

            public override double TotalBetween(DateTime from, DateTime to)
            {
                return (to - from).TotalDays / Ticks;
            }
        }

        protected class HourRounder : TickTypeRounder
        {
            public HourRounder()
                : base()
            { }
            public HourRounder(HourRounder src)
                : base(src)
            { }
            public override TickTypeRounder Clone()
            {
                return new HourRounder(this);
            }
            public override DateTime Round(DateTime time, string option)
            {
                TimeSpan span = time - FirstClock;
                if (string.IsNullOrEmpty(option))
                {
                    int ticktimes = (int)(span.TotalHours / Ticks);
                    return FirstClock.AddHours(ticktimes * Ticks);
                }
                else
                {
                    int optionticks = Ticks * Options.IndexOf(option);
                    int totalticks = Ticks * Options.Count;
                    int ticktimes = (int)(span.TotalHours / totalticks);
                    DateTime result = FirstClock.AddHours(ticktimes * totalticks + optionticks);
                    if (result > time)
                    {
                        result = result.AddHours(-totalticks);
                    }
                    return result;
                }//班
            }

            public override DateTime MoveN(DateTime time, int count)
            {
                return time.AddHours(count * Ticks);
            }

            public override string GetOption(DateTime time)
            {
                if (Options == null || Options.Count == 0)
                    return null;
                int totalticks = Ticks * Options.Count;
                TimeSpan span = time - FirstClock;
                int ticktimes = (int)(span.TotalHours / totalticks);
                for (int i = 0; i < Options.Count; i++)
                {
                    DateTime optionstart = FirstClock.AddHours(ticktimes * totalticks + Ticks * i);
                    if (optionstart > time)
                    {
                        return Options[i - 1];
                    }
                }
                return Options[Options.Count - 1];
            }

            public override double TotalBetween(DateTime from, DateTime to)
            {
                return (to - from).TotalHours / Ticks;
            }
        }

        protected class MinuteRounder : TickTypeRounder
        {
            public MinuteRounder()
                : base()
            { }
            public MinuteRounder(MinuteRounder src)
                : base(src)
            { }
            public override TickTypeRounder Clone()
            {
                return new MinuteRounder(this);
            }
            public override DateTime Round(DateTime time, string option)
            {
                TimeSpan span = time - FirstClock;
                if (string.IsNullOrEmpty(option))
                {
                    int ticktimes = (int)(span.TotalMinutes / Ticks);
                    return FirstClock.AddMinutes(ticktimes * Ticks);
                }
                else
                {
                    int optionticks = Ticks * Options.IndexOf(option);
                    int totalticks = Ticks * Options.Count;
                    int ticktimes = (int)(span.TotalMinutes / totalticks);
                    DateTime result = FirstClock.AddMinutes(ticktimes * totalticks + optionticks);
                    if (result > time)
                    {
                        result = result.AddMinutes(-totalticks);
                    }
                    return result;
                }//班
            }

            public override DateTime MoveN(DateTime time, int count)
            {
                return time.AddMinutes(count * Ticks);
            }

            public override string GetOption(DateTime time)
            {
                if (Options == null || Options.Count == 0)
                    return null;
                int totalticks = Ticks * Options.Count;
                TimeSpan span = time - FirstClock;
                int ticktimes = (int)(span.TotalMinutes / totalticks);
                for (int i = 0; i < Options.Count; i++)
                {
                    DateTime optionstart = FirstClock.AddMinutes(ticktimes * totalticks + Ticks * i);
                    if (optionstart > time)
                    {
                        return Options[i - 1];
                    }
                }
                return Options[Options.Count - 1];
            }

            public override double TotalBetween(DateTime from, DateTime to)
            {
                return (to - from).TotalMinutes / Ticks;
            }
        }

        protected class SecondRounder : TickTypeRounder
        {
            public SecondRounder()
                : base()
            { }
            public SecondRounder(SecondRounder src)
                : base(src)
            { }
            public override TickTypeRounder Clone()
            {
                return new SecondRounder(this);
            }
            public override DateTime Round(DateTime time, string option)
            {
                TimeSpan span = time - FirstClock;
                if (string.IsNullOrEmpty(option))
                {
                    long ticktimes = (long)(span.TotalSeconds / Ticks);
                    return FirstClock.AddSeconds(ticktimes * Ticks);
                }
                else
                {
                    int optionticks = Ticks * Options.IndexOf(option);
                    int totalticks = Ticks * Options.Count;
                    long ticktimes = (long)(span.TotalSeconds / totalticks);
                    DateTime result = FirstClock.AddSeconds(ticktimes * totalticks + optionticks);
                    if (result > time)
                    {
                        result = result.AddSeconds(-totalticks);
                    }
                    return result;
                }//班
            }

            public override DateTime MoveN(DateTime time, int count)
            {
                return time.AddSeconds(count * Ticks);
            }

            public override string GetOption(DateTime time)
            {
                if (Options == null || Options.Count == 0)
                    return null;
                int totalticks = Ticks * Options.Count;
                TimeSpan span = time - FirstClock;
                long ticktimes = (long)(span.TotalSeconds / totalticks);
                for (int i = 0; i < Options.Count; i++)
                {
                    DateTime optionstart = FirstClock.AddSeconds(ticktimes * totalticks + Ticks * i);
                    if (optionstart > time)
                    {
                        return Options[i - 1];
                    }
                }
                return Options[Options.Count - 1];
            }

            public override double TotalBetween(DateTime from, DateTime to)
            {
                return (to - from).TotalSeconds / Ticks;
            }
        }

        #endregion
    }

    /// <summary>
    /// 特定的周期变换对象
    /// </summary>
    [Serializable]
    public class CustomInterval : Interval
    {
        public CustomInterval(int seconds)
        {
            _Seconds = seconds;
            _FirstClock = new DateTime(2000, 1, 1);
            if (seconds > 1 && seconds < 60)
            {
                _SmallerId = 1;
                _BiggerId = 60;
            }
            else if (seconds > 60 && seconds < 3600)
            {
                _SmallerId = 60;
                _BiggerId = 3600;
            }
            else
            {
                throw new Exception(seconds + "超出了有效范围(1,3600)");
            }
            //get name
            _TickType = "秒";
            _Ticks = seconds;
            _Rounder = Interval.Rounders[_TickType].Clone();
            _Rounder.FirstClock = _FirstClock;
            _Rounder.Ticks = _Ticks;
        }

        public override string Format
        {
            get
            {
                return Smaller.Format;
            }
        }

        public override string Name
        {
            get
            {
                _Name = String.Empty;
                int remain = _Seconds;
                Interval smaller = Smaller;
                while (remain > 0)
                {
                    int times = remain / smaller.Seconds;
                    _Name += times + smaller.Name;
                    remain = remain - times * smaller.Seconds;
                    smaller = smaller.Smaller;
                }
                return _Name;
            }
        }
    }
}
