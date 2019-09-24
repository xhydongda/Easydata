using System;
using System.Collections.Generic;
using System.Text;

namespace Easydata.Engine
{
    /// <summary>
    /// ���ڱ任��.
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
            
            //ע��Ԥ���������
            string intervalsRecords = @"1,��,2000/1/1,yyyy��MM��dd�� HH:mm:ss,��,1,,,,,60
                                        60,��,2000/1/1,yyyy��MM��dd�� HH:mm,��,1,,,,1,3600
                                        900,��,2000/1/1,yyyy��MM��dd�� HH:mm,��,15,,,,60,3600
                                        3600,ʱ,2000/1/1,yyyy��MM��dd�� HH,ʱ,1,,,,60,86400
                                        28800,��,2000/1/1,yyyy��MM��dd��,ʱ,8,,,ҹ.��.��,3600,86400
                                        86400,��,2000/1/1,yyyy��MM��dd��,��,1,,,,3600,2628000
                                        604800,��,2000/1/3,yyyy��MM��dd��,��,1,,,,86400,
                                        2628000,��,2000/1/1,yyyy��MM��,��,1,,,,86400,31536000
                                        7884000,����,2000/1/1,yyyy��,��,3,,,һ����.������.������.�ļ���,2628000,31536000
                                        15768000,����,2000/1/1,yyyy��,��,6,,,�ϰ���.�°���,2628000,31536000
                                        31536000,��,2000/1/1,yyyy��,��,1,,,,2628000,";
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
        /// ����ʱ��ֵ(���������һ�����ڼ������
        /// </summary>
        /// <param name="seconds">����</param>
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
        /// ��ȡһ�����ڵ����Ƿ�
        /// </summary>
        /// <param name="seconds">���ڵ���ֵ</param>
        /// <returns>���Ƿ�</returns>
        public static string GetName(int seconds)
        {
            Interval obj = get(seconds);
            if (obj != null)
                return obj.Name;
            return seconds + "��";
        }

        /// <summary>
        /// ��ȡ�ڿؼ�����ʾʱ�ĸ�ʽ
        /// </summary>
        /// <param name="seconds">���ڵ���ֵ</param>
        /// <returns>��ʽ�ַ���</returns>
        public static string GetFormat(int seconds)
        {
            Interval obj = get(seconds);
            if (obj != null)
                return obj.Format;
            return "yyyy��MM��dd�� HH:mm:ss";
        }

        /// <summary>
        /// ����ʱ���ѡ������� ���ȡ����� ��
        /// </summary>
        /// <param name="seconds">���ڵ���ֵ</param>
        /// <returns>����ʱ���ѡ��,û�з���null</returns>
        public static List<string> GetOptions(int seconds)
        {
            Interval obj = get(seconds);
            if (obj != null)
                return obj.Options;
            return null;
        }

        /// <summary>
        /// ��ʱ���������ȡ�������С�ڵ���time
        /// �� time = 2009-02-01 20:00:01 option = һ���� ȡ����Ľ���� 2009-01-01 00:00:00
        /// </summary>
        /// <param name="seconds">���ڵ���ֵ</param>
        /// <param name="time">��ȡ��ʱ�䣬ȡ����Ľ�����ڽ��ڸ�ʱ��֮ǰ</param>
        /// <param name="option">����ʱ��ѡ�񣬿���Ϊ��</param>
        /// <returns>ȡ�����ʱ��</returns>
        public static DateTime Round(int seconds, DateTime time, string option)
        {
            Interval obj = get(seconds);
            if (obj != null)
                return obj.Round(time, option);
            return time;
        }

        /// <summary>
        /// Ӧ������ƫ��.
        /// </summary>
        /// <param name="seconds">���ڵ���ֵ</param>
        /// <param name="time">ԭʼʱ�䣬ʵ�ʵ�����ʱ��</param>
        /// <returns>ƫ�ƺ��ʱ�䣬����ǰ����ʾ</returns>
        public static DateTime Offset(int seconds, DateTime time)
        {
            Interval obj = get(seconds);
            if (obj != null)
                return obj.Offset(time);
            return time;
        }

        /// <summary>
        /// ��ȡĳʱ������Ӧ�ĸ���ʱ����
        /// </summary>
        /// <param name="seconds">���ڵ���ֵ</param>
        /// <param name="time">ʱ��</param>
        /// <returns>����ʱ���û�з���null</returns>
        public static string GetOption(int seconds, DateTime time)
        {
            Interval obj = get(seconds);
            if (obj != null)
                return obj.GetOption(time);
            return null;
        }

        /// <summary>
        /// ������ת����time ���� times ������
        /// </summary>
        /// <param name="seconds">���ڵ���ֵ</param>
        /// <param name="time">ʱ��</param>
        /// <param name="count">��ת��������</param>
        /// <returns></returns>
        public static DateTime MoveN(int seconds, DateTime time, int times)
        {
            Interval obj = get(seconds);
            if (obj != null)
                return obj.MoveN(time, times);
            return time.AddSeconds(seconds * times);
        }

        /// <summary>
        /// ��ȡ�ٽ�С����
        /// </summary>
        /// <param name="seconds">���ڵ���ֵ</param>
        /// <returns>�ٽ�С���ڣ�û�з���-1</returns>
        public static int GetSmaller(int seconds)
        {
            Interval obj = get(seconds);
            if (obj != null && obj.Smaller != null)
                return obj.Smaller.Seconds;
            return -1;
        }

        /// <summary>
        /// ��ȡ�ٽ�������
        /// </summary>
        /// <param name="seconds">���ڵ���ֵ</param>
        /// <returns>�ٽ������ڣ�û�з���-1</returns>
        public static int GetBigger(int seconds)
        {
            Interval obj = get(seconds);
            if (obj != null && obj.Bigger != null)
                return obj.Bigger.Seconds;
            return -1;
        }

        /// <summary>
        /// �������������ӽ������п�ʼʱ�䵽����ʱ���ʱ���
        /// </summary>
        /// <param name="seconds">����</param>
        /// <param name="from">��ʼʱ��</param>
        /// <param name="to">����ʱ��</param>
        /// <returns></returns>
        public static double GetTotalBetween(int seconds, DateTime from, DateTime to)
        {
            Interval obj = get(seconds);
            if (obj != null)
                return obj.TotalBetween(from, to);
            return 1;
        }

        /// <summary>
        /// �������������Ϣ����������ֵ
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
                    _Rounders.Add("��", new YearRounder());
                    _Rounders.Add("��", new MonthRounder());
                    _Rounders.Add("��", new WeekRounder());
                    _Rounders.Add("��", new DayRounder());
                    _Rounders.Add("ʱ", new HourRounder());
                    _Rounders.Add("��", new MinuteRounder());
                    _Rounders.Add("��", new SecondRounder());
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
        /// �������ڵ���ֵ����ʵ����ֵ���ܲ�һ�£�����������.
        /// </summary>
        public int Seconds { get { return _Seconds; } }

        protected string _Name;
        /// <summary>
        /// ���ڵ����Ƿ�.
        /// </summary>
        public virtual string Name { get { return _Name; } }

        protected string _Format;
        /// <summary>
        /// �ڿؼ�����ʾʱ�ĸ�ʽ.
        /// </summary>
        public virtual string Format { get { return _Format; } }

        protected DateTime _FirstClock;
        /// <summary>
        /// ��һ����ʼ��.
        /// </summary>
        public DateTime FirstClock { get { return _FirstClock; } }

        List<string> _Options;
        /// <summary>
        /// ����ʱ���ѡ������� ���ȡ����� ��
        /// </summary>
        public List<string> Options { get { return _Options; } }

        protected string _TickType;
        /// <summary>
        /// ����ʱ��������.
        /// </summary>
        public string TickType { get { return _TickType; } }

        protected int _Ticks;
        /// <summary>
        /// һ�������൱�ڶ�������ʱ����.
        /// </summary>
        public int Ticks { get { return _Ticks; } }

        protected string _OffsetTickType;
        /// <summary>
        /// ����ʱ��������.
        /// </summary>
        public string OffsetTickType { get { return _OffsetTickType; } }

        protected int _OffsetTicks = 0;
        /// <summary>
        /// ������ʼ������ʼ������ʾʹ�õ�ƫ��.
        /// </summary>
        public int OffsetTicks { get { return _OffsetTicks; } }

        protected int? _SmallerId;
        /// <summary>
        /// Сһ�������ڵ�Id.
        /// </summary>
        public int? SmallerId { get { return _SmallerId; } }

        protected Interval _Smaller;
        /// <summary>
        /// Сһ��������.
        /// </summary>
        public Interval Smaller { get { return _Smaller; } set { _Smaller = value; } }

        protected int? _BiggerId;
        /// <summary>
        /// ��һ�������ڵ�Id.
        /// </summary>
        public int? BiggerId { get { return _BiggerId; } }

        protected Interval _Bigger;
        /// <summary>
        /// ��һ��������.
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
                }//���ȡ����� ����
            }

            public override DateTime MoveN(DateTime time, int count)
            {
                DateTime normaltime = Intervals.Offset(2628000, time);//����׼��
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
                }//��
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
                }//��
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
                }//��
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
    /// �ض������ڱ任����
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
                throw new Exception(seconds + "��������Ч��Χ(1,3600)");
            }
            //get name
            _TickType = "��";
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
