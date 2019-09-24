using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Easydata.Engine
{
    /// <summary>
    /// 存储策略，包含时限、副本数信息.
    /// 每个策略对应一个文件夹，里面是若干个Shard文件夹.
    /// </summary>
    public class RP
    {
        int _Interval;
        long expireticks;
        readonly object lockthis = new object();
        const string config_file = "rp.json";
        RPInfo info = null;
        Shards shards;
        Dictionary<ulong, bool> sidExists;
        CancellationTokenSource quitFlag;
        #region 构造函数                
        /// <summary>
        /// 新建存储策略对象.
        /// </summary>
        /// <param name="days">存储时长，过期自动删除</param>
        /// <param name="db">数据库实例</param>
        public RP(int days, Database db)
        {
            Days = days;
            expireticks = days * 864000000000;
            Path = string.Format("{0}{1}\\", db.Path, days);
            Database = db;
            shards = new Shards();
            sidExists = new Dictionary<ulong, bool>();
            _Interval = getShardInterval();

            load();
        }

        /// <summary>
        /// 获取数据库实例.
        /// </summary>
        public Database Database { get; }

        //从配置文件中加载shard列表.
        private void load()
        {
            string str = Path + config_file;
            if (File.Exists(str))
            {
                using (StreamReader sr = new StreamReader(str))
                {
                    string json = sr.ReadToEnd();
                    info = JsonConvert.DeserializeObject<RPInfo>(json);
                    foreach (ulong sid in info.Sids)
                    {
                        sidExists[sid] = true;
                    }
                }
                long nowticks = DateTime.Now.Ticks;
                foreach (ShardStartEnd shardstartend in info.Shards)
                {
                    Shard shard = new Shard(this, shardstartend.ShardId, shardstartend.Start.Ticks, shardstartend.End.Ticks);
                    shards.Add(shard);
                }
            }
            else
            {
                info = new RPInfo();
            }
        }

        //保存shard列表到配置文件.
        private void save()
        {
            string str = Path + config_file;
            string jsonstring = JsonConvert.SerializeObject(info);
            if (!Directory.Exists(Path))
                Directory.CreateDirectory(Path);
            using (StreamWriter sr = new StreamWriter(str, false))
            {
                sr.Write(jsonstring);
            }
        }

        /// <summary>
        /// 注册sids，只有注册后的数据点才能读写.
        /// </summary>
        /// <param name="sids">[Sid]</param>
        /// <returns>注册成功的sid数量</returns>
        public int AddSids(List<ulong> sids)
        {
            if (sids == null || sids.Count == 0)
                return 0;
            int result = 0;
            lock (lockthis)
            {
                foreach (ulong sid in sids)
                {
                    if (!sidExists.ContainsKey(sid))
                    {
                        sidExists.Add(sid, true);
                        info.Sids.Add(sid);
                        result++;
                    }
                }
                save();
            }
            return result;
        }

        /// <summary>
        /// 删除sids注册和数据.
        /// </summary>
        /// <param name="sids">[Sid]</param>
        /// <returns>删除成功的sid数量</returns>
        public int RemoveSids(List<ulong> sids)
        {
            if (sids == null || sids.Count == 0)
                return 0;
            int result = 0;
            lock (lockthis)
            {
                foreach (ulong sid in sids)
                {
                    if (sidExists.ContainsKey(sid))
                    {
                        sidExists.Remove(sid);
                        info.Sids.Remove(sid);
                        result++;
                    }
                }
                save();
                foreach (Shard shard in shards)
                {
                    shard.DeleteSids(sids);
                }
            }
            return result;
        }

        /// <summary>
        /// 是否有某sid.
        /// </summary>
        /// <param name="sid">sid</param>
        /// <returns>true=有</returns>
        public bool ContainsSid(ulong sid)
        {
            lock (lockthis)
            {
                return sidExists.ContainsKey(sid);
            }
        }

        public long DiskSize()
        {
            long result = 0;
            lock (lockthis)
            {
                foreach (Shard shard in shards)
                {
                    result += shard.DiskSize();
                }
            }
            return result;
        }

        private void startDeleteShardTask()
        {
            if (quitFlag == null)
            {
                quitFlag = new CancellationTokenSource();
                Task.Run(async () =>
                {
                    while (true)
                    {
                        deleteShard();
                        await Task.Delay(50, quitFlag.Token);
                    }
                }, quitFlag.Token);
            }
        }

        public void Open()
        {
            lock (lockthis)
            {
                foreach (Shard shard in shards)
                {
                    shard.Open(false);
                }
                startDeleteShardTask();
            }
        }

        //first是否已经早于RP的起始时间.
        private bool isExpired(long now, long first)
        {
            return now > expireticks + first;
        }

        DateTime lastDelete = DateTime.Now;
        //每小时检查过期shard，删除目录.
        private void deleteShard()
        {
            TimeSpan span = DateTime.Now - lastDelete;
            if (span.Hours > 1)
            {
                List<Shard> items;
                lock (lockthis)
                {
                    items = new List<Shard>(shards);
                }
                List<ulong> todel = new List<ulong>();
                foreach (Shard shard in items)
                {
                    if (isExpired(lastDelete.Ticks, shard.End))//lastTime延迟1小时删除
                    {
                        shard.Delete();
                        todel.Add(shard.ID);
                    }
                }
                if (todel.Count > 0)
                {
                    lock (lockthis)
                    {
                        foreach (ulong shardid in todel)
                        {
                            shards.RemoveById(shardid);
                        }
                    }
                }
                lastDelete = DateTime.Now;
            }
        }

        public void Close()
        {
            lock (lockthis)
            {
                if (quitFlag != null)
                {
                    quitFlag.Cancel();
                    quitFlag = null;
                }
                foreach (Shard shard in shards)
                {
                    shard.Close();
                }
            }
        }

        #endregion

        #region 属性
        /// <summary>
        /// 获得存储时长，单位天.
        /// </summary>
        public int Days { get; }

        /// <summary>
        /// 获得策略所在路径.
        /// </summary>
        public string Path { get; }
        #endregion

        /// <summary>
        /// 获取shard的时间跨度.
        /// </summary>
        /// <returns>时间跨度，单位秒</returns>
        private int getShardInterval()
        {
            if (Days < 2)
            {
                return 3600;
            }//hour
            if (Days < 180)
            {
                return 86400;
            }//day
            if (Days < 700)
            {
                return 604800;
            }//week
            return 2628000;//month
        }

        private Shard createShard(long clock)
        {
            DateTime time = new DateTime(clock);
            DateTime start = Intervals.Round(_Interval, time, null);
            DateTime end = Intervals.MoveN(_Interval, start, 1);
            ulong newshardid = Database.Store.NewShardId();
            Shard newshard = new Shard(this, newshardid, start.Ticks, end.Ticks);
            newshard.Open(true);
            lock (lockthis)
            {
                shards.Add(newshard);
                info.Shards.Add(new ShardStartEnd() { ShardId = newshardid, Start = start, End = end });
                save();
            }
            return newshard;
        }

        public int Write(Dictionary<ulong, ClockValues> points)
        {
            if (points == null || points.Count == 0)
                return 0;
            Dictionary<ulong, Dictionary<ulong, ClockValues>> shard_points = new Dictionary<ulong, Dictionary<ulong, ClockValues>>();
            ShardFinder finder;
            lock (lockthis)
            {
                finder = shards.CreateFinder();
            }
            long now = DateTime.Now.Ticks;
            foreach (KeyValuePair<ulong, ClockValues> sidvalues in points)
            {
                ulong sid = sidvalues.Key;
                foreach (IClockValue cv in sidvalues.Value)
                {
                    ulong shardid = finder.FindShardId(cv.Clock);
                    if (shardid == 0)
                    {
                        if (isExpired(now, cv.Clock))
                        {
                            continue;
                        }//太久远的数据不写入.
                        Shard newshard = createShard(cv.Clock);
                        shardid = newshard.ID;
                        finder.AddShard(shardid, newshard.Start, newshard.End);
                    }
                    if (!shard_points.ContainsKey(shardid))
                        shard_points.Add(shardid, new Dictionary<ulong, ClockValues>());
                    if (!shard_points[shardid].ContainsKey(sid))
                        shard_points[shardid].Add(sid, new ClockValues());
                    shard_points[shardid][sid].Append(cv);
                }
            }//按shard分组
            if (shard_points.Count > 1)
            {
                Task[] tasks = new Task[shard_points.Count];
                int i = 0;
                int result = 0;
                foreach (KeyValuePair<ulong, Dictionary<ulong, ClockValues>> pair in shard_points)
                {
                    Shard shard;
                    lock (lockthis)
                    {
                        shard = shards.GetById(pair.Key);
                    }
                    tasks[i] = Task.Run(() =>
                    {
                        int success = shard.Write(pair.Value);
                        Interlocked.Add(ref result, success);
                    });
                    i++;
                }
                Task.WaitAll(tasks);
                return result;
            }//多个shard，多线程写入.
            else
            {
                foreach (KeyValuePair<ulong, Dictionary<ulong, ClockValues>> pair in shard_points)
                {
                    Shard shard;
                    lock (lockthis)
                    {
                        shard = shards.GetById(pair.Key);
                    }
                    return shard.Write(pair.Value);
                }
            }
            return 0;
        }

        public Dictionary<ulong, ClockValues> Read(List<ulong> sids, long start, long end)
        {
            if (sids == null || sids.Count == 0)
                return null;
            ShardFinder finder;
            lock (lockthis)
            {
                finder = shards.CreateFinder();
            }
            List<ulong> shardids = null;
            shardids = finder.FindShards(start, end);
            if (shardids != null)
            {
                int n = shardids.Count;
                if (n > 1)
                {
                    Task[] tasks = new Task[n];
                    Dictionary<ulong, ClockValues> result = new Dictionary<ulong, ClockValues>();
                    for (int i = 0; i < n; i++)
                    {
                        ulong shardid = shardids[i];
                        Shard shard;
                        lock (lockthis)
                        {
                            shard = shards.GetById(shardid);
                        }
                        tasks[i] = Task.Run(() =>
                        {
                            Dictionary<ulong, ClockValues> shardresult;
                            if (i == 0 || i == n - 1)
                            {
                                shardresult = shard.Read(sids, start, end);
                            }
                            else
                            {
                                shardresult = shard.Read(sids);
                            }
                            if (shardresult != null)
                            {
                                lock (lockthis)
                                {
                                    foreach (KeyValuePair<ulong, ClockValues> item in shardresult)
                                    {
                                        if (!result.ContainsKey(item.Key))
                                            result.Add(item.Key, item.Value);
                                        else
                                            result[item.Key].AddRange(item.Value);
                                    }
                                }
                            }
                        });
                    }
                    Task.WaitAll(tasks);
                    return result;
                }//多线程读取
                else
                {
                    ulong shardid = shardids[0];
                    Shard shard;
                    lock (lockthis)
                    {
                        shard = shards.GetById(shardid);
                    }
                    return shard.Read(sids, start, end);
                }
            }
            return null;
        }
    }

    public class Shards : IList<Shard>
    {
        Dictionary<ulong, Shard> dic;//[shard id,shard]
        List<Shard> list;
        SortedList<long, ulong> startIds;//[shart start, shard id]
        public Shards()
        {
            list = new List<Shard>();
            dic = new Dictionary<ulong, Shard>();
            startIds = new SortedList<long, ulong>();
            MaxClock = 0;
        }

        #region IList成员
        public Shard this[int index]
        {
            get => list[index];
            set
            {
                Shard old = list[index];
                dic.Remove(old.ID);
                startIds.Remove(old.Start);
                list[index] = value;
                dic[value.ID] = value;
                startIds[value.Start] = value.ID;
                if (value.End > MaxClock) MaxClock = value.End;
            }
        }

        public int Count => list.Count;

        public bool IsReadOnly => false;

        public void Add(Shard item)
        {
            list.Add(item);
            dic[item.ID] = item;
            startIds[item.Start] = item.ID;
            if (item.End > MaxClock) MaxClock = item.End;
        }

        public void Clear()
        {
            list.Clear();
            dic.Clear();
            startIds.Clear();
            MaxClock = 0;
        }

        public bool Contains(Shard item)
        {
            return list.Contains(item);
        }

        public void CopyTo(Shard[] array, int arrayIndex)
        {
            list.CopyTo(array, arrayIndex);
        }

        public IEnumerator<Shard> GetEnumerator()
        {
            return list.GetEnumerator();
        }

        public int IndexOf(Shard item)
        {
            return list.IndexOf(item);
        }

        public void Insert(int index, Shard item)
        {
            list.Insert(index, item);
            dic[item.ID] = item;
            startIds[item.Start] = item.ID;
            if (item.End > MaxClock) MaxClock = item.End;
        }

        public bool Remove(Shard item)
        {
            dic.Remove(item.ID);
            startIds.Remove(item.Start);
            return list.Remove(item);
        }

        public void RemoveAt(int index)
        {
            Shard old = list[index];
            dic.Remove(old.ID);
            startIds.Remove(old.Start);
            list.RemoveAt(index);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable)list).GetEnumerator();
        }
        #endregion

        public Shard GetById(ulong id)
        {
            if (dic.ContainsKey(id))
            {
                return dic[id];
            }
            return null;
        }

        public bool RemoveById(ulong id)
        {
            if (dic.ContainsKey(id))
            {
                Shard shard = dic[id];
                return Remove(shard);
            }
            return false;
        }

        /// <summary>
        /// 所有shard的最大时间.
        /// </summary>
        public long MaxClock { get; private set; }

        public ShardFinder CreateFinder()
        {
            return new ShardFinder(startIds, MaxClock);
        }
    }

    public class ShardFinder
    {
        SortedList<long, ulong> startIds;//[shart start, shard id]
        long curstart, curend;
        ulong curshardid;
        public ShardFinder(SortedList<long, ulong> startIds, long maxclock)
        {
            this.startIds = new SortedList<long, ulong>(startIds);
            if (startIds.Count > 0)
            {
                curstart = startIds.Keys[startIds.Count - 1];
                curshardid = startIds[curstart];
            }
            curend = maxclock;
        }

        public void AddShard(ulong shardid, long start, long end)
        {
            startIds[start] = shardid;
            if (curend < end)
            {
                curend = end;
                curstart = start;
                curshardid = shardid;
            }
        }

        /// <summary>
        /// 获取给定时标所在的shard的id，不存在返回0.
        /// </summary>
        /// <param name="clock">数据时标</param>
        /// <returns>shard的id,不存在返回0</returns>
        public ulong FindShardId(long clock)
        {
            if (clock >= curend)
            {
                return 0;
            }//超出时间范围
            else if (clock >= curstart)
            {
                return curshardid;
            }
            int n = startIds.Count;
            if (n == 0)
            {
                return 0;
            }//没有任何shard.
            for (int i = n - 1; i >= 0; i--)
            {
                long start = startIds.Keys[i];
                if (start <= clock)
                {
                    return startIds[start];
                }
            }
            return 0;
        }

        /// <summary>
        /// 获取时间范围命中的shard id集合.
        /// </summary>
        /// <param name="start">开始时刻</param>
        /// <param name="end">结束时刻</param>
        /// <returns></returns>
        public List<ulong> FindShards(long start, long end)
        {
            List<ulong> result = new List<ulong>();
            int n = startIds.Count;
            if (start < curend && n > 0)
            {
                for (int i = n - 1; i >= 0; i--)
                {
                    long shardstart = startIds.Keys[i];
                    if (shardstart <= end)
                    {
                        result.Add(startIds[shardstart]);
                    }
                    if (shardstart <= start)
                    {
                        break;
                    }
                }
            }//超出时间范围
            return result;
        }
    }

    public class RPInfo
    {
        public RPInfo()
        {
            Shards = new List<ShardStartEnd>();
            Sids = new List<ulong>();
        }

        public List<ShardStartEnd> Shards { get; set; }

        public List<ulong> Sids { get; set; }
    }

    public class ShardStartEnd
    {
        public ShardStartEnd()
        {

        }

        public ulong ShardId { get; set; }

        public DateTime Start { get; set; }

        public DateTime End { get; set; }
    }
}
