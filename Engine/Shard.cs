using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace Easydata.Engine
{
    public class Shard : IStatable, IComparable<Shard>
    {
        readonly object lockthis = new object();
        Engine engine;
        bool enabled;

        public Shard(RP rp, ulong id, long start, long end)
        {
            RP = rp;
            ID = id;
            Start = start;
            End = end;
            Path = string.Format("{0}{1}\\", rp.Path, id);
            WalPath = Path;
        }

        #region IStatable
        struct ShardStatistics
        {
            public long WriteReq;
            public long WriteReqOK;
            public long WriteReqErr;
            public long WritePointsErr;
            public long WritePointsDropped;
            public long WritePointsOK;
            public long BytesWritten;
            public long DiskBytes;
        }

        ShardStatistics stats;

        public string Name => "shard";

        public Dictionary<string, long> Stat()
        {
            Dictionary<string, long> values = new Dictionary<string, long>();
            values["writeReq"] = Interlocked.Read(ref stats.WriteReq);
            values["writeReqOk"] = Interlocked.Read(ref stats.WriteReqOK);
            values["writeReqErr"] = Interlocked.Read(ref stats.WriteReqErr);
            values["writePointsErr"] = Interlocked.Read(ref stats.WritePointsErr);
            values["writePointsDropped"] = Interlocked.Read(ref stats.WritePointsDropped);
            values["writePointsOk"] = Interlocked.Read(ref stats.WritePointsOK);
            values["writeBytes"] = Interlocked.Read(ref stats.BytesWritten);
            values["diskBytes"] = Interlocked.Read(ref stats.DiskBytes);
            return values;
        }

        #endregion

        public bool IsOld()
        {
            return End <= DateTime.Now.Ticks;
        }

        public string Path { get; }

        public string WalPath { get; }

        public RP RP { get; }

        public void SetEnabled(bool enabled)
        {
            lock (lockthis)
            {
                this.enabled = enabled;
                if (engine != null)
                {
                    engine.SetEnabled(enabled);
                }
            }
        }

        public ulong ID { get; }

        public long Start { get; }

        public long End { get; }

        public void Open(bool create)
        {
            lock (lockthis)
            {
                if (engine != null)
                    return;

                engine = new Engine(this, Path, WalPath);
                // Disable compactions while loading the index
                engine.SetEnabled(false);
                engine.Open();
                if (create || !IsOld() || !IsIdle())
                {
                    engine.SetEnabled(true);
                }
            }
        }

        public void Close()
        {
            lock (lockthis)
            {
                if (engine == null)
                    return;
                engine.Close();
                engine = null;
            }
        }

        private bool ready()
        {
            lock (lockthis)
            {
                return engine != null && enabled;
            }
        }

        public bool IsIdle()
        {
            if (!ready())
                return true;
            return engine.IsIdle();
        }

        public void SetCompactionsEnabled(bool enabled)
        {
            if (!ready())
                return;
            engine.SetCompactionsEnabled(enabled);
        }

        public long DiskSize()
        {
            long size = engine.DiskSize();
            lock (lockthis)
            {
                stats.DiskBytes = size;
            }
            return size;
        }

        // WritePoints will write the raw data points and any new metadata to the index in the shard.
        public int Write(Dictionary<ulong, ClockValues> points)
        {
            lock (lockthis)
            {
                if (!enabled)
                {
                    SetEnabled(true);
                }
            }//当shard过期后会设置为false，有新数据写入时自动设置为true.
            if (!ready())
                return 0;
            lock (lockthis)
            {
                Interlocked.Increment(ref stats.WriteReq);
                string error = engine.Write(points);
                int succeedcount = 0;
                if (error == null)
                {
                    foreach (ClockValues values in points.Values)
                    {
                        succeedcount += values.Count;
                    }
                    Interlocked.Add(ref stats.WritePointsOK, succeedcount);
                    Interlocked.Increment(ref stats.WriteReqOK);
                }
                return succeedcount;
            }
        }

        public Dictionary<ulong, ClockValues> Read(List<ulong> sids, long start, long end)
        {
            return engine.Read(sids, start, end);
        }

        public Dictionary<ulong, ClockValues> Read(List<ulong> sids)
        {
            return engine.Read(sids);
        }

        public void Delete()
        {
            Close();
            Directory.Delete(Path, true);
        }

        public void DeleteSids(List<ulong> sids)
        {
            DeleteSidsRange(sids, Constants.MinTime, Constants.MaxTime);
        }


        public void DeleteSidsRange(List<ulong> sids, long min, long max)
        {
            if (!ready())
                return;
            engine.DeleteSidsRange(sids, min, max);
        }

        public int CompareTo(Shard other)
        {
            return Start.CompareTo(other.Start);
        }
    }
}
