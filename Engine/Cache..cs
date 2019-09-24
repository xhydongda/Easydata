using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace Easydata.Engine
{
    // Cache maintains an in-memory store of Values for a set of keys.
    public class Cache : IStatable
    {
        readonly object lockthis = new object();
        Dictionary<ulong, ClockValues> store;
        Cache snapshot;
        // snapshots are the cache objects that are currently being written to tsm files
        // they're kept in memory while flushing so they can be queried along with the cache.
        // they are read only and should never be modified
        public Cache()
        {
            store = new Dictionary<ulong, ClockValues>();
        }

        public Cache(long maxSize)
        {
            store = new Dictionary<ulong, ClockValues>();
            _MaxSize = maxSize;
            stats = new CacheStatistics();
            _lastSnapshot = DateTime.Now.Ticks;
            UpdateAge();
            UpdateCompactTime(0);
            updateCachedBytes(0);
            updateMemSize(0);
            updateSnapshots();
        }

        public long Size
        {
            get
            {
                return selfsize + snapshotSize;
            }
        }

        long _MaxSize = 0;
        public long MaxSize
        {
            get
            {
                return Interlocked.Read(ref _MaxSize);
            }
            set
            {
                Interlocked.Exchange(ref _MaxSize, value);
            }
        }

        long _lastSnapshot = 0;
        public long lastSnapshot
        {
            get
            {
                return Interlocked.Read(ref _lastSnapshot);
            }
            set
            {
                Interlocked.Exchange(ref _lastSnapshot, value);
            }
        }

        long _selfsize = 0;
        private long selfsize
        {
            get
            {
                return Interlocked.Read(ref _selfsize);
            }
            set
            {
                Interlocked.Exchange(ref _selfsize, value);
            }
        }

        long _snapshotSize = 0;
        private long snapshotSize
        {
            get
            {
                return Interlocked.Read(ref _snapshotSize);
            }
            set
            {
                Interlocked.Exchange(ref _snapshotSize, value);
            }
        }

        long _snapshotting = 0;
        private bool snapshotting
        {
            get
            {
                return Interlocked.Read(ref _snapshotting) == 1;
            }
            set
            {
                if (value) Interlocked.Exchange(ref _snapshotting, 1);
                else Interlocked.Exchange(ref _snapshotting, 0);
            }
        }

        #region IStatable
        struct CacheStatistics
        {
            public long MemSizeBytes;
            public long DiskSizeBytes;
            public long CacheAgeMs;
            public long CachedBytes;
            public long WALCompactionTimeMs;
            public long WriteOK;
            public long WriteErr;
            public long WriteDropped;
        }

        CacheStatistics stats;

        public string Name => "tsm1_cache";

        public Dictionary<string, long> Stat()
        {
            Dictionary<string, long> values = new Dictionary<string, long>
            {
                ["memBytes"] = Interlocked.Read(ref stats.MemSizeBytes),
                ["diskBytes"] = Interlocked.Read(ref stats.DiskSizeBytes),
                ["cacheAgeMs"] = Interlocked.Read(ref stats.CacheAgeMs),
                ["cachedBytes"] = Interlocked.Read(ref stats.CachedBytes),
                ["WALCompactionTimeMs"] = Interlocked.Read(ref stats.WALCompactionTimeMs),
                ["writeOk"] = Interlocked.Read(ref stats.WriteOK),
                ["writeErr"] = Interlocked.Read(ref stats.WriteErr),
                ["writeDropped"] = Interlocked.Read(ref stats.WriteDropped)
            };
            return values;
        }
        #endregion

        // Write writes the set of values for the key to the cache. This function is goroutine-safe.
        // It returns an error if the cache will exceed its max size by adding the new values.
        public string Write(ulong sid, ClockValues values)
        {
            long addedSize = values.Size;
            // Enough room in the cache?
            long limit = MaxSize;
            long n = Size + addedSize;
            if (limit > 0 && n > limit)
            {
                Interlocked.Increment(ref stats.WriteErr);
                return Constants.ErrCacheMemorySizeLimitExceeded;
            }
            lock (lockthis)
            {
                if (!store.ContainsKey(sid))
                    store.Add(sid, values);
                else
                {
                    addedSize -= store[sid].Size;
                    store[sid].AddRange(values);
                    addedSize += store[sid].Size;
                }
            }//lock store.
            selfsize += addedSize;
            updateMemSize(addedSize);
            Interlocked.Increment(ref stats.WriteOK);
            return null;
        }

        // WriteMulti writes the map of keys and associated values to the cache. This
        // function is goroutine-safe. It returns an error if the cache will exceeded
        // its max size by adding the new values.  The write attempts to write as many
        // values as possible.  If one key fails, the others can still succeed and an
        // error will be returned.
        public string WriteMulti(Dictionary<ulong, ClockValues> values)
        {
            foreach (KeyValuePair<ulong, ClockValues> pair in values)
            {
                string err = Write(pair.Key, pair.Value);
                if (err != null)
                {
                    return err;
                }
            }
            return null;
        }

        public Cache Snapshot(out string err)
        {
            err = null;
            lock (lockthis)
            {
                if (snapshotting)
                {
                    err = Constants.ErrSnapshotInProgress;
                    return null;
                }
                snapshotting = true;
                if (snapshot == null)
                {
                    snapshot = new Cache() { store = new Dictionary<ulong, ClockValues>(store) };
                }
                // If no snapshot exists, create a new one, otherwise update the existing snapshot
                if (snapshot.Size > 0)
                {
                    return snapshot;
                }
                snapshotSize = selfsize;
                var v = snapshot.store;
                snapshot.store = store;//cache与snapshot交换store
                store = v;
                store.Clear();
                selfsize = 0;
                lastSnapshot = DateTime.Now.Ticks;
                updateCachedBytes(snapshotSize);// increment the number of bytes added to the snapshot
                updateSnapshots();
                return snapshot;
            }//lock store,snapshot
        }

        public void ClearSnapshot(bool success)
        {
            lock (lockthis)
            {
                snapshotting = false;
                if (success)
                {
                    updateMemSize(-snapshotSize);
                    snapshot.store.Clear();
                    snapshotSize = 0;
                    updateSnapshots();
                }
            }
        }

        public List<ulong> Sids()
        {
            lock (lockthis)
            {
                List<ulong> result = new List<ulong>(store.Keys);
                result.Sort();
                return result;
            }
        }

        public List<ulong> UnsortedSids()
        {
            lock (lockthis)
            {
                return new List<ulong>(store.Keys);
            }
        }

        // Values returns a copy of all values, deduped and sorted, for the given key.
        public ClockValues Values(ulong sid)
        {
            ClockValues e = null, snap = null;
            lock (lockthis)
            {
                if (store.ContainsKey(sid))
                    e = store[sid];
                if (snapshot != null && snapshot.store.ContainsKey(sid))
                    snap = snapshot.store[sid];
            }
            if (e == null && snap == null)
                return null;
            else if (e == null)
                return new ClockValues(snap);
            else if (snap == null)
                return new ClockValues(e);
            else
            {
                ClockValues result = new ClockValues(e);
                result.AddRange(snap);
                return result;
            }
        }

        public void Delete(List<ulong> sids)
        {
            DeleteRange(sids, Constants.MinTime, Constants.MaxTime);
        }

        public void DeleteRange(List<ulong> sids, long min, long max)
        {
            lock (lockthis)
            {
                foreach (ulong sid in sids)
                {
                    if (!store.ContainsKey(sid))
                        continue;
                    ClockValues e = store[sid];
                    long origSize = e.Size;
                    if (min == Constants.MinTime && max == Constants.MaxTime)
                    {
                        selfsize -= origSize;
                        store.Remove(sid);
                        continue;
                    }
                    e.Exclude(min, max);
                    if (e.Count() == 0)
                    {
                        store.Remove(sid);
                        selfsize -= origSize;
                    }
                }
                Interlocked.Add(ref stats.MemSizeBytes, Size);
            }
        }

        public void UpdateAge()
        {
            long span = DateTime.Now.Ticks - lastSnapshot;
            Interlocked.Add(ref stats.CacheAgeMs, span / 10000);
        }

        public void UpdateCompactTime(long ms)
        {
            Interlocked.Add(ref stats.WALCompactionTimeMs, ms);
        }

        private void updateCachedBytes(long b)
        {
            Interlocked.Add(ref stats.CachedBytes, b);
        }

        private void updateMemSize(long b)
        {
            Interlocked.Add(ref stats.MemSizeBytes, b);
        }

        private void updateSnapshots()
        {
            Interlocked.Add(ref stats.DiskSizeBytes, snapshotSize);
        }
        // Load processes a set of WAL segment files, and loads a cache with the data
        // contained within those files.  Processing of the supplied files take place in the
        // order they exist in the files slice.
        public string Load(List<string> files)
        {
            foreach (string fn in files)
            {
                try
                {
                    using (FileStream f = File.Open(fn, FileMode.Open, FileAccess.ReadWrite, FileShare.Read))
                    {
                        Logger.Write(string.Format("reading file {0}, size {1}", fn, f.Length));
                        WalSegmentReader r = new WalSegmentReader(f);
                        while (r.Next())
                        {
                            IWalEntry entry = r.Read(out string err);
                            if (err != null)
                            {
                                long n = r.Count();
                                Logger.Write(string.Format("file {0} corrupt at position {1}, truncating", fn, n));
                                f.Seek(n, SeekOrigin.Begin);
                                f.SetLength(f.Length - n);
                                break;
                            }
                            switch (entry.GetType().Name)
                            {
                                case "WriteWalEntry":
                                    err = WriteMulti(((WriteWalEntry)entry).Values);
                                    if (err != null)
                                        return err;
                                    break;
                                case "DeleteRangeWalEntry":
                                    DeleteRange(((DeleteRangeWalEntry)entry).Sids, ((DeleteRangeWalEntry)entry).Min, ((DeleteRangeWalEntry)entry).Max);
                                    break;
                                case "DeleteWalEntry":
                                    Delete(((DeleteWalEntry)entry).Sids);
                                    break;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Logger.Error(ex.Message);
                }
            }
            return null;
        }
    }
}
