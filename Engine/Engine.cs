using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Easydata.Engine
{
    public class Engine : IStatable
    {
        CancellationTokenSource quitCompact;//channel to signal level compactions to stop
        CancellationTokenSource quitSnap;

        readonly object lockthis = new object();
        Shard shard;//shard
        readonly string path;
        Wal wal;
        Cache cache;
        Compactor compactor;
        DefaultPlanner compactionPlan;
        FileStore fileStore;
        bool enableCompactionsOnOpen;

        public Engine(Shard shard, string path, string walPath)
        {
            this.shard = shard;
            this.path = path;
            wal = new Wal(walPath);
            fileStore = new FileStore(path);
            cache = new Cache(Constants.DefaultCacheMaxMemorySize);
            compactor = new Compactor(fileStore, path);
            compactionPlan = new DefaultPlanner(fileStore, Constants.DefaultCompactFullWriteColdDuration);
            enableCompactionsOnOpen = true;
            stats = new EngineStatistics();
        }

        #region IStatable
        class EngineStatistics
        {
            public EngineStatistics()
            {
                CacheStat = new CompactionStat();
                LevelStats = new CompactionStat[3];
                LevelStats[0] = new CompactionStat();
                LevelStats[1] = new CompactionStat();
                LevelStats[2] = new CompactionStat();
                OptimizeStat = new CompactionStat();
                FullStat = new CompactionStat();
            }

            public CompactionStat CacheStat;
            public CompactionStat[] LevelStats;
            public CompactionStat OptimizeStat;
            public CompactionStat FullStat;
        }

        class CompactionStat
        {
            // Counter of TSM compactions (by level) that have ever run.
            public long Success;
            // Gauge of TSM compactions (by level) currently running.
            public long Active;
            //Counter of TSM compcations (by level) that have failed due to error.
            public long Errors;
            // Counter of number of wall nanoseconds spent in TSM compactions (by level).
            public long Duration;
        }
        EngineStatistics stats;

        public string Name => "tsm1_engine";

        public Dictionary<string, long> Stat()
        {
            Dictionary<string, long> values = new Dictionary<string, long>
            {
                ["cacheCompactions"] = Interlocked.Read(ref stats.CacheStat.Success),
                ["cacheCompactionsActive"] = Interlocked.Read(ref stats.CacheStat.Active),
                ["cacheCompactionErr"] = Interlocked.Read(ref stats.CacheStat.Errors),
                ["cacheCompactionDuration"] = Interlocked.Read(ref stats.CacheStat.Duration),

                ["tsmLevel1Compactions"] = Interlocked.Read(ref stats.LevelStats[0].Success),
                ["tsmLevel1CompactionsActive"] = Interlocked.Read(ref stats.LevelStats[0].Active),
                ["tsmLevel1CompactionErr"] = Interlocked.Read(ref stats.LevelStats[0].Errors),
                ["tsmLevel1CompactionDuration"] = Interlocked.Read(ref stats.LevelStats[0].Duration),

                ["tsmLevel2Compactions"] = Interlocked.Read(ref stats.LevelStats[1].Success),
                ["tsmLevel2CompactionsActive"] = Interlocked.Read(ref stats.LevelStats[1].Active),
                ["tsmLevel2CompactionErr"] = Interlocked.Read(ref stats.LevelStats[1].Errors),
                ["tsmLevel2CompactionDuration"] = Interlocked.Read(ref stats.LevelStats[1].Duration),

                ["tsmLevel3Compactions"] = Interlocked.Read(ref stats.LevelStats[2].Success),
                ["tsmLevel3CompactionsActive"] = Interlocked.Read(ref stats.LevelStats[2].Active),
                ["tsmLevel3CompactionErr"] = Interlocked.Read(ref stats.LevelStats[2].Errors),
                ["tsmLevel3CompactionDuration"] = Interlocked.Read(ref stats.LevelStats[2].Duration),

                ["tsmOptimizeCompactions"] = Interlocked.Read(ref stats.OptimizeStat.Success),
                ["tsmOptimizeCompactionsActive"] = Interlocked.Read(ref stats.OptimizeStat.Active),
                ["tsmOptimizeCompactionErr"] = Interlocked.Read(ref stats.OptimizeStat.Errors),
                ["tsmOptimizeCompactionDuration"] = Interlocked.Read(ref stats.OptimizeStat.Duration),

                ["tsmFullCompactions"] = Interlocked.Read(ref stats.FullStat.Success),
                ["tsmFullCompactionsActive"] = Interlocked.Read(ref stats.FullStat.Active),
                ["tsmFullCompactionErr"] = Interlocked.Read(ref stats.FullStat.Errors),
                ["tsmFullCompactionDuration"] = Interlocked.Read(ref stats.FullStat.Duration)
            };
            return values;
        }

        #endregion

        // SetEnabled sets whether the engine is enabled.
        public void SetEnabled(bool enabled)
        {
            enableCompactionsOnOpen = enabled;
            SetCompactionsEnabled(enabled);
        }

        // SetCompactionsEnabled enables compactions on the engine.  When disabled
        // all running compactions are aborted and new compactions stop running.
        public void SetCompactionsEnabled(bool enabled)
        {
            if (enabled)
            {
                enableSnapshotCompactions();
                enableLevelCompactions(false);
            }
            else
            {
                disableSnapshotCompactions();
                disableLevelCompactions(false);
            }
        }

        // enableLevelCompactions will request that level compactions start back up again.
        //
        // If 'wait' is set to true, then a corresponding call to enableLevelCompactions(true) will be
        // required before level compactions will start back up again.
        private void enableLevelCompactions(bool wait)
        {
            // If we don't need to wait, see if we're already enabled
            if (!wait)
            {
                lock (lockthis)
                {
                    if (quitCompact != null)
                        return;
                }
            }
            lock (lockthis)
            {
                if (quitCompact == null)
                {
                    quitCompact = new CancellationTokenSource();
                    //compactLevel1
                    Task.Run(async () =>
                    {
                        while (true)
                        {
                            compactTSMLevel(true, 1);
                            await Task.Delay(1000, quitCompact.Token);
                        }
                    }, quitCompact.Token);
                    //compactLevel2
                    Task.Run(async () =>
                    {
                        while (true)
                        {
                            compactTSMLevel(true, 2);
                            await Task.Delay(1000, quitCompact.Token);
                        }
                    }, quitCompact.Token);
                    //compactLevel3
                    Task.Run(async () =>
                    {
                        while (true)
                        {
                            compactTSMLevel(false, 3);
                            await Task.Delay(1000, quitCompact.Token);
                        }
                    }, quitCompact.Token);
                    //compactFull
                    Task.Run(async () =>
                    {
                        while (true)
                        {
                            compactTSMFull();
                            await Task.Delay(1000, quitCompact.Token);
                        }
                    }, quitCompact.Token);
                }
                compactor.EnableCompactions();
            }
        }

        // disableLevelCompactions will stop level compactions before returning.
        //
        // 'wait' signifies that a corresponding call to disableLevelCompactions(true) was made at some
        // point, and the associated task that required disabled compactions is now complete
        private void disableLevelCompactions(bool wait)
        {
            lock (lockthis)
            {
                if (quitCompact != null)
                {
                    quitCompact.Cancel();
                    quitCompact = null;
                }
            }
        }

        private void enableSnapshotCompactions()
        {
            // Check if already enabled under read lock
            lock (lockthis)
            {
                if (quitSnap != null)
                    return;
                quitSnap = new CancellationTokenSource();
                Task.Run(async () =>
                {
                    while (true)
                    {
                        compactCache();
                        await Task.Delay(1000, quitSnap.Token);
                    }
                }, quitSnap.Token);
            }
        }

        private void disableSnapshotCompactions()
        {
            lock (lockthis)
            {
                if (quitSnap != null)
                {
                    quitSnap.Cancel();
                    quitSnap = null;
                }
            }
        }

        public string Path()
        {
            return path;
        }

        // LastModified returns the time when this shard was last modified.
        public long LastModified()
        {
            long walTime = wal.LastWriteTime;
            long fsTime = fileStore.LastModified;
            if (walTime > fsTime)
                return walTime;
            return fsTime;
        }

        // DiskSize returns the total size in bytes of all TSM and WAL segments on disk.
        public long DiskSize()
        {
            return fileStore.DiskSizeBytes() + wal.DistSizeBytes();
        }

        // Open opens and initializes the engine.
        public void Open()
        {
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
            cleanup();
            reloadCache();
            wal.Open();
            fileStore.Open();
            compactor.Open();
            if (enableCompactionsOnOpen)
            {
                SetCompactionsEnabled(true);
            }
        }

        // Close closes the engine. Subsequent calls to Close are a nop.
        public void Close()
        {
            SetCompactionsEnabled(false);
            lock (lockthis)
            {
                fileStore.Close();
                wal.Close();
            }
        }

        // IsIdle returns true if the cache is empty, there are no running compactions and the
        // shard is fully compacted.
        public bool IsIdle()
        {
            bool cacheEmpty = (cache.Size == 0);

            long runningCompactions = Interlocked.Read(ref stats.CacheStat.Active);
            runningCompactions += Interlocked.Read(ref stats.LevelStats[0].Active);
            runningCompactions += Interlocked.Read(ref stats.LevelStats[1].Active);
            runningCompactions += Interlocked.Read(ref stats.LevelStats[2].Active);
            runningCompactions += Interlocked.Read(ref stats.FullStat.Active);
            runningCompactions += Interlocked.Read(ref stats.OptimizeStat.Active);
            return cacheEmpty && runningCompactions == 0 && compactionPlan.FullyCompacted();
        }

        // Backup writes a tar archive of any TSM files modified since the passed
        // in time to the passed in writer. The basePath will be prepended to the names
        // of the files in the archive. It will force a snapshot of the WAL first
        // then perform the backup with a read lock against the file store. This means
        // that new TSM files will not be able to be created in this shard while the
        // backup is running. For shards that are still acively getting writes, this
        // could cause the WAL to backup, increasing memory usage and evenutally rejecting writes.
        public void Backup()
        { }

        // writeFileToBackup copies the file into the tar archive. Files will use the shardRelativePath
        // in their names. This should be the <db>/<retention policy>/<id> part of the path.
        private void writeFileToBackup(string name, string shardRelativePath, string fullPath)
        {

        }

        // WritePoints writes point data into the engine.
        // It returns an error if new points are added to an existing key.
        public string Write(Dictionary<ulong, ClockValues> points)
        {
            lock (lockthis)
            {
                string error = cache.WriteMulti(points);
                if (error != null) return error;
                return wal.WriteMulti(points);
            }
        }
        public Dictionary<ulong, ClockValues> Read(List<ulong> sids, long start, long end)
        {
            Dictionary<ulong, ClockValues> result = new Dictionary<ulong, ClockValues>();
            lock (lockthis)
            {
                result = fileStore.Read(sids, start, end);
                foreach (ulong sid in sids)
                {
                    ClockValues cachedsidvalues = cache.Values(sid);
                    if (cachedsidvalues != null && cachedsidvalues.Count > 0)
                    {
                        foreach (IClockValue cv in cachedsidvalues)
                        {
                            if (cv.Clock >= start && cv.Clock <= end)
                            {
                                if (!result.ContainsKey(sid))
                                    result.Add(sid, new ClockValues());
                                result[sid].Add(cv);
                            }
                        }
                    }
                }
            }
            return result;
        }


        public Dictionary<ulong, ClockValues> Read(List<ulong> sids)
        {
            Dictionary<ulong, ClockValues> result = new Dictionary<ulong, ClockValues>();
            lock (lockthis)
            {
                result = fileStore.Read(sids);
                foreach (ulong sid in sids)
                {
                    ClockValues cachedsidvalues = cache.Values(sid);
                    if (cachedsidvalues != null && cachedsidvalues.Count > 0)
                    {
                        if (!result.ContainsKey(sid))
                            result.Add(sid, cachedsidvalues);
                        else
                            result[sid].AddRange(cachedsidvalues);
                    }
                }
            }
            return result;
        }

        // deleteSids removes all series keys from the engine.
        private void deleteSids(List<ulong> sids)
        {
            DeleteSidsRange(sids, Constants.MinTime, Constants.MaxTime);
        }

        // DeleteSidsRange removes the values between min and max (inclusive) from all series.
        public void DeleteSidsRange(List<ulong> sids, long min, long max)
        {
            if (sids == null || sids.Count == 0)
                return;

            // Ensure sids are sorted since lower layers require them to be.
            sids.Sort();
            // Disable and abort running compactions so that tombstones added existing tsm
            // files don't get removed.  This would cause deleted measurements/series to
            // re-appear once the compaction completed.  We only disable the level compactions
            // so that snapshotting does not stop while writing out tombstones.  If it is stopped,
            // and writing tombstones takes a long time, writes can get rejected due to the cache
            // filling up.
            disableLevelCompactions(true);
            try
            {
                cache.DeleteRange(sids, min, max);
                wal.DeleteRange(sids, min, max);
            }
            catch
            { }
            finally
            {
                enableLevelCompactions(true);
            }
        }

        private void compactTSMFull()
        {
            bool cold = shard.IsOld()
                && DateTime.Now.Ticks - LastModified() > Constants.DefaultCompactFullWriteColdDuration;
            if (cold)
            {
                compactionPlan.ForceFull();
                quitCompact.Cancel();
                quitCompact = null;
            }
            CompactionStrategy s = fullCompactionStrategy();
            if (s != null)
            {
                s.Apply();
                compactionPlan.Release(s.compactionGroups);
            }
        }

        private void compactTSMLevel(bool fast, int level)
        {
            CompactionStrategy s = levelCompactionStrategy(fast, level);
            if (s != null)
            {
                s.Apply();
                compactionPlan.Release(s.compactionGroups);
            }
        }


        // WriteSnapshot will snapshot the cache and write a new TSM file with its contents, releasing the snapshot when done.
        public string WriteSnapshot()
        {
            // Lock and grab the cache snapshot along with all the closed WAL
            // filenames associated with the snapshot
            List<string> closedFiles;
            Cache snapshot;
            string err = null;
            DateTime? started = DateTime.Now;
            lock (lockthis)
            {
                wal.CloseSegment();
                closedFiles = wal.ClosedSegments();
                snapshot = cache.Snapshot(out err);
            }
            List<string> newFiles = writeSnapshotAndCommit(closedFiles, snapshot);
            long ms = (long)(DateTime.Now - started.Value).TotalMilliseconds;
            cache.UpdateCompactTime(ms);
            if (newFiles != null)
            {
                foreach (string newFile in newFiles)
                {
                    Logger.Write(string.Format("{0} written in {1} ms", nameWithoutPath(newFile), ms));
                }
            }
            return err;
        }

        private static string nameWithoutPath(string filename)
        {
            if (String.IsNullOrEmpty(filename))
                return filename;
            FileInfo fi = new FileInfo(filename);
            return fi.Name;
        }

        // writeSnapshotAndCommit will write the passed cache to a new TSM file and remove the closed WAL segments.
        private List<string> writeSnapshotAndCommit(List<string> closedFiles, Cache snapshot)
        {
            var newFiles = compactor.WriteSnapshot(snapshot);
            if (newFiles.Item2 != null)
            {
                Logger.Write(string.Format("error writing snapshot from compactor: {0}", newFiles.Item2));
                return newFiles.Item1;
            }
            lock (lockthis)
            {
                fileStore.Replace(null, newFiles.Item1);
                cache.ClearSnapshot(true);
                wal.Remove(closedFiles);
            }
            return newFiles.Item1;
        }
        private void compactCache()
        {
            cache.UpdateAge();
            if (ShouldCompactCache(wal.LastWriteTime))
            {
                DateTime start = DateTime.Now;
                string err = WriteSnapshot();
                if (err != null && err != Constants.errCompactionsDisabled)
                {
                    Logger.Write(string.Format("error writing snapshot {0}", err));
                    Interlocked.Increment(ref stats.CacheStat.Errors);
                }
                else
                {
                    Interlocked.Increment(ref stats.CacheStat.Success);
                }
                Interlocked.Add(ref stats.CacheStat.Duration, (DateTime.Now - start).Ticks);
            }
        }

        // ShouldCompactCache returns true if the Cache is over its flush threshold
        // or if the passed in lastWriteTime is older than the write cold threshold.
        public bool ShouldCompactCache(long lastWriteTime)
        {
            long sz = cache.Size;
            if (sz == 0)
                return false;
            return sz > Constants.DefaultCacheSnapshotMemorySize
                || DateTime.Now.Ticks - lastWriteTime > Constants.DefaultCacheSnapshotWriteColdDuration;

        }

        // levelCompactionStrategy returns a compactionStrategy for the given level.
        // It returns nil if there are no TSM files to compact.
        private CompactionStrategy levelCompactionStrategy(bool fast, int level)
        {
            List<CompactionGroup> compactionGroups = compactionPlan.PlanLevel(level);
            if (compactionGroups == null || compactionGroups.Count == 0)
                return null;
            return new CompactionStrategy()
            {
                compactionGroups = compactionGroups,
                fileStore = fileStore,
                compactor = compactor,
                fast = fast,
                description = string.Format("level {0}", level),
                stat = stats.LevelStats[level - 1]
            };
        }

        // fullCompactionStrategy returns a compactionStrategy for higher level generations of TSM files.
        // It returns nil if there are no TSM files to compact.
        private CompactionStrategy fullCompactionStrategy()
        {
            bool optimize = false;
            List<CompactionGroup> compactionGroups = compactionPlan.Plan(wal.LastWriteTime);
            if (compactionGroups == null || compactionGroups.Count == 0)
            {
                optimize = true;
                compactionGroups = compactionPlan.PlanOptimize();
            }
            if (compactionGroups == null || compactionGroups.Count == 0)
            {
                return null;
            }
            CompactionStrategy result = new CompactionStrategy()
            {
                compactionGroups = compactionGroups,
                fileStore = fileStore,
                compactor = compactor,
                fast = optimize
            };
            if (optimize)
            {
                result.description = "optimize";
                result.stat = stats.OptimizeStat;
            }
            else
            {
                result.description = "full";
                result.stat = stats.FullStat;
            }
            return result;
        }

        // reloadCache reads the WAL segment files and loads them into the cache.
        private void reloadCache()
        {
            DateTime now = DateTime.Now;
            List<string> files = Wal.SegmentFileNames(wal.Path);
            if (files != null && files.Count > 0)
            {
                long maxsize = cache.MaxSize;
                cache.MaxSize = 0;//Disable the max size during loading
                cache.Load(files);
                cache.MaxSize = maxsize;
                Logger.Write(string.Format("Reloaded WAL cache {0} in {1} ms", wal.Path, (DateTime.Now - now).TotalMilliseconds));
            }
        }

        // cleanup removes all temp files and dirs that exist on disk.  This is should only be run at startup to avoid
        // removing tmp files that are still in use.
        private void cleanup()
        {
            if (!Directory.Exists(path))
                return;
            // Check to see if there are any `.tmp` directories that were left over from failed shard snapshots
            string[] tmppaths = Directory.GetDirectories(path, ".tmp");
            foreach (string tmppath in tmppaths)
            {
                try
                {
                    Directory.Delete(tmppath, true);
                }
                catch (Exception ex)
                {
                    Logger.Write(string.Format("error removing tmp snapshot directory {0}: {1}", tmppath, ex.Message));
                }
            }
            cleanupTempTSMFiles();
        }

        private void cleanupTempTSMFiles()
        {
            string[] files = Directory.GetFiles(path, string.Format("*.{0}", Constants.CompactionTempExtension));
            if (files != null)
            {
                foreach (string file in files)
                {
                    try
                    {
                        File.Delete(file);
                    }
                    catch (Exception ex)
                    {
                        Logger.Write(string.Format("error removing temp compaction files {0}: {1}", file, ex.Message));
                    }
                }
            }
        }

        // CompactionStrategy holds the details of what to do in a compaction.
        class CompactionStrategy
        {
            public List<CompactionGroup> compactionGroups;
            public bool fast;
            public string description;
            public CompactionStat stat;
            public Compactor compactor;
            public FileStore fileStore;

            // Apply concurrently compacts all the groups in a compaction strategy.
            public void Apply()
            {
                DateTime start = DateTime.Now;
                Task[] tasks = new Task[compactionGroups.Count];
                for (int i = 0; i < compactionGroups.Count; i++)
                {
                    CompactionGroup group = compactionGroups[i];
                    tasks[i] = Task.Run(async () =>
                    {
                        DateTime taskstart = DateTime.Now;
                        Logger.Write(string.Format("beginning {0} compaction of group {1}, {2} TSM files", description, i, group.Count));
                        for (int j = 0; j < group.Count; j++)
                        {
                            string f = nameWithoutPath(group[j]);
                            Logger.Write(string.Format("compacting {0} group ({1}) {2} (#{3})", description, i, f, j));
                        }
                        List<string> files = null;
                        string err = null;
                        // Count the compaction as active only while the compaction is actually running.
                        Interlocked.Increment(ref stat.Active);
                        try
                        {
                            if (fast)
                                (files, err) = compactor.CompactFast(group);
                            else
                                (files, err) = compactor.CompactFull(group);
                        }
                        finally
                        {
                            Interlocked.Decrement(ref stat.Active);
                        }
                        if (err != null)
                        {
                            if (err == Constants.errCompactionsDisabled || err == Constants.errCompactionInProgress)
                            {
                                Logger.Write(string.Format("aborted {0} compaction group ({1}). {2}", description, i, err));
                                if (err == Constants.errCompactionInProgress)
                                    await Task.Delay(1000);
                                return;
                            }
                            Logger.Write(string.Format("error compacting TSM files:{0}", err));
                            Interlocked.Increment(ref stat.Errors);
                            await Task.Delay(1000);
                            return;
                        }
                        fileStore.Replace(group, files);//? error handle
                        for (int j = 0; j < files.Count; j++)
                        {
                            string f = nameWithoutPath(files[j]);
                            Logger.Write(string.Format("compacted {0} group ({1}) into {2} (#{3})", description, i, f, j));
                        }

                        Logger.Write(string.Format("compacted {0} {1} files into {2} files in {3}ms", description, group.Count, files.Count, (DateTime.Now - taskstart).TotalMilliseconds));
                        Interlocked.Increment(ref stat.Success);
                    });
                }
                Task.WaitAll(tasks);
                TimeSpan span = DateTime.Now - start;
                Interlocked.Add(ref stat.Duration, span.Ticks);
            }
        }
    }
}
