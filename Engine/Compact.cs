using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Easydata.Engine
{
    // CompactionGroup represents a list of files eligible to be compacted together.
    public class CompactionGroup : List<string>
    {
    }

    // DefaultPlanner implements CompactionPlanner using a strategy to roll up
    // multiple generations of TSM files into larger files in stages.  It attempts
    // to minimize the number of TSM files on disk while rolling up a bounder number
    // of files.
    public class DefaultPlanner
    {
        readonly FileStore fileStore;

        // compactFullWriteColdDuration specifies the length of time after
        // which if no writes have been committed to the WAL, the engine will
        // do a full compaction of the TSM files in this shard. This duration
        // should always be greater than the CacheFlushWriteColdDuraion
        readonly long compactFullWriteColdDuration;
        // lastPlanCheck is the last time Plan was called
        long lastPlanCheck;

        readonly object lockthis = new object();

        // lastFindGenerations is the last time findGenerations was run
        long lastFindGenerations = Constants.MinTime;

        // lastGenerations is the last set of generations found by findGenerations
        TSMGenerations lastGenerations;

        // filesInUse is the set of files that have been returned as part of a plan and might
        // be being compacted.  Two plans should not return the same file at any given time.
        Dictionary<string, bool> filesInUse;

        public DefaultPlanner(FileStore fs, long writeColdDuration)
        {
            fileStore = fs;
            compactFullWriteColdDuration = writeColdDuration;
            filesInUse = new Dictionary<string, bool>();
        }

        // FullyCompacted returns true if the shard is fully compacted.
        public bool FullyCompacted()
        {
            TSMGenerations gens = findGenerations(false);
            return gens == null || (gens.Count <= 1 && !gens.HasTombstones());
        }

        long forceFull = 0;
        // ForceFull causes the planner to return a full compaction plan the next time
        // a plan is requested.  When ForceFull is called, level and optimize plans will
        // not return plans until a full plan is requested and released.
        public void ForceFull()
        {
            Interlocked.Exchange(ref forceFull, 1);
        }

        // PlanLevel returns a set of TSM files to rewrite for a specific level.
        public List<CompactionGroup> PlanLevel(int level)
        {
            if (Interlocked.Read(ref forceFull) == 1)
            {
                return null;
            }
            // Determine the generations from all files on disk.  We need to treat
            // a generation conceptually as a single file even though it may be
            // split across several files in sequence.
            TSMGenerations generations = findGenerations(true);
            // If there is only one generation and no tombstones, then there's nothing to
            // do.
            if (generations == null || (generations.Count <= 1 && !generations.HasTombstones()))
            {
                return null;
            }

            // Group each generation by level such that two adjacent generations in the same
            // level become part of the same group.
            TSMGenerations currentGen = new TSMGenerations();
            List<TSMGenerations> groups = new List<TSMGenerations>();
            for (int i = 0; i < generations.Count; i++)
            {
                TSMGeneration cur = generations[i];
                if (currentGen.Count == 0 || currentGen.Level() == cur.Level())
                {
                    currentGen.Add(cur);
                    continue;
                }
                groups.Add(currentGen);
                currentGen = new TSMGenerations();
                currentGen.Add(cur);
            }
            if (currentGen.Count > 0)
                groups.Add(currentGen);

            // Remove any groups in the wrong level
            List<TSMGenerations> levelGroups = new List<TSMGenerations>();
            foreach (TSMGenerations cur in groups)
            {
                if (cur.Level() == level)
                {
                    levelGroups.Add(cur);
                }
            }

            int minGenerations = 4;
            if (level == 1)
            {
                minGenerations = 8;
            }
            List<CompactionGroup> cGroups = new List<CompactionGroup>();
            foreach (TSMGenerations group in levelGroups)
            {
                foreach (TSMGenerations chunk in group.Chunk(minGenerations))
                {
                    CompactionGroup cGroup = new CompactionGroup();
                    bool hasTombstones = false;
                    foreach (TSMGeneration gen in chunk)
                    {
                        if (gen.HasTombstones())
                        {
                            hasTombstones = true;
                        }
                        foreach (FileStat file in gen.Files)
                        {
                            cGroup.Add(file.Path);
                        }
                    }
                    if (chunk.Count < minGenerations && !hasTombstones)
                    {
                        continue;
                    }
                    cGroups.Add(cGroup);
                }
            }
            if (!acquire(cGroups))
                return null;
            return cGroups;
        }

        // PlanOptimize returns all TSM files if they are in different generations in order
        // to optimize the index across TSM files.  Each returned compaction group can be
        // compacted concurrently.
        public List<CompactionGroup> PlanOptimize()
        {
            // If a full plan has been requested, don't plan any levels which will prevent
            // the full plan from acquiring them.
            bool b = (Interlocked.Read(ref forceFull) == 1);
            if (b)
            {
                return null;
            }
            // Determine the generations from all files on disk.  We need to treat
            // a generation conceptually as a single file even though it may be
            // split across several files in sequence.
            TSMGenerations generations = findGenerations(true);
            // If there is only one generation and no tombstones, then there's nothing to
            // do.
            if (generations == null || (generations.Count <= 1 && !generations.HasTombstones()))
            {
                return null;
            }

            // Group each generation by level such that two adjacent generations in the same
            // level become part of the same group.
            TSMGenerations currentGen = new TSMGenerations();
            List<TSMGenerations> groups = new List<TSMGenerations>();
            for (int i = 0; i < generations.Count; i++)
            {
                TSMGeneration cur = generations[i];
                // Skip the file if it's over the max size and contains a full block and it does not have any tombstones
                if (cur.Count() > 2
                    && cur.Size() > Constants.maxTSMFileSize
                    && fileStore.BlockCount(cur.Files[0].Path, 1) == Constants.DefaultMaxPointsPerBlock
                    && !cur.HasTombstones())
                {
                    continue;
                }

                // See if this generation is orphan'd which would prevent it from being further
                // compacted until a final full compactin runs.
                if (i < generations.Count - 1)
                {
                    if (cur.Level() < generations[i + 1].Level())
                    {
                        currentGen.Add(cur);
                        continue;
                    }
                }

                if (currentGen.Count == 0 || currentGen.Level() == cur.Level())
                {
                    currentGen.Add(cur);
                    continue;
                }
                groups.Add(currentGen);
                currentGen = new TSMGenerations();
                currentGen.Add(cur);
            }
            if (currentGen.Count > 0)
                groups.Add(currentGen);
            // Only optimize level 4 files since using lower-levels will collide
            // with the level planners
            List<TSMGenerations> levelGroups = new List<TSMGenerations>();
            foreach (TSMGenerations cur in groups)
            {
                if (cur.Level() == 4)
                {
                    levelGroups.Add(cur);
                }
            }

            List<CompactionGroup> cGroups = new List<CompactionGroup>();
            foreach (TSMGenerations group in levelGroups)
            {
                if (group.Count < 4 && !group.HasTombstones())
                {
                    continue;
                }

                CompactionGroup cGroup = new CompactionGroup();
                foreach (TSMGeneration gen in group)
                {
                    foreach (FileStat file in gen.Files)
                    {
                        cGroup.Add(file.Path);
                    }
                }
                cGroups.Add(cGroup);
            }
            if (!acquire(cGroups))
                return null;
            return cGroups;
        }

        // Plan returns a set of TSM files to rewrite for level 4 or higher.  The planning returns
        // multiple groups if possible to allow compactions to run concurrently.
        public List<CompactionGroup> Plan(long lastWrite)
        {
            TSMGenerations generations = findGenerations(true);
            bool b = (Interlocked.Read(ref forceFull) == 1);
            // first check if we should be doing a full compaction because nothing has been written in a long time
            if (b || compactFullWriteColdDuration > 0
                && (DateTime.Now.Ticks - lastWrite) > compactFullWriteColdDuration
                && generations.Count > 1)
            {
                // Reset the full schedule if we planned because of it.
                if (b)
                {
                    Interlocked.Exchange(ref forceFull, 0);
                }
                CompactionGroup tsmFiles = new CompactionGroup();
                int genCount = 0;
                int i = 0;
                foreach (TSMGeneration group in generations)
                {
                    bool skip = false;
                    // Skip the file if it's over the max size and contains a full block and it does not have any tombstones
                    if (generations.Count > 2
                        && group.Size() > Constants.maxTSMFileSize
                        && fileStore.BlockCount(group.Files[0].Path, 1) == Constants.DefaultMaxPointsPerBlock
                        && !group.HasTombstones())
                    {
                        skip = true;
                    }
                    // We need to look at the level of the next file because it may need to be combined with this generation
                    // but won't get picked up on it's own if this generation is skipped.  This allows the most recently
                    // created files to get picked up by the full compaction planner and avoids having a few less optimally
                    // compressed files.
                    if (i < generations.Count - 1)
                    {
                        if (generations[i + 1].Level() <= 3)
                        {
                            skip = false;
                        }
                    }
                    if (skip)
                    {
                        continue;
                    }
                    foreach (FileStat f in group.Files)
                    {
                        tsmFiles.Add(f.Path);
                    }
                    genCount++;
                    i++;
                }
                tsmFiles.Sort();
                // Make sure we have more than 1 file and more than 1 generation
                if (tsmFiles.Count <= 1 || genCount <= 1)
                    return null;
                List<CompactionGroup> result = new List<CompactionGroup>() { tsmFiles };
                if (!acquire(result))
                    return null;
                return result;
            }

            // don't plan if nothing has changed in the filestore
            if (lastPlanCheck > fileStore.LastModified
                && !generations.HasTombstones())
            {
                return null;
            }
            lastPlanCheck = DateTime.Now.Ticks;

            // If there is only one generation, return early to avoid re-compacting the same file
            // over and over again.
            if (generations.Count <= 1 && !generations.HasTombstones())
                return null;
            // Need to find the ending point for level 4 files.  They will be the oldest files. We scan
            // each generation in descending break once we see a file less than 4.
            int start = 0, end = 0;
            int j = 0;
            foreach (TSMGeneration g in generations)
            {
                if (g.Level() <= 3)
                {
                    break;
                }
                end = j + 1;
                j++;
            }
            // As compactions run, the oldest files get bigger.  We don't want to re-compact them during
            // this planning if they are maxed out so skip over any we see.
            bool hasTombstones = false;
            for (j = 0; j < end; j++)
            {
                TSMGeneration g = generations[j];
                if (g.HasTombstones())
                    hasTombstones = true;
                if (hasTombstones)
                    continue;
                // Skip the file if it's over the max size and contains a full block or the generation is split
                // over multiple files.  In the latter case, that would mean the data in the file spilled over
                // the 2GB limit.
                if (g.Size() > Constants.maxTSMFileSize
                    && fileStore.BlockCount(g.Files[0].Path, 1) == Constants.DefaultMaxPointsPerBlock)
                {
                    start = j + 1;
                }

                // This is an edge case that can happen after multiple compactions run.  The files at the beginning
                // can become larger faster than ones after them.  We want to skip those really big ones and just
                // compact the smaller ones until they are closer in size.
                if (j > 0)
                {
                    if (g.Size() * 2 < generations[j - 1].Size())
                    {
                        start = j;
                        break;
                    }
                }
            }

            // step is how many files to compact in a group.  We want to clamp it at 4 but also stil
            // return groups smaller than 4.
            int step = 4;
            if (step > end)
            {
                step = end;
            }

            // Loop through the generations in groups of size step and see if we can compact all (or
            // some of them as group)
            List<TSMGenerations> groups = new List<TSMGenerations>();
            generations = generations.Slice(start, end);
            for (j = 0; j < generations.Count; j += step)
            {
                bool skipGroup = false;
                int startIndex = j;
                for (int k = j; k < j + step && k < generations.Count; k++)
                {
                    TSMGeneration gen = generations[k];
                    int lvl = gen.Level();

                    // Skip compacting this group if there happens to be any lower level files in the
                    // middle.  These will get picked up by the level compactors.
                    if (lvl <= 3)
                    {
                        skipGroup = true;
                        break;
                    }
                    // Skip the file if it's over the max size and it contains a full block
                    if (gen.Size() >= Constants.maxTSMFileSize
                        && fileStore.BlockCount(gen.Files[0].Path, 1) == Constants.DefaultMaxPointsPerBlock
                        && !gen.HasTombstones())
                    {
                        startIndex++;
                        continue;
                    }
                }
                if (skipGroup)
                {
                    continue;
                }
                int endIndex = j + step;
                if (endIndex > generations.Count)
                    endIndex = generations.Count;
                if (endIndex - startIndex > 0)
                {
                    groups.Add(generations.Slice(startIndex, endIndex));
                }
            }
            if (groups.Count == 0)
            {
                return null;
            }

            // With the groups, we need to evaluate whether the group as a whole can be compacted
            List<TSMGenerations> compactable = new List<TSMGenerations>();
            foreach (TSMGenerations group in groups)
            {
                //if we don't have enough generations to compact, skip it
                if (group.Count < 4 && !group.HasTombstones())
                    continue;
                compactable.Add(group);
            }

            // All the files to be compacted must be compacted in order.  We need to convert each
            // group to the actual set of files in that group to be compacted.
            List<CompactionGroup> result2 = new List<CompactionGroup>();
            foreach (TSMGenerations c in compactable)
            {
                CompactionGroup cGroup = new CompactionGroup();
                foreach (TSMGeneration group in c)
                {
                    foreach (FileStat f in group.Files)
                    {
                        cGroup.Add(f.Path);
                    }
                }
                cGroup.Sort();
                result2.Add(cGroup);
            }
            if (!acquire(result2))
            {
                return null;
            }
            return result2;
        }

        // findGenerations groups all the TSM files by generation based
        // on their filename, then returns the generations in descending order (newest first).
        // If skipInUse is true, tsm files that are part of an existing compaction plan
        // are not returned.
        private TSMGenerations findGenerations(bool skipInUse)
        {
            lock (lockthis)
            {
                if (lastFindGenerations != Constants.MinTime
                    && fileStore.LastModified.Equals(lastFindGenerations))
                {
                    return lastGenerations;
                }
                long genTime = fileStore.LastModified;
                List<FileStat> tsmStats = fileStore.Stats();
                Dictionary<int, TSMGeneration> generations = new Dictionary<int, TSMGeneration>(tsmStats.Count);
                foreach (FileStat f in tsmStats)
                {
                    int gen, seq;
                    FileStore.ParseTSMFileName(f.Path, out gen, out seq);
                    if (skipInUse && filesInUse.ContainsKey(f.Path))
                        continue;
                    TSMGeneration group;
                    if (generations.ContainsKey(gen))
                        group = generations[gen];
                    else
                    {
                        group = new TSMGeneration() { ID = gen };
                        generations[gen] = group;
                    }
                    group.Files.Add(f);
                }
                TSMGenerations orderedGenerations = new TSMGenerations();
                foreach (TSMGeneration g in generations.Values)
                    orderedGenerations.Add(g);
                if (!orderedGenerations.IsSorted())
                    orderedGenerations.Sort();
                lastFindGenerations = genTime;
                lastGenerations = orderedGenerations;
                return orderedGenerations;
            }
        }

        private bool acquire(List<CompactionGroup> groups)
        {
            lock (lockthis)
            {
                // See if the new files are already in use
                foreach (CompactionGroup g in groups)
                {
                    foreach (string f in g)
                    {
                        if (filesInUse.ContainsKey(f) && filesInUse[f])
                            return false;
                    }
                }
                //Mark all the new files in use
                foreach (CompactionGroup g in groups)
                {
                    foreach (string f in g)
                    {
                        filesInUse[f] = true;
                    }
                }
                return true;
            }
        }
        // Release removes the files reference in each compaction group allowing new plans
        // to be able to use them.
        public void Release(List<CompactionGroup> groups)
        {
            lock (lockthis)
            {
                //Mark all the new files in use
                foreach (CompactionGroup g in groups)
                {
                    foreach (string f in g)
                    {
                        filesInUse[f] = false;
                    }
                }
            }
        }
    }

    // Compactor merges multiple TSM files into new files or
    // writes a Cache into 1 or more TSM files.
    public class Compactor
    {
        readonly object lockthis = new object();
        long snapshotsEnabled;
        long compactionsEnabled;
        List<string> inusefiles;

        readonly string dir;
        readonly int maxpoints;
        readonly FileStore fileStore;
        public Compactor(FileStore filestore, string dir, int maxpoints = 0)
        {
            this.dir = dir;
            this.fileStore = filestore;
            this.maxpoints = maxpoints;
            inusefiles = new List<string>();
        }

        // Open initializes the Compactor.
        public void Open()
        {
            if (Interlocked.Read(ref snapshotsEnabled) == 1 || Interlocked.Read(ref compactionsEnabled) == 1)
                return;
            Interlocked.Exchange(ref snapshotsEnabled, 1);
            Interlocked.Exchange(ref compactionsEnabled, 1);
        }

        // Close disables the Compactor.
        public void Close()
        {
            if (Interlocked.Read(ref snapshotsEnabled) == 1 || Interlocked.Read(ref compactionsEnabled) == 1)
                return;
            Interlocked.Exchange(ref snapshotsEnabled, 0);
            Interlocked.Exchange(ref compactionsEnabled, 0);
        }

        // DisableSnapshots disables the compactor from performing snapshots.
        public void DisabledSnapshots()
        {
            Interlocked.Exchange(ref snapshotsEnabled, 0);
        }

        // EnableSnapshots allows the compactor to perform snapshots.
        public void EnableSnapshots()
        {
            Interlocked.Exchange(ref snapshotsEnabled, 1);
        }
        // DisableCompactions disables the compactor from performing compactions.
        public void DisableCompactions()
        {
            Interlocked.Exchange(ref compactionsEnabled, 0);
        }

        // EnableCompactions allows the compactor to perform compactions.
        public void EnableCompactions()
        {
            Interlocked.Exchange(ref compactionsEnabled, 1);
        }

        // WriteSnapshot writes a Cache snapshot to one or more new TSM files.
        public (List<string>, string) WriteSnapshot(Cache cache)
        {
            if (Interlocked.Read(ref snapshotsEnabled) == 0)
            {
                return (null, Constants.errSnapshotsDisabled);
            }
            CacheSidIterator iter = new CacheSidIterator(cache, Constants.DefaultMaxPointsPerBlock);
            List<string> files = writeNewFiles(fileStore.NextGeneration(), 0, iter);
            if (Interlocked.Read(ref snapshotsEnabled) == 0)
            {
                return (null, Constants.errSnapshotsDisabled);
            }
            return (files, null);
        }

        private List<string> compact(bool fast, List<string> tsmFiles)
        {
            int size = maxpoints;
            if (size <= 0)
            {
                size = Constants.DefaultMaxPointsPerBlock;
            }
            // The new compacted files need to added to the max generation in the
            // set.  We need to find that max generation as well as the max sequence
            // number to ensure we write to the next unique location.
            int maxGeneration = 0, maxSequence = 0;
            foreach (string f in tsmFiles)
            {
                int gen, seq;
                FileStore.ParseTSMFileName(f, out gen, out seq);
                if (gen > maxGeneration)
                {
                    maxGeneration = gen;
                    maxSequence = seq;
                }
                if (gen == maxGeneration && seq > maxSequence)
                {
                    maxSequence = seq;
                }
            }
            List<TSMReader> trs = new List<TSMReader>();
            foreach (string file in tsmFiles)
            {
                FileInfo f = new FileInfo(file);
                TSMReader tr = new TSMReader(f);
                trs.Add(tr);
            }
            if (trs.Count == 0)
                return null;
            TSMSidIterator iter = new TSMSidIterator(size, fast, trs);
            return writeNewFiles(maxGeneration, maxSequence, iter);
        }
        public (List<string>, string) CompactFull(List<string> tsmFiles)
        {
            if (Interlocked.Read(ref snapshotsEnabled) == 0)
            {
                return (null, Constants.errSnapshotsDisabled);
            }
            if (!add(tsmFiles))
            {
                return (null, Constants.errCompactionInProgress);
            }
            List<string> files = compact(false, tsmFiles);
            if (Interlocked.Read(ref snapshotsEnabled) == 0)
            {
                removeTmpFiles(files);
                return (null, Constants.errSnapshotsDisabled);
            }
            remove(tsmFiles);//?defer c.remove(tsmFiles)
            return (files, null);
        }

        public (List<string>, string) CompactFast(List<string> tsmFiles)
        {
            if (Interlocked.Read(ref snapshotsEnabled) == 0)
            {
                return (null, Constants.errSnapshotsDisabled);
            }
            if (!add(tsmFiles))
            {
                return (null, Constants.errCompactionInProgress);
            }
            List<string> files = compact(true, tsmFiles);
            if (Interlocked.Read(ref snapshotsEnabled) == 0)
            {
                removeTmpFiles(files);
                return (null, Constants.errSnapshotsDisabled);
            }
            remove(tsmFiles);//?defer c.remove(tsmFiles)
            return (files, null);
        }

        private void removeTmpFiles(List<string> files)
        {
            foreach (string f in files)
            {
                File.Delete(f);
            }
        }

        // writeNewFiles writes from the iterator into new TSM files, rotating
        // to a new file once it has reached the max TSM file size.
        private List<string> writeNewFiles(int generation, int sequence, ISidIterator iter)
        {
            List<string> files = new List<string>();
            while (true)
            {
                sequence++;
                // New TSM files are written to a temp file and renamed when fully completed.
                string fileName = string.Format("{0}{1:d9}-{2:d9}.{3}.tmp", dir, generation, sequence, Constants.TSMFileExtension);
                // Write as much as possible to this file
                string err = write(fileName, iter);
                // We've hit the max file limit and there is more to write.  Create a new file
                // and continue.
                if (err == Constants.errMaxFileExceeded || err == Constants.ErrMaxBlocksExceeded)
                {
                    files.Add(fileName);
                    continue;
                }
                else if (err == Constants.ErrNoValues)
                {
                    // If the file only contained tombstoned entries, then it would be a 0 length
                    // file that we can drop.
                    File.Delete(fileName);
                    break;
                }
                else if (err == Constants.errCompactionInProgress)
                {
                    // Don't clean up the file as another compaction is using it.  This should not happen as the
                    // planner keeps track of which files are assigned to compaction plans now.
                    return null;
                }
                else if (err != null)
                {
                    // Remove any tmp files we already completed
                    foreach(string f in files)
                    {
                        File.Delete(f);
                    }
                    // We hit an error and didn't finish the compaction.  Remove the temp file and abort.
                    File.Delete(fileName);
                    return null;
                }
                files.Add(fileName);
                break;
            }
            return files;
        }

        private string write(string path, ISidIterator iter)
        {
            FileStream fd = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 2 << 15, FileOptions.SequentialScan);
            //FileStream fd = UnbufferStream.CreateForWrite(path);
            //MemoryMappedFile map = MemoryMappedFile.Create(path, MapProtection.PageReadWrite, 0);
            //Stream fd = map.MapView(MapAccess.FileMapWrite, 0, Constants.MAX_MAP_SIZE);
            TSMWriter w = new TSMWriter(fd);
            bool fileExceeded = false;
            try
            {
                while (iter.Next())
                {
                    if (Interlocked.Read(ref snapshotsEnabled) == 0 && Interlocked.Read(ref compactionsEnabled) == 0)
                    {
                        return Constants.errCompactionAborted;
                    }
                    //Each call to read returns the next sorted key(or the prior one if there are
                    //more values to write).  The size of values will be less than or equal to our
                    //chunk size(1000)
                    Block block = iter.Read();
                    w.WriteBlock(block.Sid, block.MinTime, block.MaxTime, block.Buf);
                    if (w.Size > Constants.maxTSMFileSize)
                    {
                        w.WriteIndex();
                        fileExceeded = true;
                        return Constants.errMaxFileExceeded;
                    }
                }
                //We're all done.  Close out the file.
                w.WriteIndex();
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
                Logger.Error(e.StackTrace);
                return e.Message;
            }
            finally
            {
                if (!fileExceeded)
                {
                    iter.Close();
                }//如果文件超过2G，iter还要继续使用，不能关闭.
                w.Close();
            }
            return null;
        }

        private bool add(List<string> files)
        {
            lock (lockthis)
            {
                // See if the new files are already in use
                foreach (string f in files)
                {
                    if (inusefiles.Contains(f))
                    {
                        return false;
                    }
                }
                foreach (string f in files)
                {
                    inusefiles.Add(f);
                }
                return true;
            }
        }

        private void remove(List<string> files)
        {
            lock (lockthis)
            {
                foreach (string f in files)
                {
                    if (inusefiles.Contains(f))
                    {
                        inusefiles.Remove(f);
                    }
                }
            }
        }
    }

    // tsmGeneration represents the TSM files within a generation.
    // 000001-01.tsm, 000001-02.tsm would be in the same generation
    // 000001 each with different sequence numbers.
    public class TSMGeneration : IComparable<TSMGeneration>
    {
        public int ID { get; set; }

        public List<FileStat> Files { get; set; }

        public TSMGeneration()
        {
            Files = new List<FileStat>();
        }
        // size returns the total size of the files in the generation.
        public long Size()
        {
            long n = 0;
            foreach (FileStat f in Files)
            {
                n += f.Size;
            }
            return n;
        }

        // compactionLevel returns the level of the files in this generation.
        public int Level()
        {
            // Level 0 is always created from the result of a cache compaction.  It generates
            // 1 file with a sequence num of 1.  Level 2 is generated by compacting multiple
            // level 1 files.  Level 3 is generate by compacting multiple level 2 files.  Level
            // 4 is for anything else.
            int generation, seq;
            FileStore.ParseTSMFileName(Files[Files.Count - 1].Path, out generation, out seq);
            if (seq < 4)
                return seq;
            return 4;
        }

        // count returns the number of files in the generation.
        public int Count()
        {
            return Files.Count;
        }

        // hasTombstones returns true if there are keys removed for any of the files.
        public bool HasTombstones()
        {
            foreach (FileStat f in Files)
            {
                if (f.HasTombstone)
                    return true;
            }
            return false;
        }


        public int CompareTo(TSMGeneration other)
        {
            return ID.CompareTo(other.ID);
        }
    }

    public class TSMGenerations : List<TSMGeneration>
    {
        public bool HasTombstones()
        {
            foreach (TSMGeneration g in this)
            {
                if (g.HasTombstones())
                    return true;
            }
            return false;
        }

        public int Level()
        {
            int result = 0;
            foreach (TSMGeneration g in this)
            {
                int lev = g.Level();
                if (lev > result)
                    result = lev;
            }
            return result;
        }

        //分块
        public List<TSMGenerations> Chunk(int size)
        {
            List<TSMGenerations> chunks = new List<TSMGenerations>();
            int j = 0;
            TSMGenerations onechunk = new TSMGenerations();
            chunks.Add(onechunk);
            for (int i = 0; i < Count; i++)
            {
                if (j >= size)
                {
                    j = 0;
                    onechunk = new TSMGenerations();
                    chunks.Add(onechunk);
                }
                onechunk.Add(this[i]);
                j++;
            }
            return chunks;
        }

        public bool IsSorted()
        {
            if (Count <= 1)
                return true;
            for (int i = 1; i < Count; i++)
            {
                if (this[i].CompareTo(this[i - 1]) < 0)
                    return false;
            }
            return true;
        }

        public TSMGenerations Slice(int start, int end)
        {
            TSMGenerations result = new TSMGenerations();
            for (int i = start; i < end; i++)
            {
                result.Add(this[i]);
            }
            return result;
        }
    }

    // KeyIterator allows iteration over set of keys and values in sorted order.
    public interface ISidIterator
    {
        // Next returns true if there are any values remaining in the iterator.
        bool Next();

        // Read returns the key, time range, and raw data for the next block,
        // or any error that occurred.
        Block Read();

        // Close closes the iterator.
        void Close();
    }

    public class TSMSidIterator : ISidIterator
    {
        // readers is the set of readers it produce a sorted key run with
        List<TSMReader> readers;
        // values is the temporary buffers for each key that is returned by a reader
        Dictionary<ulong, IClockValue> values;

        // indicates whether the iterator should choose a faster merging strategy over a more
        // optimally compressed one.  If fast is true, multiple blocks will just be added as is
        // and not combined.  In some cases, a slower path will need to be utilized even when
        // fast is true to prevent overlapping blocks of time for the same key.
        // If false, the blocks will be decoded and duplicated (if needed) and
        // then chunked into the maximally sized blocks.
        bool fast;
        // size is the maximum number of values to encode in a single block
        int size;
        // key is the current key lowest key across all readers that has not be fully exhausted
        // of values.
        ulong sid;

        byte typ;
        List<BlockIterator> iterators;
        List<Block> blocks;
        List<List<Block>> buf;

        // mergeValues are decoded blocks that have been combined
        ClockValues mergedValues;
        // merged are encoded blocks that have been combined or used as is
        // without decode
        List<Block> merged;

        public TSMSidIterator(int size, bool fast, List<TSMReader> readers)
        {
            iterators = new List<BlockIterator>();
            foreach (TSMReader r in readers)
            {
                iterators.Add(r.BlockIterator());
            }
            this.readers = readers;
            values = new Dictionary<ulong, IClockValue>();
            this.size = size;
            this.fast = fast;
            blocks = new List<Block>();
            mergedValues = new ClockValues();
            merged = new List<Block>();
            buf = new List<List<Block>>(iterators.Count);
            for (int i = 0; i < iterators.Count; i++)
            {
                buf.Add(new List<Block>());
            }
        }

        private bool hasMergedValues()
        {
            return mergedValues != null && mergedValues.Count > 0;
        }

        // Next returns true if there are any values remaining in the iterator.
        public bool Next()
        {
            // Any merged blocks pending?
            if (merged.Count > 0)
            {
                merged.RemoveAt(0);
                if (merged.Count > 0)
                    return true;
            }
            // Any merged values pending?
            if (hasMergedValues())
            {
                merge();
                if (merged.Count > 0 || hasMergedValues())
                    return true;
            }
            // If we still have blocks from the last read, merge them
            if (blocks.Count > 0)
            {
                merge();
                if (merged.Count > 0 || hasMergedValues())
                    return true;
            }
            // Read the next block from each TSM iterator
            int i = 0;
            foreach (List<Block> v in buf)
            {
                if (v.Count == 0)
                {
                    BlockIterator iter = iterators[i];
                    if (iter.Next())
                    {
                        // This block may have ranges of time removed from it that would
                        // reduce the block min and max time.
                        Block block = iter.Read();
                        List<TimeRange> tombstones = iter.TombstoneRange(sid);
                        block.TombStones = tombstones;

                        v.Add(block);
                        while (iter.PeekNext() == sid)
                        {
                            iter.Next();
                            block = iter.Read();
                            tombstones = iter.TombstoneRange(sid);
                            block.TombStones = tombstones;
                            v.Add(block);
                        }
                    }
                }
                i++;
            }
            // Each reader could have a different key that it's currently at, need to find
            // the next smallest one to keep the sort ordering.
            ulong minSid = 0;
            byte minType = 0;
            foreach (List<Block> b in buf)
            {
                // block could be nil if the iterator has been exhausted for that file
                if (b.Count == 0)
                    continue;
                if (minSid == 0 || b[0].Sid < minSid)
                {
                    minSid = b[0].Sid;
                    minType = b[0].Typ;
                }
            }
            sid = minSid;
            typ = minType;

            // Now we need to find all blocks that match the min key so we can combine and dedupe
            // the blocks if necessary
            foreach (List<Block> b in buf)
            {
                if (b.Count == 0)
                    continue;
                if (b[0].Sid == sid)
                {
                    blocks.AddRange(b);
                    b.Clear();
                }
            }
            if (blocks.Count == 0)
                return false;
            merge();
            return merged.Count > 0;
        }

        // merge combines the next set of blocks into merged blocks.
        private void merge()
        {
            // No blocks left, or pending merged values, we're done
            if (blocks.Count == 0 && merged.Count == 0 && mergedValues.Count == 0)
                return;
            bool dedup = (mergedValues.Count > 0);
            if (blocks.Count > 0 && !dedup)
            {
                // If we have more than one block or any partially tombstoned blocks, we many need to dedup
                dedup = (blocks[0].TombStones != null && blocks[0].TombStones.Count > 0) || blocks[0].PartiallyRead();
                // Quickly scan each block to see if any overlap with the prior block, if they overlap then
                // we need to dedup as there may be duplicate points now
                for (int i = 1; !dedup && i < blocks.Count; i++)
                {
                    if (blocks[i].PartiallyRead())
                    {
                        dedup = true;
                        break;
                    }
                    if (blocks[i].MinTime <= blocks[i - 1].MaxTime ||
                        (blocks[i].TombStones != null && blocks[i].TombStones.Count > 0))
                    {
                        dedup = true;
                        break;
                    }
                }
            }
            // combine returns a new set of blocks using the current blocks in the buffers.  If dedup
            // is true, all the blocks will be decoded, dedup and sorted in in order.  If dedup is false,
            // only blocks that are smaller than the chunk size will be decoded and combined.
            if (dedup)
            {
                while (mergedValues.Count < size && blocks.Count > 0)
                {
                    while (blocks.Count > 0 && blocks[0].ReadDone())
                    {
                        blocks.RemoveAt(0);//k.blocks = k.blocks[1:]
                    }
                    if (blocks.Count == 0)
                    {
                        break;
                    }
                    Block first = blocks[0];
                    long minTime = first.MinTime;
                    long maxTime = first.MaxTime;

                    // Adjust the min time to the start of any overlapping blocks.
                    for (int i = 0; i < blocks.Count; i++)
                    {
                        Block blocki = blocks[i];
                        if (blocki.OverlapsTimeRange(minTime, maxTime)
                            && !blocki.ReadDone())
                        {
                            if (blocki.MinTime < minTime)
                                minTime = blocki.MinTime;
                            if (blocki.MaxTime > minTime && blocki.MaxTime < maxTime)
                                maxTime = blocki.MaxTime;
                        }
                    }
                    // We have some overlapping blocks so decode all, append in order and then dedup
                    for (int i = 0; i < blocks.Count; i++)
                    {
                        Block blocki = blocks[i];
                        if (!blocki.OverlapsTimeRange(minTime, maxTime)
                            || blocki.ReadDone())
                            continue;
                        ClockValues v = Encoding.Decode(blocks[i].Buf, 0);
                        // Remove values we already read
                        v.Exclude(blocki.ReadMin, blocki.ReadMax);
                        // Filter out only the values for overlapping block
                        v.Include(minTime, maxTime);
                        if (v.Count > 0)
                        {
                            // Record that we read a subset of the block
                            blocki.MarkRead(v[0].Clock, v[v.Count - 1].Clock);
                        }
                        // Apply each tombstone to the block
                        if (blocki.TombStones != null)
                        {
                            foreach (TimeRange ts in blocki.TombStones)
                            {
                                v.Exclude(ts.Min, ts.Max);
                            }
                        }
                        mergedValues.AddRange(v);
                    }
                }
                // Since we combined multiple blocks, we could have more values than we should put into
                // a single block.  We need to chunk them up into groups and re-encode them.
                merged = chunk();
            }
            else
            {
                List<Block> chunked = new List<Block>();
                int i = 0;
                while (i < blocks.Count)
                {
                    // skip this block if it's values were already read
                    if (blocks[i].ReadDone())
                    {
                        i++;
                        continue;
                    }
                    // If we this block is already full, just add it as is
                    if (Encoding.BlockCount(blocks[i].Buf) > size)
                    {
                        chunked.Add(blocks[i]);
                    }
                    else
                    {
                        break;
                    }
                    i++;
                }
                if (fast)
                {
                    while (i < blocks.Count)
                    {
                        // skip this block if it's values were already read
                        if (blocks[i].ReadDone())
                        {
                            i++;
                            continue;
                        }
                        chunked.Add(blocks[i]);
                        i++;
                    }
                }
                // If we only have 1 blocks left, just append it as is and avoid decoding/recoding
                if (i == blocks.Count - 1)
                {
                    if (!blocks[i].ReadDone())
                    {
                        chunked.Add(blocks[i]);
                    }
                    i++;
                }
                // The remaining blocks can be combined and we know that they do not overlap and
                // so we can just append each, sort and re-encode.
                while (i < blocks.Count && mergedValues.Count < size)
                {
                    Block blocki = blocks[i];
                    if (blocki.ReadDone())
                    {
                        i++;
                        continue;
                    }
                    ClockValues v = Encoding.Decode(blocki.Buf, 0);
                    if (blocki.TombStones != null)
                    {
                        foreach (TimeRange ts in blocki.TombStones)
                        {
                            v.Exclude(ts.Min, ts.Max);
                        }
                    }
                    blocki.MarkRead(blocki.MinTime, blocki.MaxTime);
                    mergedValues.AddRange(v);
                    i++;
                }
                blocks.RemoveRange(0, i);
                chunked.AddRange(chunk());
                merged = chunked;
            }
        }

        private List<Block> chunk()
        {
            List<Block> dst = new List<Block>();
            if (mergedValues.Count > size)
            {
                IList<IClockValue> values = mergedValues.GetRange(0, size);
                var cb = Encoding.Encode(values, 0, values.Count);
                if (cb.error != null)
                {
                    return null;
                }
                dst.Add(new Block()
                {
                    MinTime = values[0].Clock,
                    MaxTime = values[values.Count - 1].Clock,
                    Sid = sid,
                    Buf = cb.Item1.EndWriteCopy()
                });
                cb.Item1.Release();
                mergedValues.RemoveRange(0, size);
                return dst;
            }
            // Re-encode the remaining values into the last block
            if (mergedValues.Count > 0)
            {
                var cb = Encoding.Encode(mergedValues, 0, mergedValues.Count);
                if (cb.error != null)
                {
                    return null;
                }
                ByteWriter byteWriter = cb.Item1;
                dst.Add(new Block()
                {
                    MinTime = mergedValues[0].Clock,
                    MaxTime = mergedValues[mergedValues.Count - 1].Clock,
                    Sid = sid,
                    Buf = byteWriter.EndWriteCopy()
                });
                byteWriter.Release();
                mergedValues.Clear();
            }
            return dst;
        }

        public Block Read()
        {
            if (merged.Count == 0)
                return null;
            return merged[0];
        }

        public void Close()
        {
            values = null;
            iterators = null;
            foreach (TSMReader r in readers)
            {
                r.Close();
            }
        }
    }

    // tsmKeyIterator implements the KeyIterator for set of TSMReaders.  Iteration produces
    // keys in sorted order and the values between the keys sorted and deduped.  If any of
    // the readers have associated tombstone entries, they are returned as part of iteration.
    public class CacheSidIterator : ISidIterator
    {
        Cache cache;
        int size;//一个 block 包含的点数，用于对数据进行分片
        List<ulong> sids;
        bool firstrun = true;
        int i;//sid index
        int j = -1;//block index for sid i
        List<List<Block>> blocks;
        int n;//sids.Count
        List<AutoResetEvent> ready;

        public CacheSidIterator(Cache cache, int size)
        {
            this.cache = cache;
            this.size = size;
            sids = cache.Sids();
            n = sids.Count;
            blocks = new List<List<Block>>(n);
            ready = new List<AutoResetEvent>(n);
            for (int k = 0; k < n; k++)
            {
                blocks.Add(new List<Block>());
                ready.Add(new AutoResetEvent(false));
            }
            encode();
        }

        private void encode()
        {
            // Divide the keyset across each CPU
            int chunkSize = 128;
            long idx = 0;
            while (true)
            {
                int start = (int)Interlocked.Read(ref idx);
                if (start >= n)
                    break;
                int end = start + chunkSize;
                if (end > n)
                    end = n;
                //Task.Run(() =>
                {
                    for (int k = start; k < end; k++)
                    {
                        ulong sid = sids[k];
                        ClockValues values = cache.Values(sid);
                        int valueIndex = 0;
                        while (valueIndex < values.Count)
                        {
                            long minTime = values[valueIndex].Clock;
                            long maxTime = values[values.Count - 1].Clock;
                            ByteWriter b;
                            string error;
                            if (values.Count - valueIndex > size)
                            {
                                maxTime = values[valueIndex + size - 1].Clock;
                                (b, error) = Encoding.Encode(values, valueIndex, size);
                                valueIndex += size;
                            }
                            else
                            {
                                (b, error) = Encoding.Encode(values, valueIndex, values.Count - valueIndex);
                                valueIndex = values.Count;
                            }
                            blocks[k].Add(new Block()
                            {
                                Sid = sid,
                                MinTime = minTime,
                                MaxTime = maxTime,
                                Buf = b.EndWriteCopy()
                            });
                            b.Release();
                        }
                        // Notify this key is fully encoded
                        ready[k].Set();
                    }
                }//);//encodeRange
                Interlocked.Add(ref idx, chunkSize);
            }
        }

        public bool Next()
        {
            if (firstrun)
            {
                firstrun = false;
                ready[0].WaitOne();
            }//first run
            if (j < blocks[i].Count - 1)
            {
                j++;
                return true;
            }
            i++;
            j = 0;
            if (i >= n)
                return false;
            ready[i].WaitOne();
            return true;
        }

        public Block Read()
        {
            return blocks[i][j];
        }

        public void Close()
        {
            //do nothing
        }
    }
}
