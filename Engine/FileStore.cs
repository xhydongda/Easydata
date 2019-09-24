using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Easydata.Engine
{
    // FileStore is an abstraction around multiple TSM files.
    public class FileStore : IStatable
    {
        readonly object lockthis = new object();
        // Most recently known file stats. If nil then stats will need to be
        // recalculated
        List<FileStat> lastFileStats;
        int currentGeneration;
        readonly string dir;
        List<TSMReader> files;
        bool traceLogging;
        readonly Purger purger;

        public FileStore(string dir)
        {
            this.dir = dir;
            _LastModified = Constants.MinTime;
            purger = new Purger();
            files = new List<TSMReader>();
        }

        #region IStatable
        struct FileStoreStatistics
        {
            public long DiskBytes;
            public long FileCount;
        }

        FileStoreStatistics stats;

        public string Name => "tsm1_filestore";

        public Dictionary<string, long> Stat()
        {
            Dictionary<string, long> values = new Dictionary<string, long>();
            values["diskBytes"] = Interlocked.Read(ref stats.DiskBytes);
            values["numFiles"] = Interlocked.Read(ref stats.FileCount);
            return values;
        }

        #endregion
        public void EnableTraceLogging(bool enabled)
        {
            lock (lockthis)
            {
                traceLogging = enabled;
            }
        }

        public int Count()
        {
            lock (lockthis)
            {
                return files.Count;
            }
        }

        public int NextGeneration()
        {
            return Interlocked.Increment(ref currentGeneration);
        }

        public void Delete(List<ulong> sids)
        {
            DeleteRange(sids, Constants.MinTime, Constants.MaxTime);
        }

        public void DeleteRange(List<ulong> sids, long min, long max)
        {
            List<TSMReader> copyfiles = new List<TSMReader>();
            lock (lockthis)
            {
                copyfiles.AddRange(files);
            }
            Task[] tasks = new Task[copyfiles.Count];
            int i = 0;
            foreach (TSMReader file in copyfiles)
            {
                tasks[i++] = Task.Run(() =>
                {
                    file.DeleteRange(sids, min, max);
                });
            }
            Task.WaitAll(tasks);
            LastModified = DateTime.Now.Ticks;
            lock (lockthis)
            {
                lastFileStats = null;
            }
        }

        // Open loads all the TSM files in the configured directory.
        public string Open()
        {
            lock (lockthis)
            {
                files.Clear();
                if (String.IsNullOrEmpty(dir))
                    return null;
                string[] tsmfiles = Directory.GetFiles(dir, "*." + Constants.TSMFileExtension);
                if (tsmfiles != null && tsmfiles.Length > 0)
                {
                    long lm = 0;
                    foreach (string fn in tsmfiles)
                    {
                        int generation, sequence;
                        string err = ParseTSMFileName(fn, out generation, out sequence);
                        if (err != null)
                        {
                            return err;
                        }
                        if (generation >= currentGeneration)
                        {
                            currentGeneration = generation + 1;
                        }
                        FileInfo file = new FileInfo(fn);
                        TSMReader df = new TSMReader(file);
                        if (traceLogging) Logger.Info(string.Format("{0} opened in {1}", fn, DateTime.Now));
                        files.Add(df);
                        stats.DiskBytes += df.Size;
                        foreach (FileStat ts in df.TombstoneFiles())
                        {
                            stats.DiskBytes += (long)ts.Size;
                        }
                        if (df.LastModified() > lm)
                            lm = df.LastModified();
                    }
                    LastModified = lm;
                    files.Sort();
                    stats.FileCount = files.Count;
                }
            }
            return null;
        }

        public static string ParseTSMFileName(string name, out int generation, out int sequence)
        {
            string str = Path.GetFileNameWithoutExtension(name);
            generation = 0;
            sequence = 0;
            int idx = str.IndexOf("-");
            if (idx < 0)
            {
                return string.Format("file {0} is named incorrectly", name);
            }
            generation = Convert.ToInt32(str.Substring(0, idx));
            sequence = Convert.ToInt32(str.Substring(idx + 1));
            return null;
        }

        public void Close()
        {
            lock (lockthis)
            {
                foreach (TSMReader file in files)
                {
                    file.Close();
                }
                lastFileStats = null;
                files = null;
                stats.FileCount = 0;
            }
        }

        public long DiskSizeBytes()
        {
            return Interlocked.Read(ref stats.DiskBytes);
        }


        public Dictionary<ulong, ClockValues> Read(List<ulong> sids, long start, long end)
        {
            Dictionary<ulong, ClockValues> result = new Dictionary<ulong, ClockValues>();
            List<TSMReader> files2;
            lock (lockthis)
            {
                files2 = new List<TSMReader>(files);
            }
            Task[] tasks = new Task[files2.Count];
            for (int i = 0; i < files2.Count; i++)
            {
                TSMReader file = files2[i];
                tasks[i] = Task.Run(() =>
                {
                    Dictionary<ulong, ClockValues> points = file.Read(sids, start, end);
                    if (points != null)
                    {
                        foreach (KeyValuePair<ulong, ClockValues> item in points)
                        {
                            lock (result)
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
        }//TODO:每个文件的时间范围是否可以像Shard一样已知？
        
        public Dictionary<ulong, ClockValues> Read(List<ulong> sids)
        {
            List<TSMReader> files2;
            lock (lockthis)
            {
                files2 = new List<TSMReader>(files);
            }
            Dictionary<ulong, ClockValues> result = new Dictionary<ulong, ClockValues>();
            Task[] tasks = new Task[files2.Count];
            for (int i = 0; i < files2.Count; i++)
            {
                TSMReader file = files2[i];
                tasks[i] = Task.Run(() =>
                {
                    Dictionary<ulong, ClockValues> points = file.Read(sids);
                    if (points != null)
                    {
                        foreach (KeyValuePair<ulong, ClockValues> item in points)
                        {
                            lock (result)
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
        }

        // Stats returns the stats of the underlying files, preferring the cached version if it is still valid.
        public List<FileStat> Stats()
        {
            lock (lockthis)
            {
                if (lastFileStats != null && lastFileStats.Count > 0)
                    return lastFileStats;

                if (lastFileStats == null)
                    lastFileStats = new List<FileStat>(files.Count);

                foreach (TSMReader fd in files)
                {
                    lastFileStats.Add(fd.Stats());
                }

                return lastFileStats;
            }
        }


        // Replace replaces oldFiles with newFiles.
        public void Replace(List<string> oldFiles, List<string> newFiles)
        {
            bool oldFilesNull = (oldFiles == null || oldFiles.Count == 0);
            if (oldFilesNull && (newFiles == null || newFiles.Count == 0))
                return;
            long maxTime = LastModified;
            List<TSMReader> updated = new List<TSMReader>(newFiles.Count);
            foreach (string file in newFiles)
            {
                string newName = file;
                if (file.EndsWith(".tmp"))
                {
                    // The new TSM files have a tmp extension.  First rename them.
                    newName = file.Substring(0, file.Length - 4);
                    File.Move(file, newName);
                }
                FileInfo fd = new FileInfo(newName);
                if (fd.LastWriteTime.Ticks > maxTime)
                    maxTime = fd.LastWriteTime.Ticks;
                TSMReader tsm = new TSMReader(fd);
                updated.Add(tsm);
            }
            lock (lockthis)
            {
                // Copy the current set of active files while we rename
                // and load the new files.  We copy the pointers here to minimize
                // the time that locks are held as well as to ensure that the replacement
                // is atomic.©
                updated.AddRange(files);
                List<TSMReader> active = new List<TSMReader>();
                List<TSMReader> inuse = new List<TSMReader>();
                foreach (TSMReader file in updated)
                {
                    bool keep = true;
                    if (!oldFilesNull)
                    {
                        foreach (string remove in oldFiles)
                        {
                            if (remove == file.Path)
                            {
                                keep = false;
                                // If queries are running against this file, then we need to move it out of the
                                // way and let them complete.  We'll then delete the original file to avoid
                                // blocking callers upstream.  If the process crashes, the temp file is
                                // cleaned up at startup automatically.
                                if (file.InUse)
                                {
                                    List<string> deletes = new List<string>();
                                    foreach (FileStat t in file.TombstoneFiles())
                                    {
                                        deletes.Add(t.Path);
                                    }
                                    deletes.Add(file.Path);
                                    // Rename the TSM file used by this reader
                                    string tempPath = file.Path + ".tmp";
                                    file.Rename(tempPath);
                                    // Remove the old file and tombstones.  We can't use the normal TSMReader.Remove()
                                    // because it now refers to our temp file which we can't remove.
                                    foreach (string f in deletes)
                                    {
                                        File.Delete(f);
                                    }

                                    inuse.Add(file);
                                    continue;
                                }
                                file.Close();
                                file.Remove();
                                break;
                            }
                        }
                    }
                    if (keep)
                    {
                        active.Add(file);
                    }
                }
                purger.Add(inuse);

                // If times didn't change (which can happen since file mod times are second level),
                // then add a ns to the time to ensure that lastModified changes since files on disk
                // actually did change
                if (maxTime == LastModified)
                {
                    maxTime = maxTime + 1;
                }
                LastModified = maxTime;
                lastFileStats = null;
                files = active;
                files.Sort();
                stats.FileCount = files.Count;
                // Recalculate the disk size stat
                long totalSize = 0;
                foreach (TSMReader file in files)
                {
                    totalSize += file.Size;
                    foreach (FileStat ts in file.TombstoneFiles())
                    {
                        totalSize += ts.Size;
                    }
                }
                stats.DiskBytes = totalSize;
            }
        }


        long _LastModified;
        public long LastModified
        {
            get
            {
                return Interlocked.Read(ref _LastModified);
            }
            set
            {
                Interlocked.Exchange(ref _LastModified, value);
            }
        }

        // BlockCount returns number of values stored in the block at location idx
        // in the file at path.  If path does not match any file in the store, 0 is
        // returned.  If idx is out of range for the number of blocks in the file,
        // 0 is returned.
        public int BlockCount(string path, int idx)
        {
            if (idx < 0)
                return 0;
            TSMReader file = null;
            lock (lockthis)
            {
                foreach (TSMReader fd in files)
                {
                    if (fd.Path == path)
                    {
                        file = fd;
                        break;
                    }
                }
            }
            if (file != null)
            {
                BlockIterator iter = file.BlockIterator();
                for (int i = 0; i < idx; i++)
                {
                    if (!iter.Next())
                        return 0;
                }
                Block block = iter.Read();
                return Encoding.BlockCount(block.Buf);
            }
            return 0;
        }
    }

    /// <summary>
    /// 在旧tsm文件不再被查询占用时删除.
    /// </summary>
    internal class Purger
    {
        readonly object lockthis = new object();
        Dictionary<string, TSMReader> files;
        bool running = false;

        public Purger()
        {
            files = new Dictionary<string, TSMReader>();
        }

        public void Add(List<TSMReader> files)
        {
            lock (lockthis)
            {
                foreach (TSMReader f in files)
                {
                    this.files[f.Path] = f;
                }
            }
            Purge();
        }

        public void Purge()
        {
            lock (lockthis)
            {
                if (running)
                    return;
                //compactLevel1
                Task.Run(async () =>
                {
                    while (true)
                    {
                        lock (lockthis)
                        {
                            List<string> todel = new List<string>();
                            foreach (KeyValuePair<string, TSMReader> pair in files)
                            {
                                if (!pair.Value.InUse)
                                {
                                    pair.Value.Close();
                                    pair.Value.Remove();
                                    todel.Add(pair.Key);
                                }
                            }
                            foreach (string key in todel)
                            {
                                files.Remove(key);
                            }
                            if (files.Count == 0)
                            {
                                running = false;
                                break;
                            }
                        }
                        await Task.Delay(1000);
                    }
                });
            }
        }
    }
}
