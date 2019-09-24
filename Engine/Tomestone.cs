using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;

namespace Easydata.Engine
{
    public class Tombstone
    {
        public ulong Sid { get; set; }
        public long Min { get; set; }
        public long Max { get; set; }
    }

    public delegate void WalkFn(Tombstone t, object obj);

    public class Tombstoner
    {
        object lockthis = new object();
        // cache of the stats for this tombstone
        List<FileStat> fileStats;
        // indicates that the stats may be out of sync with what is on disk and they
        // should be refreshed.
        bool statsLoaded;
        const int headerSize = 4;
        const int v3header = 0x1503;
        public Tombstoner(string path)
        {
            Path = path;
            fileStats = new List<FileStat>();
        }

        // Path is the location of the file to record tombstone. This should be the
        // full path to a TSM file.
        public string Path { get; private set; }

        public void Add(List<ulong> sids)
        {
            AddRange(sids, Constants.MinTime, Constants.MaxTime);
        }

        // AddRange adds all keys to the tombstone specifying only the data between min and max to be removed.
        public void AddRange(List<ulong> sids, long min, long max)
        {
            if (sids == null || sids.Count == 0)
                return;
            lock (lockthis)
            {
                if (string.IsNullOrEmpty(Path))
                    return;
                statsLoaded = false;
                List<Tombstone> tombstones = readTombstone();
                foreach (ulong sid in sids)
                {
                    tombstones.Add(new Tombstone() { Sid = sid, Min = min, Max = max });
                }
                writeTombstone(tombstones);
            }
        }

        public List<Tombstone> ReadAll()
        {
            return readTombstone();
        }

        public void Delete()
        {
            lock (lockthis)
            {
                File.Delete(tombstonePath());
                statsLoaded = false;
            }
        }

        public bool HasTombstones()
        {
            List<FileStat> files = TombstoneFiles();
            return files != null && files.Count > 0 && files[0].Size > 0;
        }

        public List<FileStat> TombstoneFiles()
        {
            lock (lockthis)
            {
                if (statsLoaded)
                {
                    return fileStats;
                }
                string filename = tombstonePath();
                if (!File.Exists(filename))
                {
                    statsLoaded = false;
                    fileStats.Clear();
                    return fileStats;
                }
                FileInfo info = new FileInfo(tombstonePath());
                fileStats.Add(new FileStat()
                {
                    Path = filename,
                    LastModified = info.LastWriteTime.Ticks,
                    Size = (uint)info.Length
                });
                statsLoaded = true;
                return fileStats;
            }
        }

        public void Walk(WalkFn fn, object obj)
        {
            lock (lockthis)
            {
                using (FileStream fs = File.Open(Path, FileMode.Open))
                {
                    using (GZipStream gzip = new GZipStream(fs, CompressionMode.Decompress))
                    {
                        byte[] bytes = new byte[headerSize];
                        int count = gzip.Read(bytes, 0, 4);
                        if (count == 4)
                        {
                            while (true)
                            {
                                try
                                {
                                    bytes = new byte[24];
                                    count = gzip.Read(bytes, 0, 24);
                                    if (count < 24)
                                        break;
                                    Tombstone tombstone = new Tombstone();
                                    tombstone.Sid = BitConverter.ToUInt64(bytes, 0);
                                    tombstone.Min = BitConverter.ToInt64(bytes, 8);
                                    tombstone.Max = BitConverter.ToInt64(bytes, 16);
                                    fn(tombstone, obj);
                                }
                                catch (Exception ex)
                                {
                                    Logger.Error("Walk error: " + ex.Message);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

        private List<Tombstone> readTombstone()
        {
            List<Tombstone> result = new List<Tombstone>();
            Walk(new WalkFn(append), result);
            return result;
        }

        private void append(Tombstone t, object obj)
        {
            ((List<Tombstone>)obj).Add(t);
        }

        private void writeTombstone(List<Tombstone> tombstones)
        {
            string tempFile = System.IO.Path.GetTempFileName();
            using (FileStream fs = new FileStream(tempFile, FileMode.Open))
            {
                using (GZipStream gzip = new GZipStream(fs, CompressionMode.Compress))
                {
                    gzip.Write(BitConverter.GetBytes(v3header), 0, 4);
                    foreach (Tombstone t in tombstones)
                    {
                        gzip.Write(BitConverter.GetBytes(t.Sid), 0, 8);
                        gzip.Write(BitConverter.GetBytes(t.Min), 0, 8);
                        gzip.Write(BitConverter.GetBytes(t.Max), 0, 8);
                    }
                }
            }
            File.Copy(tempFile, tombstonePath());
            File.Delete(tempFile);
        }

        private string tombstonePath()
        {
            if (Path.EndsWith("tombstone"))
                return Path;
            if (System.IO.Path.HasExtension(Path))
            {
                return System.IO.Path.ChangeExtension(Path, ".tombstone");
            }// Path is 0000001.tsm1
            else
            {
                return System.IO.Path.Combine(Path, ".tombstone");
            }
        }
    }
}
