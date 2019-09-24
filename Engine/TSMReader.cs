using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace Easydata.Engine
{
    public class TSMReader : IComparable<TSMReader>
    {
        // refs is the count of active references to this reader.
        long refs;
        readonly object lockthis = new object();
        // index is the index of all blocks.
        TSMIndex index;
        // tombstoner ensures tombstoned keys are not available by the index.
        Tombstoner tombstoner;
        // lastModified is the last time this file was modified on disk
        readonly long lastModified;
        //MemoryMappedFile map;
        Stream fs;
        public TSMReader(FileInfo f)
        {
            Size = f.Length;
            lastModified = f.LastWriteTime.Ticks;
            Path = f.FullName;
            fs = new FileStream(Path, FileMode.Open, FileAccess.Read, FileShare.Read, 2 << 15, FileOptions.SequentialScan);
            //fs = UnbufferStream.CreateForRead(path);
            //map = MemoryMappedFile.Create(path, MapProtection.PageReadOnly, f.Length);
            //fs = map.MapView(MapAccess.FileMapRead, 0, (int)f.Length);
            init();
            tombstoner = new Tombstoner(Path);
        }

        private void init()
        {
            //verify version
            fs.Seek(0, SeekOrigin.Begin);
            byte[] bytes = new byte[4];
            fs.Read(bytes, 0, 4);
            if (BitConverter.ToUInt32(bytes, 0) != Constants.MagicNumber)
            {
                throw new Exception("init: error reading magic number of file");
            }
            if (fs.ReadByte() != Constants.Version)
            {
                throw new Exception("init: file version unexpected");
            }
            if (fs.Length < 8)
            {
                throw new Exception("Accessor: byte slice too small for indirectIndex");
            }
            /*
            //整块写入时最后8个字节为索引起始位置的偏移长度，而非索引起始位置本身
            long indexOfsLength = fs.Length - 8;
            fs.Seek(indexOfsLength, SeekOrigin.Begin);
            byte[] byte8 = new byte[8];
            fs.Read(byte8, 0, 8);
            long indexOfsPos = BitConverter.ToInt64(byte8, 0) - 8;
            fs.Seek(indexOfsPos, SeekOrigin.Begin);//
            fs.Read(byte8, 0, 8);
            ulong indexStart = BitConverter.ToUInt64(byte8, 0);*/

            //正常读写最后8个字节为索引起始位置
            long indexOfsPos = fs.Length - 8;
            fs.Seek(indexOfsPos, SeekOrigin.Begin);
            byte[] byte8 = new byte[8];
            fs.Read(byte8, 0, 8);
            ulong indexStart = BitConverter.ToUInt64(byte8, 0);
            if (indexStart >= (ulong)indexOfsPos)
            {
                throw new Exception("Accessor: invalid indexStart");
            }
            index = new TSMIndex();
            long indexLength = indexOfsPos - (long)indexStart;
            byte[] indexBytes = new byte[indexLength];
            fs.Seek((long)indexStart, SeekOrigin.Begin);
            fs.Read(indexBytes, 0, (int)indexLength);
            index.UnmarshalBinary(indexBytes, 0, (int)indexLength);
        }

        private void applyTombstones()
        {
            Tombstone cur = null, prev = null;
            List<ulong> batch = new List<ulong>(4096);
            tombstoner.Walk(new WalkFn(walk), new object[] { cur, prev, batch });
            if (batch.Count > 0)
            {
                index.DeleteRange(batch, cur.Min, cur.Max);
            }
        }

        private void walk(Tombstone ts, object obj)
        {
            object[] paras = (object[])obj;
            Tombstone cur = (Tombstone)paras[0];
            Tombstone prev = (Tombstone)paras[1];
            List<ulong> batch = (List<ulong>)paras[2];
            cur = ts;
            if (batch.Count > 0)
            {
                if (prev.Min != cur.Min || prev.Max != cur.Max)
                {
                    index.DeleteRange(batch, prev.Min, prev.Max);
                    batch.Clear();
                }
            }
            batch.Add(ts.Sid);
            if (batch.Count > 4096)
            {
                index.DeleteRange(batch, prev.Min, prev.Max);
                batch.Clear();
            }
            prev = ts;
        }

        public string Path { get; private set; }

        public ulong Sid(int idx, out byte typ, out List<IndexEntry> entries)
        {
            lock (lockthis)
            {
                return index.Sid(idx, out typ, out entries);
            }
        }

        public ulong SidAt(int idx)
        {
            lock (lockthis)
            {
                return index.SidAt(idx);
            }
        }

        public Dictionary<ulong, ClockValues> Read(List<ulong> sids, long start, long end)
        {
            try
            {
                Interlocked.Increment(ref refs);
                Dictionary<ulong, ClockValues> result = new Dictionary<ulong, ClockValues>();
                foreach (ulong sid in sids)
                {
                    ClockValues sidvalues = null;
                    lock (lockthis)
                    {
                        List<IndexEntry> blocks = index.Entries(sid);
                        if (blocks == null || blocks.Count == 0)
                            continue;
                        List<TimeRange> tombstones = index.TombstoneRange(sid);
                        bool hasTombstone = (tombstones != null && tombstones.Count > 0);
                        foreach (IndexEntry block in blocks)
                        {
                            if (!block.OverlapsTimeRange(start, end))
                                continue;
                            bool skip = false;
                            if (hasTombstone)
                            {
                                foreach (TimeRange t in tombstones)
                                {
                                    // Should we skip this block because it contains points that have been deleted
                                    if (t.Min <= block.MinTime && t.Max >= block.MaxTime)
                                    {
                                        skip = true;
                                        break;
                                    }
                                }
                            }
                            if (skip)
                            {
                                continue;
                            }
                            //TODO: Validate checksum
                            ClockValues temp;
                            if (block.MinTime >= start && block.MaxTime <= end)
                            {
                                temp = readBlock(block);
                            }
                            else
                            {
                                temp = readBlock(block, start, end);
                            }
                            if (hasTombstone)
                            {
                                foreach (TimeRange t in tombstones)
                                {
                                    temp.Exclude(t.Min, t.Max);
                                }
                            }
                            if (sidvalues == null)
                                sidvalues = temp;
                            else
                                sidvalues.AddRange(temp);
                        }
                    }
                    if (sidvalues != null && sidvalues.Count > 0)
                    {
                        result.Add(sid, sidvalues);
                    }
                }
                return result;
            }
            finally
            {
                Interlocked.Decrement(ref refs);
            }
        }

        public Dictionary<ulong, ClockValues> Read(List<ulong> sids)
        {
            try
            {
                Interlocked.Increment(ref refs);
                Dictionary<ulong, ClockValues> result = new Dictionary<ulong, ClockValues>();
                foreach (ulong sid in sids)
                {
                    ClockValues sidvalues = null;
                    lock (lockthis)
                    {
                        List<IndexEntry> blocks = index.Entries(sid);
                        if (blocks == null || blocks.Count == 0)
                            continue;
                        List<TimeRange> tombstones = index.TombstoneRange(sid);
                        bool hasTombstone = (tombstones != null && tombstones.Count > 0);
                        foreach (IndexEntry block in blocks)
                        {
                            bool skip = false;
                            if (hasTombstone)
                            {
                                foreach (TimeRange t in tombstones)
                                {
                                    // Should we skip this block because it contains points that have been deleted
                                    if (t.Min <= block.MinTime && t.Max >= block.MaxTime)
                                    {
                                        skip = true;
                                        break;
                                    }
                                }
                            }
                            if (skip)
                            {
                                continue;
                            }
                            //TODO: Validate checksum
                            ClockValues temp = readBlock(block);
                            if (hasTombstone)
                            {
                                foreach (TimeRange t in tombstones)
                                {
                                    temp.Exclude(t.Min, t.Max);
                                }
                            }
                            if (sidvalues == null)
                                sidvalues = temp;
                            else
                                sidvalues.AddRange(temp);
                        }
                    }
                    if (sidvalues != null && sidvalues.Count > 0)
                    {
                        result.Add(sid, sidvalues);
                    }
                }
                return result;
            }
            finally
            {
                Interlocked.Decrement(ref refs);
            }
        }
        public byte[] ReadBytes(IndexEntry e)
        {
            lock (lockthis)
            {
                try
                {
                    Interlocked.Increment(ref refs);
                    if (fs.Length < e.Offset + e.Size)
                    {
                        throw new Exception(Constants.ErrTSMClosed);
                    }
                    fs.Seek(e.Offset + 4, SeekOrigin.Begin);
                    int datalength = (int)e.Size - 4;
                    long dataStart = e.Offset + 4;//4 for checksum
                    byte[] result = new byte[datalength];
                    fs.Read(result, 0, datalength);
                    return result;
                }
                finally
                {
                    Interlocked.Decrement(ref refs);
                }
            }
        }

        public byte Type(ulong sid)
        {
            lock (lockthis)
            {
                return index.Type(sid);
            }
        }

        public bool InUse
        {
            get
            {
                return Interlocked.Read(ref refs) > 0;
            }
        }

        public void Remove()
        {
            lock (lockthis)
            {
                if (!InUse)
                {
                    try
                    {
                        Close();
                        File.Delete(Path);
                    }
                    catch (Exception ex)
                    {
                        Logger.Write(string.Format("TMSReader remove {0} error: {1}", Path, ex.Message));
                    }
                }
                tombstoner.Delete();
            }
        }

        public bool Contains(ulong sid)
        {
            lock (lockthis)
            {
                return index.Contains(sid);
            }
        }

        public bool ContainsValue(ulong sid, long ts)
        {
            lock (lockthis)
            {
                return index.ContainsValue(sid, ts);
            }
        }

        public void DeleteRange(List<ulong> sids, long minTime, long maxTime)
        {
            if (sids == null || sids.Count == 0)
                return;
            ulong minSid = sids[0];
            ulong maxSid = sids[sids.Count - 1];
            lock (lockthis)
            {
                if (!index.OverlapsSidRange(minSid, maxSid))
                    return;
                if (!index.OverlapsTimeRange(minTime, maxTime))
                    return;
                tombstoner.AddRange(sids, minTime, maxTime);
                index.DeleteRange(sids, minTime, maxTime);
            }
        }

        public void Delete(List<ulong> sids)
        {
            lock (lockthis)
            {
                tombstoner.Add(sids);
                index.Delete(sids);
            }
        }

        public long MinTime
        {
            get
            {
                lock (lockthis)
                {
                    return index.MinTime;
                }
            }
        }

        public long MaxTime
        {
            get
            {
                lock (lockthis)
                {
                    return index.MaxTime;
                }
            }
        }

        public ulong MinSid
        {
            get
            {
                lock (lockthis)
                {
                    return index.MinSid;
                }
            }
        }

        public ulong MaxSid
        {
            get
            {
                lock (lockthis)
                {
                    return index.MaxSid;
                }
            }
        }

        public List<IndexEntry> Entries(ulong sid)
        {
            lock (lockthis)
            {
                return index.Entries(sid);
            }
        }

        public uint IndexSize
        {
            get
            {
                lock (lockthis)
                {
                    return index.Size;
                }
            }
        }

        public long Size { get; private set; }

        public long LastModified()
        {
            long result = lastModified;
            lock (lockthis)
            {
                foreach (FileStat ts in tombstoner.TombstoneFiles())
                {
                    if (ts.LastModified > result)
                        result = ts.LastModified;
                }
                return result;
            }
        }

        public bool HasTombstones()
        {
            lock (lockthis)
            {
                return tombstoner.HasTombstones();
            }
        }

        public List<FileStat> TombstoneFiles()
        {
            lock (lockthis)
            {
                return tombstoner.TombstoneFiles();
            }
        }

        public List<TimeRange> TombstoneRange(ulong sid)
        {
            lock (lockthis)
            {
                return index.TombstoneRange(sid);
            }
        }

        public FileStat Stats()
        {
            lock (lockthis)
            {
                return new FileStat()
                {
                    Path = this.Path,
                    Size = this.Size,
                    LastModified = LastModified(),
                    MinTime = MinTime,
                    MaxTime = MaxTime,
                    MinSid = MinSid,
                    MaxSid = MaxSid,
                    HasTombstone = tombstoner.HasTombstones()
                };
            }
        }

        public BlockIterator BlockIterator()
        {
            lock (lockthis)
            {
                return new BlockIterator(this, index.SidCount);
            }
        }

        public int CompareTo(TSMReader other)
        {
            lock (lockthis)
            {
                return Path.CompareTo(other.Path);
            }
        }

        public void Rename(string path)
        {
            lock (lockthis)
            {
                string oldpath = path;
                File.Copy(oldpath, path);
                fs.Close();
                //map.Close();
                Path = path;
                //map = MemoryMappedFile.Create(path, MapProtection.PageReadOnly, fs.Length);
                //fs = map.MapView(MapAccess.FileMapRead, 0, (int)fs.Length);
                fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 2 << 15, FileOptions.SequentialScan);
            }
        }

        public ClockValues ReadBlock(IndexEntry entry)
        {
            lock (lockthis)
            {
                if (fs.Length < entry.Offset + entry.Size)
                {
                    throw new Exception(Constants.ErrTSMClosed);
                }
                return readBlock(entry);
            }
        }

        private ClockValues readBlock(IndexEntry entry)
        {
            int datalength = (int)entry.Size - 4;
            long dataStart = entry.Offset + 4;//4 for checksum
            byte[] bytes = new byte[datalength];
            fs.Seek(dataStart, SeekOrigin.Begin);
            fs.Read(bytes, 0, datalength);
            return Encoding.Decode(bytes, 0);
        }

        private ClockValues readBlock(IndexEntry entry, long start, long end)
        {
            int datalength = (int)entry.Size - 4;
            long dataStart = entry.Offset + 4;//4 for checksum
            byte[] bytes = new byte[datalength];
            fs.Seek(dataStart, SeekOrigin.Begin);
            fs.Read(bytes, 0, datalength);
            return Encoding.Decode(bytes, 0, start, end);
        }

        public void Close()
        {
            lock (lockthis)
            {
                if (!InUse)
                {
                    fs.Close();
                    //map.Close();
                }
            }
        }

        /* TSMIndex uses a raw byte slice representation of an index.  This
        // implementation can be used for indexes that may be MMAPed into memory.
        // indirectIndex works a follows.  Assuming we have an index structure in memory as
        // the diagram below:
        //
        // ┌────────────────────────────────────────────────────────────────────┐
        // │                               Index                                │
        // ├─┬──────────────────────┬──┬───────────────────────┬───┬────────────┘
        // │0│                      │62│                       │145│
        // ├─┴───────┬─────────┬────┼──┴──────┬─────────┬──────┼───┴─────┬──────┐
        // │Key 1 Len│   Key   │... │Key 2 Len│  Key 2  │ ...  │  Key 3  │ ...  │
        // │ 2 bytes │ N bytes │    │ 2 bytes │ N bytes │      │ 2 bytes │      │
        // └─────────┴─────────┴────┴─────────┴─────────┴──────┴─────────┴──────┘

        // We would build an `offsets` slices where each element pointers to the byte location
        // for the first key in the index slice.

        // ┌────────────────────────────────────────────────────────────────────┐
        // │                              Offsets                               │
        // ├────┬────┬────┬─────────────────────────────────────────────────────┘
        // │ 0  │ 62 │145 │
        // └────┴────┴────┘

        // Using this offset slice we can find `Key 2` by doing a binary search
        // over the offsets slice.  Instead of comparing the value in the offsets
        // (e.g. `62`), we use that as an index into the underlying index to
        // retrieve the key at postion `62` and perform our comparisons with that.

        // When we have identified the correct position in the index for a given
        // key, we could perform another binary search or a linear scan.  This
        // should be fast as well since each index entry is 28 bytes and all
        // contiguous in memory.  The current implementation uses a linear scan since the
        // number of block entries is expected to be < 100 per key.*/
        class TSMIndex
        {
            // b is the underlying index byte slice.  This could be a copy on the heap or an MMAP
            // slice reference
            byte[] b;
            // offsets contains the positions in b for each key.  It points to the 2 byte length of
            // key.
            List<int> offsets;

            // tombstones contains only the tombstoned keys with subset of time values deleted.  An
            // entry would exist here if a subset of the points for a key were deleted and the file
            // had not be re-compacted to remove the points on disk.
            Dictionary<ulong, List<TimeRange>> tombstones;
            public TSMIndex()
            {
                tombstones = new Dictionary<ulong, List<TimeRange>>();
                offsets = new List<int>();
            }
            // search returns the index of i in offsets for where sid is located.  If sid is not
            // in the index, len(index) is returned.
            private int search(ulong sid)
            {
                int i = 0, j = offsets.Count;
                while (i < j)
                {
                    int h = i + (j - i) / 2;// avoid overflow when computing h
                    int offset = offsets[h];
                    ulong sid2 = BitConverter.ToUInt64(b, offset);
                    if (sid2 >= sid)
                        j = h;//
                    else
                        i = h + 1;
                }//binary search
                // See if we might have found the right index
                if (i < offsets.Count)
                {
                    int offset = offsets[i];
                    ulong sid2 = BitConverter.ToUInt64(b, offset);
                    if (sid != sid2)
                        return b.Length;
                    return offset;
                }
                // The key is not in the index.  i is the index where it would be inserted so return
                // a value outside our offset range.
                return b.Length;
            }

            // Entries returns all index entries for a key.
            public List<IndexEntry> Entries(ulong sid)
            {
                int ofs = search(sid);
                if (ofs < b.Length)
                {
                    ulong sid2 = BitConverter.ToUInt64(b, ofs);
                    if (sid != sid2)
                        return null;
                    ofs += 8;
                    return readEntries(b, ofs);
                }
                // The key is not in the index.  i is the index where it would be inserted.
                return null;
            }

            private List<IndexEntry> readEntries(byte[] b, int n)
            {
                if (b.Length < 1 + Constants.indexCountSize)
                    return null;
                byte type = b[n];
                n++;//type
                int count = (int)BitConverter.ToUInt16(b, n);
                n += Constants.indexCountSize;
                IndexEntries entries = new IndexEntries(count);
                entries.Type = type;
                for (int i = 0; i < count; i++)
                {
                    IndexEntry ie = new IndexEntry();
                    int start = n;
                    int end = start + Constants.indexEntrySize;
                    if (end <= b.Length)
                    {
                        ie.UnmarshalBinary(b, start);
                    }
                    entries.Entries.Add(ie);
                    n += Constants.indexEntrySize;
                }
                return entries.Entries;
            }

            // Entry returns the index entry for the specified key and timestamp.  If no entry
            // matches the key an timestamp, nil is returned.
            public IndexEntry Entry(ulong sid, long timestamp)
            {
                List<IndexEntry> entries = Entries(sid);
                foreach (IndexEntry entry in entries)
                {
                    if (entry.Contains(timestamp))
                        return entry;
                }
                return null;
            }

            public ulong Sid(int idx, out byte type, out List<IndexEntry> entries)
            {
                type = 0;
                entries = null;
                if (idx < 0 || idx >= offsets.Count)
                    return 0;
                int offset = offsets[idx];
                ulong result = BitConverter.ToUInt64(b, offset);
                type = b[offset + 8];
                entries = readEntries(b, offset + 8);
                return result;
            }

            public ulong SidAt(int idx)
            {
                if (idx < 0 || idx >= offsets.Count)
                    return 0;
                int offset = offsets[idx];
                ulong result = BitConverter.ToUInt64(b, offset);
                return result;
            }

            public int SidCount
            {
                get
                {
                    return offsets.Count;
                }
            }

            // Delete removes the given keys from the index.
            public void Delete(List<ulong> sids)
            {
                if (sids == null || sids.Count == 0)
                    return;
                sids.Sort();
                // Both keys and offsets are sorted.  Walk both in order and skip
                // any keys that exist in both.
                List<int> offsets2 = new List<int>(offsets.Count);
                foreach (int offset in offsets)
                {
                    ulong sid = BitConverter.ToUInt64(b, offset);
                    if (!sids.Contains(sid))
                    {
                        offsets2.Add(offset);
                    }
                }
                offsets = offsets2;
            }
            // DeleteRange removes the given keys with data between minTime and maxTime from the index.
            public void DeleteRange(List<ulong> sids, long minTime, long maxTime)
            {
                if (sids == null || sids.Count == 0)
                    return;
                if (minTime == Constants.MinTime && maxTime == Constants.MaxTime)
                {
                    Delete(sids);
                    return;
                }
                if (minTime > maxTime || maxTime < minTime)
                {
                    return;
                }

                Dictionary<ulong, List<TimeRange>> tombstones = new Dictionary<ulong, List<TimeRange>>();
                foreach (ulong sid in sids)
                {
                    List<IndexEntry> entries = Entries(sid);
                    if (entries == null || entries.Count == 0)
                        continue;
                    long min = entries[0].MinTime;
                    long max = entries[entries.Count - 1].MaxTime;
                    if (minTime > max || maxTime < min)
                        continue;

                    // Is the range passed in cover every value for the key?
                    if (minTime <= min && maxTime >= max)
                    {
                        Delete(sids);
                    }
                    if (!tombstones.ContainsKey(sid))
                        tombstones.Add(sid, new List<TimeRange>());
                    tombstones[sid].Add(new TimeRange() { Min = minTime, Max = maxTime });
                }
                if (tombstones.Count == 0)
                    return;
                foreach (KeyValuePair<ulong, List<TimeRange>> pair in tombstones)
                {
                    if (!tombstones.ContainsKey(pair.Key))
                        tombstones.Add(pair.Key, pair.Value);
                    else
                        tombstones[pair.Key].AddRange(pair.Value);
                }
            }

            // TombstoneRange returns ranges of time that are deleted for the given key.
            public List<TimeRange> TombstoneRange(ulong sid)
            {
                if (tombstones.ContainsKey(sid))
                    return tombstones[sid];
                return null;
            }

            // Contains return true if the given key exists in the index.
            public bool Contains(ulong sid)
            {
                List<IndexEntry> entries = Entries(sid);
                return entries != null && entries.Count > 0;
            }

            // ContainsValue returns true if key and time might exist in this file.
            public bool ContainsValue(ulong sid, long timestamp)
            {
                IndexEntry entry = Entry(sid, timestamp);
                if (entry == null)
                    return false;
                if (tombstones.ContainsKey(sid))
                {
                    foreach (TimeRange t in tombstones[sid])
                    {
                        if (t.Min <= timestamp && t.Max >= timestamp)
                        {
                            return false;
                        }
                    }
                }
                return true;
            }

            // Type returns the block type of the values stored for the key.
            public byte Type(ulong sid)
            {
                int ofs = search(sid);
                if (ofs < b.Length)
                {
                    return b[ofs + 8];
                }
                return 0;
            }

            // OverlapsTimeRange returns true if the time range of the file intersect min and max.
            public bool OverlapsTimeRange(long min, long max)
            {
                return MinTime <= max && MaxTime >= min;
            }

            // OverlapsKeyRange returns true if the min and max keys of the file overlap the arguments min and max.
            public bool OverlapsSidRange(ulong min, ulong max)
            {
                return MinSid <= max && MaxSid >= min;
            }

            public ulong MinSid { get; private set; }
            public ulong MaxSid { get; private set; }

            public long MinTime { get; private set; }
            public long MaxTime { get; private set; }

            // UnmarshalBinary populates an index from an encoded byte slice
            // representation of an index.
            public void UnmarshalBinary(byte[] b, int startIndex, int endIndex)
            {
                this.b = b;
                if (b == null || b.Length - startIndex <= 0)
                    return;
                long minT = Constants.MaxTime, maxT = 0;
                // To create our "indirect" index, we need to find the location of all the keys in
                // the raw byte slice.  The keys are listed once each (in sorted order).  Following
                // each key is a time ordered list of index entry blocks for that key.  The loop below
                // basically skips across the slice keeping track of the counter when we are at a key
                // field.
                int i = startIndex;
                while (i < endIndex)
                {
                    offsets.Add(i);
                    i += 9;//8 sid + 1 type
                    if (i + Constants.indexCountSize > endIndex)
                    {
                        throw new Exception("indirectIndex: not enough data for index entries count");
                    }
                    int count = (int)BitConverter.ToUInt16(b, i);
                    i += Constants.indexCountSize;

                    if (i + 8 > endIndex)
                    {
                        throw new Exception("indirectIndex: not enough data for min time");
                    }
                    long l = BitConverter.ToInt64(b, i);
                    if (l < minT)
                        minT = l;
                    i += (count - 1) * Constants.indexEntrySize;
                    if (i + 16 > endIndex)
                    {
                        throw new Exception("indirectIndex: not enough data for max time");
                    }
                    l = BitConverter.ToInt64(b, i);
                    if (l > maxT)
                        maxT = l;
                    i += Constants.indexEntrySize;
                }
                int firstOfs = offsets[0];
                MinSid = BitConverter.ToUInt64(b, firstOfs);
                int lastOfs = offsets[offsets.Count - 1];
                MaxSid = BitConverter.ToUInt64(b, lastOfs);
                MinTime = minT;
                MaxTime = maxT;
            }
            // Size returns the size of the current index in bytes.
            public uint Size
            {
                get
                {
                    return (uint)b.Length;
                }
            }
        }
    }
}
