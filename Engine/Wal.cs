using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace Easydata.Engine
{
    /// <summary>
    /// WAL represents the write-ahead log used for writing TSM files.
    /// </summary>
    public class Wal : IStatable
    {
        readonly object lockthis = new object();
        /// <summary>
        ///  SegmentSize is the file size at which a segment file will be rotated.
        /// </summary>
        readonly int SegmentSize;
        WalSegmentWriter currentSegmentWriter;
        public Wal(string path)
        {
            _LastWriteTime = Constants.MinTime;
            Path = path;
            SegmentSize = Constants.DefaultSegmentSize;
        }

        #region IStatable
        struct WalStatistics
        {
            public long OldBytes;
            public long CurrentBytes;
            public long WriteOK;
            public long WriteErr;
        }

        WalStatistics stats;

        public string Name => "tsm1_wal";

        public Dictionary<string, long> Stat()
        {
            Dictionary<string, long> values = new Dictionary<string, long>
            {
                ["oldSegmentsDiskBytes"] = Interlocked.Read(ref stats.OldBytes),
                ["currentSegmentDiskBytes"] = Interlocked.Read(ref stats.CurrentBytes),
                ["writeOk"] = Interlocked.Read(ref stats.WriteOK),
                ["writeErr"] = Interlocked.Read(ref stats.WriteErr)
            };
            return values;
        }
        #endregion

        //仅在构造函数中初始化.        
        public string Path { get; private set; }

        long traceLogging = 1;
        public bool EnableTraceLogging
        {
            get
            {
                return Interlocked.Read(ref traceLogging) == 1;
            }
            set
            {
                if (value) Interlocked.Exchange(ref traceLogging, 1);
                else Interlocked.Exchange(ref traceLogging, 0);
            }
        }

        long _LastWriteTime;
        public long LastWriteTime
        {
            get
            {
                return Interlocked.Read(ref _LastWriteTime);
            }
            set
            {
                Interlocked.Exchange(ref _LastWriteTime, value);
            }
        }
        long _currentSegmentID;
        private long currentSegmentID
        {
            get
            {
                return Interlocked.Read(ref _currentSegmentID);
            }
            set
            {
                Interlocked.Exchange(ref _currentSegmentID, value);
            }
        }

        // Open opens and initializes the Log. Open can recover from previous unclosed shutdowns.
        public void Open()
        {
            if (EnableTraceLogging)
            {
                Logger.Info(string.Format("tsm1 WAL starting with {0} segment size", SegmentSize));
                Logger.Info(string.Format("tsm1 WAL writing to {0}", Path));
            }
            lock (lockthis)
            {
                if (!Directory.Exists(Path))
                    Directory.CreateDirectory(Path);
                List<string> segments = SegmentFileNames(Path);
                if (segments != null && segments.Count > 0)
                {
                    string lastSegment = segments[segments.Count - 1];
                    int id = idFromFileName(lastSegment, out string err);
                    if (err != null)
                        return;
                    currentSegmentID = id;
                    FileInfo stat = new FileInfo(lastSegment);
                    if (stat.Length == 0)
                    {
                        try
                        {
                            File.Delete(lastSegment);
                            segments.RemoveAt(segments.Count - 1);
                        }
                        catch (Exception ex)
                        {
                            Logger.Error("Wal Open Error: " + ex.Message);
                        }//删除正在使用的wal会抛出异常
                    }
                    else
                    {
                        FileStream fd = stat.Open(FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
                        currentSegmentWriter = new WalSegmentWriter(fd);
                        Interlocked.Add(ref stats.CurrentBytes, stat.Length);
                    }
                    long totalOldDiskSize = 0;
                    foreach (string seg in segments)
                    {
                        stat = new FileInfo(seg);
                        totalOldDiskSize += stat.Length;
                        if (stat.LastWriteTime.Ticks > LastWriteTime)
                            LastWriteTime = stat.LastWriteTime.Ticks;
                    }
                    Interlocked.Add(ref stats.OldBytes, totalOldDiskSize);
                }
            }
        }

        public static List<string> SegmentFileNames(string dir)
        {
            string[] names = Directory.GetFiles(dir, string.Format("{0}*.{1}", Constants.WALFilePrefix, Constants.WALFileExtension));
            if (names != null && names.Length > 0)
            {
                List<string> result = names.ToList();
                result.Sort();
                return result;
            }
            return null;
        }


        // idFromFileName parses the segment file ID from its name.
        private int idFromFileName(string name, out string error)
        {
            error = null;
            string[] parts = System.IO.Path.GetFileName(name).Split('.');
            if (parts.Length != 2)
            {
                error = string.Format("file {0} has wrong name format to have an id", name);
                return 0;
            }
            return Convert.ToInt32(parts[0].Remove(0, 1));//_00001.wal
        }

        // newSegmentFile will close the current segment file and open a new one, updating bookkeeping info on the log.
        private void newSegmentFile()
        {
            currentSegmentID++;
            if (currentSegmentWriter != null)
            {
                currentSegmentWriter.Flush();
                currentSegmentWriter.Close();
                Interlocked.Add(ref stats.OldBytes, currentSegmentWriter.Size);
            }

            string fileName = string.Format("{0}{1}{2:D5}.{3}", Path, Constants.WALFilePrefix, currentSegmentID, Constants.WALFileExtension);
            FileStream fd = File.Create(fileName);
            currentSegmentWriter = new WalSegmentWriter(fd);

            LastWriteTime = DateTime.Now.Ticks;

            // Reset the current segment size stat
            Interlocked.Add(ref stats.CurrentBytes, 0);
        }

        public string Write(ulong id, ClockValues cvs)
        {
            Dictionary<ulong, ClockValues> dic = new Dictionary<ulong, ClockValues>
            {
                [id] = cvs
            };
            return WriteMulti(dic);
        }

        public string WriteMulti(Dictionary<ulong, ClockValues> values)
        {
            WriteWalEntry entry = new WriteWalEntry()
            {
                Values = values
            };
            var err = writeToLog(entry);
            if (err != null)
            {
                Interlocked.Increment(ref stats.WriteErr);
            }
            else
            {
                Interlocked.Increment(ref stats.WriteOK);
            }
            return err;
        }

        private string writeToLog(IWalEntry entry)
        {
            var bytes = entry.Marshal(out string err);
            byte[] compressed;
            if (err == null)
            {
                compressed = SnappyPI.SnappyCodec.Compress(bytes.EndWrite(), 0, bytes.Length);
                bytes.Release();
            }
            else
            {
                bytes.Release();
                return err;
            }
            lock (lockthis)
            {
                rollSegment();
                currentSegmentWriter.Write(entry.Type(), compressed);
                currentSegmentWriter.Flush();
                Interlocked.Add(ref stats.CurrentBytes, currentSegmentWriter.Size);
                LastWriteTime = DateTime.Now.Ticks;
                return err;
            }
        }

        // rollSegment checks if the current segment is due to roll over to a new segment;
        // and if so, opens a new segment file for future writes.
        private void rollSegment()
        {
            if (currentSegmentWriter == null || currentSegmentWriter.Size > Constants.DefaultSegmentSize)
            {
                newSegmentFile();
            }
        }

        // CloseSegment closes the current segment if it is non-empty.
        public void CloseSegment()
        {
            lock (lockthis)
            {
                if (currentSegmentWriter != null)
                {
                    currentSegmentWriter.Flush();
                    currentSegmentWriter.Close();
                    Interlocked.Add(ref stats.OldBytes, currentSegmentWriter.Size);
                    currentSegmentWriter = null;
                }
            }
        }

        public List<string> ClosedSegments()
        {
            lock (lockthis)
            {
                if (Path == string.Empty)
                    return null;
                string currentFile = null;
                if (currentSegmentWriter != null)
                    currentFile = currentSegmentWriter.Path;
                List<string> files = SegmentFileNames(Path);
                List<string> closedFiles = new List<string>();
                foreach (string fn in files)
                {
                    if (fn == currentFile)
                        continue;
                    else
                        closedFiles.Add(fn);
                }
                return closedFiles;
            }
        }

        // Remove deletes the given segment file paths from disk and cleans up any associated objects.
        public void Remove(List<string> files)
        {
            if (files == null || files.Count == 0)
                return;
            lock (lockthis)
            {
                foreach (string fn in files)
                {
                    if (EnableTraceLogging) Logger.Info(string.Format("Removing {0}", fn));
                    File.Delete(fn);
                }

                // Refresh the on-disk size stats
                List<string> segments = SegmentFileNames(Path);
                if (segments != null && segments.Count > 0)
                {
                    long totalOldDiskSize = 0;
                    foreach (string seg in segments)
                    {
                        FileInfo stat = new FileInfo(seg);
                        totalOldDiskSize += stat.Length;
                    }
                    Interlocked.Add(ref stats.OldBytes, totalOldDiskSize);
                }
            }
        }

        public long DistSizeBytes()
        {
            return Interlocked.Read(ref stats.OldBytes) + Interlocked.Read(ref stats.CurrentBytes);
        }

        // Delete deletes the given keys, returning the segment ID for the operation.
        public string Delete(List<ulong> sids)
        {
            if (sids == null || sids.Count == 0)
                return null;
            DeleteWalEntry entry = new DeleteWalEntry()
            {
                Sids = sids
            };
            return writeToLog(entry);
        }

        // DeleteRange deletes the given keys within the given time range,
        // returning the segment ID for the operation.
        public string DeleteRange(List<ulong> sids, long min, long max)
        {
            if (sids == null || sids.Count == 0)
                return null;
            DeleteRangeWalEntry entry = new DeleteRangeWalEntry()
            {
                Sids = sids,
                Min = min,
                Max = max
            };
            return writeToLog(entry);
        }

        public void Close()
        {
            lock (lockthis)
            {
                if (EnableTraceLogging) Logger.Info(string.Format("Closing {0}", Path));
                if (currentSegmentWriter != null)
                {
                    currentSegmentWriter.Flush();
                    currentSegmentWriter.Close();
                    currentSegmentWriter = null;
                }
            }
        }
    }

    public class WalSegmentWriter
    {
        FileStream stream;
        public WalSegmentWriter(FileStream w)
        {
            stream = w;
        }

        public string Path
        {
            get
            {
                if (stream != null)
                    return stream.Name;
                return string.Empty;
            }
        }

        public int Size { get; set; }

        public void Write(byte entryType, byte[] compressed)
        {
            stream.WriteByte(entryType);
            stream.Write(BitConverter.GetBytes(compressed.Length), 0, 4);
            stream.Write(compressed, 0, compressed.Length);
            Size = 5 + compressed.Length;
        }

        public void Flush()
        {
            stream.Flush();
        }

        public void Close()
        {
            stream.Close();
        }
    }

    public class WalSegmentReader
    {
        FileStream stream;
        IWalEntry entry;
        long n;
        string err;
        public WalSegmentReader(FileStream r)
        {
            stream = r;
        }

        public bool Next()
        {
            int nReadOK = 0;
            // read the type and the length of the entry
            byte[] lv = new byte[5];
            try
            {
                nReadOK += stream.Read(lv, 0, 5);
                if (nReadOK == 0)
                    return false;
            }
            catch (EndOfStreamException ex)
            {
                err = ex.Message;
                return false;
            }
            catch (Exception ex)
            {
                err = ex.Message;
                // We return true here because we want the client code to call read which
                // will return the this error to be handled.
                return true;
            }
            byte entryType = lv[0];
            int length = BitConverter.ToInt32(lv, 1);
            byte[] b = ArrayPool<byte>.Shared.Rent(length); ;//? b := *(getBuf(int(length)))
            try
            {
                nReadOK += stream.Read(b, 0, length);
            }
            catch (Exception ex)
            {
                err = ex.Message;
                return true;
            }
            IWalEntry newentry = null;
            switch (entryType)
            {
                case Constants.WriteWALEntryType:
                    newentry = new WriteWalEntry();
                    break;
                case Constants.DeleteWALEntryType:
                    newentry = new DeleteWalEntry();
                    break;
                case Constants.DeleteRangeWALEntryType:
                    newentry = new DeleteRangeWalEntry();
                    break;
                default:
                    err = string.Format("unknown wal entry type: {0}", entryType);
                    return true;
            }
            byte[] data = SnappyPI.SnappyCodec.Uncompress(b, 0, length);
            ArrayPool<byte>.Shared.Return(b);
            err = newentry.UnmarshalBinary(data, 0, data.Length);
            if (err == null)
            {
                // Read and decode of this entry was successful.
                n += nReadOK;
            }
            entry = newentry;
            return true;
        }

        public IWalEntry Read(out string err)
        {
            err = this.err;
            if (err != null)
                return null;
            return entry;
        }

        // Count returns the total number of bytes read successfully from the segment, as
        // of the last call to Read(). The segment is guaranteed to be valid up to and
        // including this number of bytes.
        public long Count()
        {
            return n;
        }

        public string Error()
        {
            return err;
        }

        public void Close()
        {
            stream.Close();
        }
    }
}
