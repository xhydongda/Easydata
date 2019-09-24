using System;
using System.Collections.Generic;

namespace Easydata.Engine
{
    /// <summary>
    /// IndexEntry is the index information for a given block in a TSM file.
    /// </summary>
    public class IndexEntry : IComparable<IndexEntry>
    {
        /// <summary>
        /// The min time of all points stored in the block.
        /// </summary>
        public long MinTime { get; set; }

        /// <summary>
        /// The max time of all points stored in the block.
        /// </summary>
        public long MaxTime { get; set; }

        /// <summary>
        /// The absolute position in the file where this block is located.
        /// </summary>
        public long Offset { get; set; }

        /// <summary>
        /// The size in bytes of the block in the file.
        /// </summary>
        public uint Size { get; set; }

        /// <summary>
        /// UnmarshalBinary decodes an IndexEntry from a byte slice.
        /// </summary>
        /// <param name="b"></param>
        /// <param name="startIndex"></param>
        public void UnmarshalBinary(byte[] b, int startIndex)
        {
            MinTime = BitConverter.ToInt64(b, startIndex);
            MaxTime = BitConverter.ToInt64(b, startIndex + 8);
            Offset = BitConverter.ToInt64(b, startIndex + 16);
            Size = BitConverter.ToUInt32(b, startIndex + 24);
        }

        /// <summary>
        /// AppendTo writes a binary-encoded version of IndexEntry to b.
        /// </summary>
        /// <param name="b"></param>
        public void AppendTo(ByteWriter b)
        {
            b.Write(MinTime);
            b.Write(MaxTime);
            b.Write(Offset);
            b.Write(Size);
        }

        /// <summary>
        /// Contains returns true if this IndexEntry may contain values for the given time.
        /// The min and max times are inclusive.
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        public bool Contains(long t)
        {
            return MinTime <= t && MaxTime >= t;
        }
        /// <summary>
        /// OverlapsTimeRange returns true if the given time ranges are completely within the entry's time bounds.
        /// </summary>
        /// <param name="min"></param>
        /// <param name="max"></param>
        /// <returns></returns>
        public bool OverlapsTimeRange(long min, long max)
        {
            return MinTime <= max && MaxTime >= min;
        }

        /// <summary>
        /// String returns a string representation of the entry.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.Format("min={0:yyyy-MM-dd HH:mm:ss.fff} max={1:yyyy-MM-dd HH:mm:ss.fff} ofs={2} siz={3}",
                new DateTime(MinTime), new DateTime(MaxTime), Offset, Size);
        }

        public int CompareTo(IndexEntry other)
        {
            return MinTime.CompareTo(other.MinTime);
        }
    }

    public class IndexEntries
    {
        public IndexEntries()
        {
            Entries = new List<IndexEntry>();
        }

        public IndexEntries(int sz)
        {
            Entries = new List<IndexEntry>(sz);
        }

        public byte Type { get; set; }
        public List<IndexEntry> Entries { get; }

        public int Len()
        {
            return Entries.Count;
        }

        public long WriteTo(ByteWriter w)
        {
            int total = 0;
            foreach (IndexEntry entry in Entries)
            {
                entry.AppendTo(w);
                total += Constants.indexEntrySize;
            }
            return total;
        }

        public int UnmarshalBinary(byte[] b, int n)
        {
            int result = n;
            if (b.Length - n < 1 + Constants.indexEntrySize)
            {
                throw new Exception("readEntries: data too short for headers");
            }
            Type = b[n];
            result++;
            int count = BitConverter.ToUInt16(b, n);
            result += Constants.indexCountSize;
            Entries.Clear();
            for (int i = 0; i < count; i++)
            {
                int start = i * Constants.indexEntrySize + Constants.indexCountSize + Constants.indexTypeSize;
                IndexEntry ie = new IndexEntry();
                ie.UnmarshalBinary(b, start);
                Entries.Add(ie);
                result += Constants.indexEntrySize;
            }
            return result;
        }
    }
}
