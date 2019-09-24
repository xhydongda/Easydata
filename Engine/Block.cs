using System;
using System.Collections.Generic;

namespace Easydata.Engine
{
    public class Block : IComparable<Block>
    {
        public Block()
        {
            ReadMin = Constants.MaxTime;
            ReadMax = Constants.MinTime;
        }

        public ulong Sid { get; set; }
        public long MinTime { get; set; }
        public long MaxTime { get; set; }
        public byte Typ { get; set; }
        public byte[] Buf { get; set; }

        public List<TimeRange> TombStones { get; set; }
        // readMin, readMax are the timestamps range of values have been
        // read and encoded from this block.
        public long ReadMin { get; set; }
        public long ReadMax { get; set; }

        public bool OverlapsTimeRange(long min, long max)
        {
            return MinTime <= max && MaxTime >= min;
        }

        /// <summary>
        /// is read finished.
        /// </summary>
        /// <returns></returns>
        public bool ReadDone()
        {
            return ReadMin <= MinTime && ReadMax >= MaxTime;
        }

        public void MarkRead(long min, long max)
        {
            if (min < ReadMin)
                ReadMin = min;
            if (max > ReadMax)
                ReadMax = max;
        }

        public bool PartiallyRead()
        {
            return ReadMin != MinTime || ReadMax != MaxTime;
        }

        public int CompareTo(Block other)
        {
            if (Sid == other.Sid)
                return MinTime.CompareTo(other.MinTime);
            return Sid.CompareTo(other.Sid);
        }
    }
}