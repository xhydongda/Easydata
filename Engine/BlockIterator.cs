using System.Collections.Generic;
namespace Easydata.Engine
{
    // BlockIterator allows iterating over each block in a TSM file in order.  It provides
    // raw access to the block bytes without decoding them.
    public class BlockIterator
    {
        private int i;
        private readonly int n;
        private ulong sid;
        private List<IndexEntry> entries;
        private int entryCount = 0;
        private int entryIndex = 0;
        private byte typ;
        private TSMReader r;
        public BlockIterator(TSMReader r, int n)
        {
            this.r = r;
            this.n = n;
        }

        public List<TimeRange> TombstoneRange(ulong sid)
        {
            return r.TombstoneRange(sid);
        }

        public Block Read()
        {
            IndexEntry e = entries[entryIndex];
            byte[] buf = r.ReadBytes(e);
            return new Block()
            {
                Sid = sid,
                MinTime = e.MinTime,
                MaxTime = e.MaxTime,
                Typ = typ,
                Buf = buf
            };
        }

        // PeekNext returns the next sid to be iterated or zero.
        public ulong PeekNext()
        {
            if (entryIndex < entryCount - 1)
                return sid;
            if (i < n - 1)
            {
                return r.SidAt(i + 1);
            }
            return 0;
        }

        public bool Next()
        {
            if (i == n && (entryIndex >= entryCount - 1))
                return false;
            if (entryIndex < entryCount - 1)
            {
                entryIndex++;
                return true;
            }
            if (i < n)
            {
                sid = r.Sid(i, out typ, out entries);
                entryIndex = 0;
                entryCount = (entries == null ? 0 : entries.Count);
                i++;
                if (entryCount > 0)
                    return true;
            }
            return false;
        }
    }
}
