namespace Easydata.Engine
{
    public class FileStat
    {
        public string Path { get; set; }
        public bool HasTombstone { get; set; }
        public long Size { get; set; }
        public long LastModified { get; set; }
        public long MinTime { get; set; }
        public long MaxTime { get; set; }
        public ulong MinSid { get; set; }
        public ulong MaxSid { get; set; }

        public bool OverlapsTimeRange(long min, long max)
        {
            return MinTime <= max && MaxTime >= min;
        }

        public bool OverlapsSidRange(ulong min, ulong max)
        {
            return MinSid <= max && MaxSid >= min;
        }

        public bool ContainsSid(ulong sid)
        {
            return sid >= MinSid && sid <= MaxSid;
        }
    }
}
//file_store.go
