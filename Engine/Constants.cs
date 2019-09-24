namespace Easydata.Engine
{
    public sealed class Constants
    {
        // uvnan is the constant returned from double.NaN.
        public const ulong uvnan = 0xfff8000000000000;
        // MagicNumber is written as the first 4 bytes of a data file to
        // identify the file as a tsm1 formatted file
        public const uint MagicNumber = 0x16D116D1;
        // Version indicates the version of the TSM file format.
        public const byte Version = 1;
        //Size in bytes of an index entry
        public const int indexEntrySize = 28;
        // Size in bytes used to store the count of index entries for a sid
        public const int indexCountSize = 2;
        // Size in bytes used to store the type of block encode
        public const int indexTypeSize = 1;
        // Max number of blocks for a given sid that can exist in a single file
        public const int maxIndexEntries = (1 << (indexCountSize * 8)) - 1;

        //ErrNoValues is returned when TSMWriter.WriteIndex is called and there are no values to write.
        public const string ErrNoValues = "no values written";
        // ErrTSMClosed is returned when performing an operation against a closed TSM file.
        public const string ErrTSMClosed = "tsm file closed";
        // ErrMaxBlocksExceeded is returned when attempting to write a block past the allowed number.
        public const string ErrMaxBlocksExceeded = "max blocks exceeded";

        //DateTime.MinTime.Ticks
        public const long MinTime = 0;
        //DateTime.MaxTime.Ticks
        public const long MaxTime = 3155378975999999999;
        // ringShards specifies the number of partitions that the hash ring used to
        // store the entry mappings contains. It must be a power of 2. From empirical
        // testing, a value above the number of cores on the machine does not provide
        // any additional benefit. For now we'll set it to the number of cores on the
        // largest box we could imagine running influx.
        public const int RingShards = 4096;

        // ErrSnapshotInProgress is returned if a snapshot is attempted while one is already running.
        public const string ErrSnapshotInProgress = "snapshot in progress";
        // ErrCacheMemorySizeLimitExceeded returns an error indicating an operation
        // could not be completed due to exceeding the cache-max-memory-size setting.
        public const string ErrCacheMemorySizeLimitExceeded = "cache-max-memory-size exceeded: ({0}/{1})";

        // WriteWALEntryType indicates a write entry.
        public const byte WriteWALEntryType = 0x01;
        // DeleteWALEntryType indicates a delete entry.
        public const byte DeleteWALEntryType = 0x02;
        // DeleteRangeWALEntryType indicates a delete range entry.
        public const byte DeleteRangeWALEntryType = 0x03;

        // ErrWALCorrupt is returned when reading a corrupt WAL entry.
        public const string ErrWALCorrupt = "corrupted WAL entry";

        // DefaultSegmentSize of 10MB is the size at which segment files will be rolled over.
        public const int DefaultSegmentSize = 10 * 1024 * 1024;
        // WALFileExtension is the file extension we expect for wal segments.
        public const string WALFileExtension = "wal";
        // WALFilePrefix is the prefix on all wal segment files.
        public const string WALFilePrefix = "_";

        public const uint maxTSMFileSize = 2147483648;// 2GB
        // CompactionTempExtension is the extension used for temporary files created during compaction.
        public const string CompactionTempExtension = "tmp";
        // TSMFileExtension is the extension used for TSM files.
        public const string TSMFileExtension = "tsm";

        public const string errMaxFileExceeded = "max file exceeded";
        public const string errSnapshotsDisabled = "snapshots disabled";
        public const string errCompactionsDisabled = "compactions disabled";
        public const string errCompactionAborted = "compaction aborted";
        public const string errCompactionInProgress = "compaction in progress";

        // DefaultMaxPointsPerBlock is the maximum number of points in an encoded
        // block in a TSM file
        public const int DefaultMaxPointsPerBlock = 1000;

        public const int DefaultCacheMaxMemorySize = 1024 * 1024 * 1024;//1GB
        public const long DefaultCompactFullWriteColdDuration = 144000000000;//4 hours
        public const int DefaultCacheSnapshotMemorySize = 25 * 1024 * 1024; // 25MB
        public const long DefaultCacheSnapshotWriteColdDuration = 6000000000;//10 mins
        public const int FOREVER = 366000;//Days
    }
}
