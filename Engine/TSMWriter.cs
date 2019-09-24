using System;
using System.Collections.Generic;
using System.IO;

namespace Easydata.Engine
{
    /*
    A TSM file is composed for four sections: header, blocks, index and the footer.

    ┌────────┬────────────────────────────────────┬─────────────┬──────────────┐
    │ Header │               Blocks               │    Index    │    Footer    │
    │5 bytes │              N bytes               │   N bytes   │   4 bytes    │
    └────────┴────────────────────────────────────┴─────────────┴──────────────┘

    Header is composed of a magic number to identify the file type and a version
    number.

    ┌───────────────────┐
    │      Header       │
    ├─────────┬─────────┤
    │  Magic  │ Version │
    │ 4 bytes │ 1 byte  │
    └─────────┴─────────┘

    Blocks are sequences of pairs of CRC32 and data.  The block data is opaque to the
    file.  The CRC32 is used for block level error detection.  The length of the blocks
    is stored in the index.

    ┌───────────────────────────────────────────────────────────┐
    │                          Blocks                           │
    ├───────────────────┬───────────────────┬───────────────────┤
    │      Block 1      │      Block 2      │      Block N      │
    ├─────────┬─────────┼─────────┬─────────┼─────────┬─────────┤
    │  CRC    │  Data   │  CRC    │  Data   │  CRC    │  Data   │
    │ 4 bytes │ N bytes │ 4 bytes │ N bytes │ 4 bytes │ N bytes │
    └─────────┴─────────┴─────────┴─────────┴─────────┴─────────┘

    Following the blocks is the index for the blocks in the file.  The index is
    composed of a sequence of index entries ordered lexicographically by key and
    then by time.  Each index entry starts with a key length and key followed by a
    count of the number of blocks in the file.  Each block entry is composed of
    the min and max time for the block, the offset into the file where the block
    is located and the the size of the block.

    The index structure can provide efficient access to all blocks as well as the
    ability to determine the cost associated with acessing a given key.  Given a key
    and timestamp, we can determine whether a file contains the block for that
    timestamp as well as where that block resides and how much data to read to
    retrieve the block.  If we know we need to read all or multiple blocks in a
    file, we can use the size to determine how much to read in a given IO.

    ┌────────────────────────────────────────────────────────────────────────────┐
    │                                   Index                                    │
    ├─────────┬─────────┬──────┬───────┬─────────┬─────────┬────────┬────────┬───┤
    │ Key Len │   Key   │ Type │ Count │Min Time │Max Time │ Offset │  Size  │...│
    │ 2 bytes │ N bytes │1 byte│2 bytes│ 8 bytes │ 8 bytes │8 bytes │4 bytes │   │
    └─────────┴─────────┴──────┴───────┴─────────┴─────────┴────────┴────────┴───┘

    The last section is the footer that stores the offset of the start of the index.

    ┌─────────┐
    │ Footer  │
    ├─────────┤
    │Index Ofs│
    │ 8 bytes │
    └─────────┘
    */
    public class TSMWriter
    {
        Stream wrapped;
        TSMIndexInmem index;
        readonly object lockthis = new object();
        long blocksize;
        public TSMWriter(Stream w)
        {
            index = new TSMIndexInmem();
            wrapped = w;
        }

        private void writeHeader(ByteWriter w)
        {
            w.Write(Constants.MagicNumber);
            w.Write(Constants.Version);
            blocksize = 5;
        }

        public void WriteBlock(ulong sid, long minTime, long maxTime, byte[] block)
        {
            lock (lockthis)
            {
                if (block == null || block.Length == 0)
                    return;
                byte blockType = block[0];
                ByteWriter writer;
                if (blocksize == 0)
                {
                    writer = new ByteWriter(4 + 1 + 4 + block.Length);//magicnumer 4 + version 1 + checksum 4 + block
                    writeHeader(writer);
                }
                else
                {
                    writer = new ByteWriter(4 + block.Length);//checksum 4 + block
                }
                uint checksum = Crc32.Compute(block);
                writer.Write(checksum);
                int nn = 4;//checksum
                writer.Write(block);
                wrapped.Write(writer.EndWrite(), 0, writer.Length);
                writer.Release();
                nn += block.Length;
                index.Add(sid, blockType, minTime, maxTime, blocksize, (uint)nn);
                blocksize += nn;
                if (index.Entries(sid).Count >= Constants.maxIndexEntries)
                {
                    throw new Exception(Constants.ErrMaxBlocksExceeded);
                }
            }
        }

        public void WriteIndex()
        {
            lock (lockthis)
            {
                if (index.SidCount == 0)
                {
                    throw new Exception(Constants.ErrNoValues);
                }
                int size = 8 + (int)index.Size;//8 for n
                ByteWriter writer = new ByteWriter(size);
                index.WriteTo(writer);
                writer.Write(blocksize);
                wrapped.Write(writer.EndWrite(), 0, writer.Length);
                writer.Release();
            }
        }

        public void Close()
        {
            lock (lockthis)
            {
                wrapped.Flush();
                wrapped.Close();
                wrapped = null;
            }
        }

        public uint Size
        {
            get
            {
                lock (lockthis)
                {
                    return (uint)blocksize + index.Size;
                }
            }
        }

        /// <summary>
        /// directIndex is a simple in-memory index implementation for a TSM file.  The full index
        /// must fit in memory.
        /// </summary>
        class TSMIndexInmem
        {
            Dictionary<ulong, IndexEntries> blocks;
            public TSMIndexInmem()
            {
                blocks = new Dictionary<ulong, IndexEntries>();
            }

            public void Add(ulong sid, byte blockType, long minTime, long maxTime, long offset, uint size)
            {
                if (!blocks.ContainsKey(sid))
                {
                    blocks.Add(sid, new IndexEntries() { Type = blockType });
                    // size of the sid stored in the index
                    this.Size += 8;//sid
                    this.Size += 1;// Type ?
                    this.Size += Constants.indexCountSize;
                }
                blocks[sid].Entries.Add(new IndexEntry()
                {
                    MinTime = minTime,
                    MaxTime = maxTime,
                    Offset = offset,
                    Size = size
                });
                // size of the encoded index entry
                this.Size += Constants.indexEntrySize;
            }

            public List<IndexEntry> Entries(ulong sid)
            {
                if (blocks.ContainsKey(sid))
                    return blocks[sid].Entries;
                return null;
            }

            public int SidCount
            {
                get
                {
                    return blocks.Count;
                }
            }

            private void addEntries(ulong sid, IndexEntries entries)
            {
                if (!blocks.ContainsKey(sid))
                    blocks.Add(sid, entries);
                else
                    blocks[sid].Entries.AddRange(entries.Entries);
            }

            public long WriteTo(ByteWriter w)
            {
                long N = 0;
                List<ulong> sids = new List<ulong>(blocks.Keys);
                sids.Sort();
                foreach (ulong sid in sids)
                {
                    IndexEntries entries = blocks[sid];
                    if (entries.Len() > Constants.maxIndexEntries)
                    {
                        throw new ArgumentOutOfRangeException(
                            string.Format("sid {0} exceeds max index entries: {1} > {2}",
                            sid, entries.Len(), Constants.maxIndexEntries));
                    }
                    entries.Entries.Sort();
                    w.Write(sid);
                    N += 8;
                    w.Write(entries.Type);
                    N += 1;
                    w.Write((ushort)entries.Len());
                    N += 2;
                    N += entries.WriteTo(w);
                }
                return N;
            }

            public uint Size { get; private set; }
        }
    }
}
