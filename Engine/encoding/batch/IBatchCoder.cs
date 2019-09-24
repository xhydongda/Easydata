using System;

namespace Easydata.Engine
{
    public interface IBatchCoder<T> : IBatchCoder
    {
        (int, string) DecodeAll(Span<byte> src, Span<T> to);
        (ByteWriter, string) EncodingAll(Span<T> src);
    }

    public interface IBatchCoder
    {

    }
}
