namespace Easydata.Engine
{
    public interface IEncoder<T> : IEncoder
    {
        // Write encodes v to the underlying buffers.
        void Write(T v);
    }

    public interface IEncoder
    {
        void Flush();
        // Reset sets the encoder back to its initial state.
        void Reset();
        // Bytes returns a copy of the underlying buffer.
        (ByteWriter, string) Bytes();
    }
}
