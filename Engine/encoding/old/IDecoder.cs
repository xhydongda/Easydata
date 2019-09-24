namespace Easydata.Engine
{
    public interface IDecoder<T> : IDecoder
    {
        // Read returns the next value from the decoder.
        T Read();
        IClockValue Create(long clock, T value, int quality);
    }

    public interface IDecoder
    {
        // SetBytes sets the underlying byte slice of the decoder.
        string SetBytes(byte[] b, int startindex, int len);

        // Next returns true if there are any values remaining to be decoded.
        bool Next();

        // Error returns the current decoding error.
        string Error();
    }
}
