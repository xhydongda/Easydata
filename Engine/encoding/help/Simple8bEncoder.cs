using System;
using System.Buffers;

namespace Easydata.Engine
{
    /// <summary>
    /// <seealso cref="https://github.com/jwilder/encoding/blob/master/simple8b/encoding.go"/>
    /// Package simple8b implements the 64bit integer encoding algoritm as published
    /// by Ann and Moffat in "Index compression using 64-bit words", Softw. Pract. Exper. 2010; 40:131–147
    ///
    /// It is capable of encoding multiple integers with values betweeen 0 and to 1^60 -1, in a single word.
    /// Simple8b is 64bit word-sized encoder that packs multiple integers into a single word using
    /// a 4 bit selector values and up to 60 bits for the remaining values.  Integers are encoded using
    /// the following table:
    ///
    /// ┌──────────────┬─────────────────────────────────────────────────────────────┐
    /// │   Selector   │       0    1   2   3   4   5   6   7  8  9  0 11 12 13 14 15│
    /// ├──────────────┼─────────────────────────────────────────────────────────────┤
    /// │     Bits     │       0    0   1   2   3   4   5   6  7  8 10 12 15 20 30 60│
    /// ├──────────────┼─────────────────────────────────────────────────────────────┤
    /// │      N       │     240  120  60  30  20  15  12  10  8  7  6  5  4  3  2  1│
    /// ├──────────────┼─────────────────────────────────────────────────────────────┤
    /// │   Wasted Bits│      60   60   0   0   0   0  12   0  4  4  0  0  0  0  0  0│
    /// └──────────────┴─────────────────────────────────────────────────────────────┘
    ///
    /// For example, when the number of values can be encoded using 4 bits, selected 5 is encoded in the
    /// 4 most significant bits followed by 15 values encoded used 4 bits each in the remaing 60 bits.
    /// </summary>
    public class Simple8bEncoder
    {
        public const ulong MaxValue = ((ulong)1 << 60) - 1;
        const int buflen = 240;
        // most recently written integers that have not been flushed
        ulong[] buf;
        // current bytes written and flushed
        byte[] bytes;
        int len = 0;
        // index in buf of the head of the buf
        int head;
        // index in buf of the tail of the buf
        int tail;
        // NewEncoder returns an Encoder able to convert uint64s to compressed byte slices

        public Simple8bEncoder()
        {
            buf = new ulong[buflen];
            bytes = new byte[Constants.DefaultMaxPointsPerBlock * 8];
        }

        public void SetValues(ulong[] v)
        {
            buf = v;
            tail = v.Length;
            head = 0;
            len = 0;
        }

        public void Reset()
        {
            tail = 0;
            head = 0;
            len = 0;
        }

        public string Write(ulong v)
        {
            if (tail >= buflen)
            {
                string err = flush();
                if (err != null)
                    return err;
            }

            // The buf is full but there is space at the front, just shift
            // the values down for now. TODO: use ring buffer
            if (tail >= buflen)
            {
                Span<ulong> buf_span = buf.AsSpan();
                Span<ulong> buf_slice = buf_span.Slice(head);
                int i = 0, moved = buflen - head;
                while (i < moved)
                {
                    buf_span[i] = buf_slice[i];
                    i++;
                }
                tail -= head;
                head = 0;
            }
            buf[tail] = v;
            tail += 1;
            return null;
        }

        private string flush()
        {
            if (tail == 0)
            {
                return null;
            }

            // encode as many values into one as we can
            int n;
            var encoded = Encode(buf, head, tail, out n);
            if (encoded.error != null)
                return encoded.error;
            len = ByteWriter.Write(bytes, len, encoded.Item1);
            //Move the head forward since we encoded those values
            head += n;
            // If we encoded them all, reset the head/tail pointers to the beginning
            if (head == tail)
            {
                head = 0;
                tail = 0;
            }
            return null;
        }

        public (byte[], int len, string error) Bytes()
        {
            string error = null;
            while (tail > 0)
            {
                error = flush();
            }
            return (bytes, len, error);
        }

        // Encode packs as many values into a single uint64.  It returns the packed
        // uint64, how many values from src were packed, or an error if(the values exceed
        // the maximum value range.
        public (ulong, string error) Encode(ulong[] src, int startIndex, int endIndex, out int n)
        {
            ulong v;
            (n, v) = Pack(src, startIndex, endIndex);
            if (n > 0)
            {
                return (v, null);
            }
            else
            {
                if (src.Length - startIndex > 0)
                {
                    return (0, "value out of bounds");
                }
                return (0, null);
            }
        }

        const int S8B_BIT_SIZE = 60;
        // EncodeAll returns a packed slice of the values from src.  if(a value is over
        // 1 << 60, an error is returned. .
        public static string EncodeAll(ulong[] src, int index, int end, ByteWriter dst)
        {
            int i = 0;
            ReadOnlySpan<ulong> src_span = new ReadOnlySpan<ulong>(src, index, end-index);
            while(i< src_span.Length)
            {
                bool continu_nextvalue = false;
                ReadOnlySpan<ulong> remaining = src_span.Slice(i);
                if (remaining.Length > 120)
                {
                    // Invariant: len(a) is fixed to 120 or 240 values
                    ReadOnlySpan<ulong> a;
                    if (remaining.Length >= 240)
                    {
                        a = remaining.Slice(0, 240);
                    }
                    else
                    {
                        a = remaining.Slice(0, 120);
                    }
                    // search for the longest sequence of 1s in a
                    // Postcondition: k equals the index of the last 1 or -1
                    int k = 0;
                    for (; k < a.Length; k++)
                    {
                        if (a[k] != 1)
                        {
                            k--;
                            break;
                        }
                    }
                    ulong v = 0;
                    if (k == 239)
                    {
                        i += 240;
                    }// 240 1s
                    else if (k >= 119)
                    {
                        v = (ulong)1 << 60;
                        i += 120;
                    }// at least 120 1s
                    else
                    {
                        goto CODES;
                    }
                    dst.Write(v);
                    continue;
                }
            CODES:
                for (int s = 0; s < LEN; s++)
                {
                    int n = arr_n2[s];
                    byte bits = arr_bits[s];
                    if (n > remaining.Length)
                    {
                        continue;
                    }
                    ulong maxVal = (ulong)1 << (bits & 0x3F);
                    ulong val = (ulong)(s + 2) << S8B_BIT_SIZE;
                    bool continue_codes = false;
                    for (int k = 0; k < remaining.Length; k++)
                    {
                        ulong inV = remaining[k];
                        if (k < n)
                        {
                            if (inV >= maxVal)
                            {
                                continue_codes = true;
                                break;//continue CODES
                            }
                            val |= inV << (((byte)k * bits) & 0x3f);
                        }
                        else
                        {
                            break;
                        }
                    }
                    if (!continue_codes)
                    {
                        dst.Write(val);
                        i += n;
                        continu_nextvalue = true;
                        break;
                    }
                }
                if (!continu_nextvalue)
                {
                    return "value out of bounds";
                }
            }
            return null;
        }

        #region packing
        readonly static int[] arr_n;
        readonly static int[] arr_n2;
        readonly static ulong[] arr_max;
        readonly static byte[] arr_bits;
        const int LEN = 14;
        const ulong UL_60 = (ulong)1 << 60;
        static Simple8bEncoder()
        {
            arr_n = new int[]
            {
                    240,120,60,30,20,15,12,10,8,7,6,5,4,3,2,1
            };
            arr_n2 = new int[]
            {
                    60,30,20,15,12,10,8,7,6,5,4,3,2,1
            };
            arr_bits = new byte[LEN];//=arr_n2 reverse.
            arr_max = new ulong[LEN];
            for (int i = LEN - 1; i >= 0; i--)
            {
                arr_bits[i] = (byte)arr_n2[LEN - 1 - i];
                arr_max[i] = ((ulong)1 << arr_bits[i]) - 1;
            };
        }

        public static (int n, ulong v) Pack(ulong[] src, int startIndex, int endIndex)
        {
            ReadOnlySpan<ulong> src_span = new ReadOnlySpan<ulong>(src);
            bool all_1 = true;
            ulong[] maxs = ArrayPool<ulong>.Shared.Rent(LEN);
            Span<ulong> maxs_span = maxs.AsSpan();
            ulong max = 0;
            int j = 1;
            int end = endIndex;
            if (end > startIndex + 240) end = startIndex + 240;
            int nindex = LEN - 1;
            bool nindex_neg = false;
            int n2 = arr_n2[nindex];
            ReadOnlySpan<int> arr_n2_span = new ReadOnlySpan<int>(arr_n2);
            for (int i = startIndex; i < end; i++)
            {
                ulong ul = src_span[i];
                if (all_1) { all_1 = (ul == 1); }
                //all_1 &= (ul == 1);
                if (ul > max)
                {
                    max = ul;
                }
                if (!nindex_neg && j == n2)
                {
                    maxs_span[nindex--] = max;
                    if (nindex < 0) nindex_neg = true;
                    else n2 = arr_n2_span[nindex];
                }
                j++;
            }
            int range = end - startIndex;
            ReadOnlySpan<int> arr_n_span = new ReadOnlySpan<int>(arr_n);
            ReadOnlySpan<ulong> arr_max_span = new ReadOnlySpan<ulong>(arr_max);
            for (int i = 0; i < 16; i++)
            {
                if (range < arr_n_span[i])
                    continue;
                if (i < 2)
                {
                    if (all_1)
                    {
                        if (i == 0) return (240, 0);
                        else return (120, UL_60);//i==1
                    }
                }//pack0,pack1 are special and use 0 bits to encode runs of 1's
                else
                {
                    ulong m = maxs_span[i - 2];
                    if (m <= arr_max_span[i - 2])
                    {
                        return (arr_n_span[i], Packing.All[i].pack(startIndex, src));
                    }
                }
            }
            ArrayPool<ulong>.Shared.Return(maxs);
            return (0, 0);
        }
        #endregion
    }
}
