using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;

namespace Easydata.Engine
{
    /// <summary>
    /// 一组时序数据，按照时间顺序自动排序，且不重复.
    /// </summary>
    [Serializable]
    public class ClockValues : IList<IClockValue>
    {
        List<IClockValue> list;

        public ClockValues()
        {
            list = new List<IClockValue>();
        }

        public ClockValues(int capacity)
        {
            list = new List<IClockValue>(capacity);
        }

        public ClockValues(ClockValues src)
        {
            list = new List<IClockValue>(src.list);
        }

        #region ILIST

        public int IndexOf(IClockValue item)
        {
            return list.IndexOf(item);
        }

        public void Insert(int index, IClockValue item)
        {
            Add(item);
        }

        public void RemoveAt(int index)
        {
            if (index >= 0 && index < list.Count)
            {
                IClockValue item = list[index];
                list.RemoveAt(index);
            }
        }

        public IClockValue this[int index]
        {
            get
            {
                return list[index];
            }
            set
            {
                list[index] = value;
            }
        }

        public void Append(IClockValue item)
        {
            list.Add(item);
        }

        public void Add(IClockValue item)
        {
            if (item != null)
            {
                (int index, bool same) = findprevest(0, item.Clock);
                if (same)
                {
                    list[index] = item;
                }
                else
                {
                    list.Insert(index + 1, item);
                }
            }
        }

        //二分法查找临近的位置
        private (int, bool) findprevest(int since, long clock)
        {
            int count = list.Count;
            //二分法查询.
            int pos = -1;
            int low = since;
            int high = count - 1;
            while (low <= high)
            {
                int middle = (low + high) / 2;
                IClockValue cv = list[middle];
                if (cv.Clock == clock)
                {
                    return (middle, true);
                }//时标相同的项已经存在，直接覆盖
                else if (cv.Clock < clock)
                {
                    pos = middle;
                    low = middle + 1;
                }
                else if (cv.Clock > clock)
                {
                    high = middle - 1;
                }
            }
            if (pos > -1)
            {
                return (pos, false);
            }//pos > -1 说明有比添加项小的，要插入到其后，pos=-1表示没有比添加项小的，要插入到第0处
            return (since - 1, false);
        }

        public void AddRange(ClockValues b)
        {
            if (b != null && b.Count > 0)
            {
                if (list.Count == 0)
                {
                    list.AddRange(b);
                }
                else
                {
                    int acount = list.Count, bcount = b.Count;
                    if (list[acount - 1].Clock < b[0].Clock)
                    {
                        list.AddRange(b);
                    }
                    else if (list[0].Clock > b[bcount - 1].Clock)
                    {
                        list.InsertRange(0, b);
                    }
                    else
                    {
                        IClockValue[] array = ArrayPool<IClockValue>.Shared.Rent(acount + bcount);
                        Span<IClockValue> span = array.AsSpan();
                        int i = 0, j = 0, k = 0;
                        while (i < acount && j < bcount)
                        {
                            if (list[i].Clock < b[j].Clock)
                            {
                                span[k++] = list[i++];
                            }
                            else if (list[i].Clock == b[j].Clock)
                            {
                                i++;
                            }
                            else
                            {
                                span[k++] = b[j++];
                            }
                        }
                        while (i < acount)
                        {
                            span[k++] = list[i++];
                        }
                        while (j < bcount)
                        {
                            span[k++] = b[j++];
                        }
                        if (k > acount)
                        {
                            for (int l = 0; l < acount; l++)
                            {
                                list[l] = span[l];
                            }
                            list.AddRange(array.Skip(acount).Take(k - acount));
                        }//将array的内容复制给list.
                        ArrayPool<IClockValue>.Shared.Return(array);
                    }
                }
            }
        }

        public void Clear()
        {
            list.Clear();
        }

        public bool Contains(IClockValue item)
        {
            return list.Contains(item);
        }

        public void CopyTo(IClockValue[] array, int arrayIndex)
        {
            list.CopyTo(array, arrayIndex);
        }

        public int Count
        {
            get { return list.Count; }
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public bool Remove(IClockValue item)
        {
            if (item != null)
            {
                return list.Remove(item);
            }
            return false;
        }

        public void RemoveRange(int index, int count)
        {
            list.RemoveRange(index, count);
        }

        public IEnumerator<IClockValue> GetEnumerator()
        {
            return list.GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return ((System.Collections.IEnumerable)list).GetEnumerator();
        }

        public IList<IClockValue> GetRange(int index, int count)
        {
            return list.GetRange(index, count);
        }
        #endregion


        /// <summary>
        /// 最小时间.
        /// </summary>
        public long MinTime
        {
            get
            {
                if (Count == 0)
                    return Constants.MaxTime;
                return list[0].Clock;
            }
        }

        /// <summary>
        /// 最小时间.
        /// </summary>
        public long MaxTime
        {
            get
            {
                if (Count == 0)
                    return Constants.MinTime;
                return list[Count - 1].Clock;
            }
        }


        /// <summary>
        /// 计算总字节数.
        /// </summary>
        /// <returns>字节数</returns>
        public int Size
        {
            get
            {
                int n = list.Count;
                if (n == 0) return 0;
                else return list[0].Size * n;
            }
        }


        /// <summary>
        /// 删除时标在[min,max]范围内的项.
        /// </summary>
        /// <param name="min">时间范围下限（包含）</param>
        /// <param name="max">时间范围上限（包含）</param>
        public void Exclude(long min, long max)
        {
            int count = list.Count;
            (int start, bool same) = findprevest(0, min);
            if (!same) start++;
            if (start < count)
            {
                (int end, bool same1) = findprevest(start, max);
                if (end >= start)
                {
                    list.RemoveRange(start, end - start + 1);
                }
            }
        }

        /// <summary>
        /// 删除时标不在[min,max]范围内的项.
        /// </summary>
        /// <param name="min">时间范围下限（包含）</param>
        /// <param name="max">时间范围上限（包含）</param>
        public void Include(long min, long max)
        {
            int count = list.Count;
            (int end, bool same) = findprevest(0, max);
            if (end + 1 < count)
            {
                list.RemoveRange(end + 1, count - end - 1);
            }
            (int start, bool same1) = findprevest(0, min);
            if (!same1) start++;
            if (start > 0)
            {
                list.RemoveRange(0, start);
            }
        }

    }
}
