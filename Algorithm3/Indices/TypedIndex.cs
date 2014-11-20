/*
The MIT License (MIT)

Copyright (c) 2014 Microsoft Corporation

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

*/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.PartitionBy;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;

using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Frameworks.GraphLINQ;

namespace Algorithm3.Indices
{
    public static class TypedIndexExtensionMethods
    {
        /// <summary>
        /// Builds an index with an arbitrary type of key
        /// </summary>
        /// <typeparam name="TKey">The type of the keys</typeparam>
        /// <typeparam name="TValue">The type of the values</typeparam>
        /// <typeparam name="TRecord">The type of the records</typeparam>
        /// <param name="source">The source of records</param>
        /// <param name="controller">The controller</param>
        /// <param name="keySelector">A function from record to key</param>
        /// <param name="valueSelector">A function from record to value</param>
        /// <returns>An index of values</returns>
        public static TypedKeyIndex<TKey, TValue, TRecord> ToTypedIndex<TKey, TValue, TRecord>(this InterGraphDataSink<TRecord> source, Controller controller, Func<TRecord, TKey> keySelector, Func<TRecord, TValue> valueSelector)
            where TValue : IComparable<TValue>
        {
            return new TypedKeyIndex<TKey, TValue, TRecord>(source, controller, keySelector, valueSelector);
        }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    /// <typeparam name="TRecord"></typeparam>
    public class TypedKeyIndex<TKey, TValue, TRecord> where TValue : IComparable<TValue>
    {
        private class Fragment
        {
            private struct LinkedListNode<T>
            {
                public T Data;
                public int Next;

                public LinkedListNode(T data) { this.Data = data; this.Next = -1; }
                public LinkedListNode(T data, int next) { this.Data = data; this.Next = next; }
            }

            private Dictionary<TKey, Pair<int, int>> OffsetCounts = new Dictionary<TKey, Pair<int, int>>();
            private TValue[] Values;

            private Dictionary<TKey, int> tempDict = new Dictionary<TKey, int>();
            private HashSet<Pair<TKey, TValue>> tempHash = new HashSet<Pair<TKey, TValue>>();
            private List<LinkedListNode<TValue>> tempList = new List<LinkedListNode<TValue>>();

            internal void AddRecords(Pair<TKey, TValue>[] records, int count)
            {
                for (int i = 0; i < count; i++)
                {
                    var record = records[i];

                    if (this.tempHash.Add(record))
                    {
                        if (!tempDict.ContainsKey(record.First))
                        {
                            tempList.Add(new LinkedListNode<TValue>(record.Second));
                            tempDict.Add(record.First, tempList.Count - 1);
                        }
                        else
                        {
                            tempList.Add(new LinkedListNode<TValue>(record.Second, tempDict[record.First]));
                            tempDict[record.First] = tempList.Count - 1;
                        }
                    }
                }
            }

            internal Fragment Build()
            {
                this.Values = new TValue[tempList.Count];
                var cursor = 0;

                foreach (var key in tempDict.Keys)
                {
                    var start = cursor;

                    for (int next = tempDict[key]; next != -1; next = tempList[next].Next)
                        this.Values[cursor++] = tempList[next].Data;

                    this.OffsetCounts.Add(key, start.PairWith(cursor - start));
                }

                this.tempDict = null;
                this.tempList = null;
                this.tempHash = null;

                foreach (var pair in this.OffsetCounts.Values)
                {
                    Array.Sort(this.Values, pair.First, pair.Second);
                }

                return this;
            }

            internal Int64 Count(TKey prefix) { if (this.OffsetCounts.ContainsKey(prefix)) return this.OffsetCounts[prefix].Second; else return 0; }

            internal ArraySegment<TValue> Extend(TKey prefix)
            {
                Pair<int, int> lookup;
                if (this.OffsetCounts.TryGetValue(prefix, out lookup))
                    return new ArraySegment<TValue>(this.Values, lookup.First, lookup.Second);
                else
                    return new ArraySegment<TValue>(this.Values, 0, 0);
            }

            private TValue[] tempArray = new TValue[] { };

            internal Pair<TKey, TValue[]> Validate(Pair<TKey, TValue[]> extensions)
            {
                if (this.OffsetCounts.ContainsKey(extensions.First))
                {
                    var offsetCount = this.OffsetCounts[extensions.First];

                    if (Math.Min(offsetCount.Second, extensions.Second.Length) >= tempArray.Length)
                        tempArray = new TValue[Math.Max(Math.Min(offsetCount.Second, extensions.Second.Length) + 1, 2 * tempArray.Length)];

                    var counter = IntersectSortedArrays(new ArraySegment<TValue>(this.Values, offsetCount.First, offsetCount.Second),
                                                        new ArraySegment<TValue>(extensions.Second),
                                                        tempArray);

                    if (counter > 0)
                    {
                        var resultArray = new TValue[counter];

                        Array.Copy(tempArray, resultArray, counter);

                        return extensions.First.PairWith(resultArray);
                    }
                    else
                        return extensions.First.PairWith((TValue[])null);
                }
                else
                    return extensions.First.PairWith((TValue[])null);
            }

            internal static int IntersectSortedArrays<T>(ArraySegment<T> array1, ArraySegment<T> array2, T[] destination) where T : IComparable<T>
            {
                int matchCount = 0;

                int cursor1 = array1.Offset;
                int cursor2 = array2.Offset;

                int extent1 = array1.Offset + array1.Count;
                int extent2 = array2.Offset + array2.Count;

                while (cursor1 < extent1 && cursor2 < extent2)
                {
                    if (array1.Array[cursor1].CompareTo(array2.Array[cursor2]) == 0)
                    {
                        destination[matchCount] = array1.Array[cursor1];
                        matchCount++;

                        cursor1++;
                        cursor2++;
                    }
                    else
                    {
                        if (array1.Array[cursor1].CompareTo(array2.Array[cursor2]) < 0)
                            cursor1 = AdvanceCursor(cursor1, array1, array2.Array[cursor2]);
                        else
                            cursor2 = AdvanceCursor(cursor2, array2, array1.Array[cursor1]);
                    }
                }

                return matchCount;
            }

            // returns the position of the first element at least as large as otherValue. 
            // If no such element exists, returns values.Offset + values.Count.
            internal static int AdvanceCursor<T>(int cursor, ArraySegment<T> values, T otherValue) where T : IComparable<T>
            {
                var extent = values.Offset + values.Count;

                int step = 1;

                while (cursor + step < extent && values.Array[cursor + step].CompareTo(otherValue) <= 0)
                {
                    cursor = cursor + step;
                    step *= 2;
                }

                // either cursor + step is off the end of the array, or points at a value larger than otherValue
                while (step > 0)
                {
                    if (cursor + step < extent && values.Array[cursor + step].CompareTo(otherValue) <= 0)
                        cursor = cursor + step;

                    step = step / 2;
                }

                // perhaps we didn't find the right value, but should still advance the cursor to/past the match
                if (values.Array[cursor].CompareTo(otherValue) < 0)
                    cursor++;

                return cursor;
            }

        }

        private class Extender<TPrefix> : PrefixExtender<TPrefix, TValue>
        {
            private readonly Stream<Fragment, Epoch> Index;
            private readonly Func<TPrefix, TKey> tupleKey;

            public Extender(Stream<Fragment, Epoch> stream, Func<TPrefix, TKey> tKey)
            {
                this.Index = stream;
                this.tupleKey = tKey;
            }

            public Stream<ExtensionProposal<TPrefix>, Epoch> CountExtensions(Stream<TPrefix, Epoch> stream, int identifier)
            {
                return this.Index
                           .IndexJoin(stream, tupleKey, (index, tuple) => new ExtensionProposal<TPrefix>(identifier, tuple, index.Count(tupleKey(tuple))));
            }

            public Stream<Pair<TPrefix, TValue[]>, Epoch> ExtendPrefixes(Stream<ExtensionProposal<TPrefix>, Epoch> stream, int identifier)
            {
                return this.Index
                           .IndexJoin(stream.Where(x => x.Index == identifier)
                                            .Select(x => x.Record),
                                      tupleKey, (index, tuple) => tuple.PairWith(index.Extend(tupleKey(tuple)).ToArray()));
            }

            public Stream<Pair<TPrefix, TValue[]>, Epoch> ValidateExtensions(Stream<Pair<TPrefix, TValue[]>, Epoch> stream)
            {
                return this.Index
                           .IndexJoin(stream, tuple => tupleKey(tuple.First), (index, tuple) => tuple.First.PairWith(index.Validate(tupleKey(tuple.First).PairWith(tuple.Second)).Second))
                           .Where(x => x.Second != null && x.Second.Length != 0);
            }


            public Stream<ExtensionProposal<TPrefix>, Epoch> ImproveExtensions(Stream<ExtensionProposal<TPrefix>, Epoch> stream, int identifier)
            {
                throw new NotImplementedException();
            }


            public Stream<Pair<TPrefix, TValue>, Epoch> ValidateExtensions(Stream<Pair<TPrefix, TValue>, Epoch> stream)
            {
                throw new NotImplementedException();
            }
        }

        private readonly InterGraphDataSink<Fragment> result;

        public TypedKeyIndex(InterGraphDataSink<TRecord> relation, Controller controller, Func<TRecord, TKey> keySelector, Func<TRecord, TValue> valueSelector)
        {
            using (var compuation = controller.NewComputation())
            {
                var stream = compuation.NewInput(relation.NewDataSource())
                                       .Select(x => keySelector(x).PairWith(valueSelector(x)))
                                       .NewUnaryStage((i, s) => new IndexBuilder(i, s), x => x.First.GetHashCode(), null, "IndexBuilder");

                result = new InterGraphDataSink<Fragment>(stream);
                compuation.Activate();
                compuation.Join();
            }
        }

        public PrefixExtender<TPrefix, TValue> CreateExtender<TPrefix>(Computation computation, Func<TPrefix, TKey> prefixKeySelector)
        {
            return new Extender<TPrefix>(computation.NewInput(result.NewDataSource()), prefixKeySelector);
        }

        private class IndexBuilder : UnaryVertex<Pair<TKey, TValue>, Fragment, Epoch>
        {
            Fragment PrefixIndex = new Fragment();

            public override void OnReceive(Message<Pair<TKey, TValue>, Epoch> message)
            {
                this.PrefixIndex.AddRecords(message.payload, message.length);
                NotifyAt(message.time);
            }

            public override void OnNotify(Epoch time)
            {
                this.Output.GetBufferForTime(time).Send(this.PrefixIndex.Build());
            }

            public IndexBuilder(int index, Stage<Epoch> stage) : base(index, stage) { }
        }

    }
}
