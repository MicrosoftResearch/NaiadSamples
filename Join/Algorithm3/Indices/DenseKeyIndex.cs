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
    public static class DenseKeyIndexExtensionMethods
    {
        /// <summary>
        /// Creates an index with a dense integer key.
        /// </summary>
        /// <typeparam name="TValue">The type of the values</typeparam>
        /// <typeparam name="TRecord">The type of the source records</typeparam>
        /// <param name="source">The source of records</param>
        /// <param name="controller">The controller</param>
        /// <param name="keySelector">A function from record to dense integer key</param>
        /// <param name="valueSelector">A function from record to value</param>
        /// <returns>An index of values keyed by a dense integer</returns>
        public static DenseKeyIndex<TValue, TRecord> ToDenseIndex<TValue, TRecord>(this InterGraphDataSink<TRecord> source, Controller controller, Func<TRecord, int> keySelector, Func<TRecord, TValue> valueSelector)
            where TValue : IComparable<TValue>
        {
            return new DenseKeyIndex<TValue, TRecord>(source, controller, keySelector, valueSelector);
        }
    }

    /// <summary>
    /// Builds PrefixExtenders based on dense integer keys.
    /// </summary>
    /// <typeparam name="TValue">Type of value to extend with.</typeparam>
    /// <typeparam name="TRecord">Type of record the builder is based on.</typeparam>
    public class DenseKeyIndex<TValue, TRecord> where TValue : IComparable<TValue>
    {
        private readonly InterGraphDataSink<Fragment> result;

        /// <summary>
        /// Constructs a PrefixExtender factory keyed on dense integers.
        /// </summary>
        /// <param name="relation">Source of data for the prefix extender.</param>
        /// <param name="controller">Controller from which computation can be rooted.</param>
        /// <param name="keySelector">Map from records to relatively dense integers.</param>
        /// <param name="valueSelector">Map from records to extending values.</param>
        public DenseKeyIndex(InterGraphDataSink<TRecord> relation, Controller controller, Func<TRecord, int> keySelector, Func<TRecord, TValue> valueSelector)
        {
            using (var computation = controller.NewComputation())
            {
                var stream = computation.NewInput(relation.NewDataSource())
                                        .Select(x => keySelector(x).PairWith(valueSelector(x)))
                                        .NewUnaryStage((i, s) => new FragmentBuilder(i, s), x => x.First, null, "IndexBuilder");

                result = new InterGraphDataSink<Fragment>(stream);
                computation.Activate();
                computation.Join();
            }
        }


        /// <summary>
        /// Creates a dense integer keyed PrefixExtender.
        /// </summary>
        /// <typeparam name="TPrefix">Type of the prefix to extend</typeparam>
        /// <param name="computation">The computation </param>
        /// <param name="keySelector">Map from prefixes to relatively dense integers.</param>
        /// <returns>A PrefixExtender based on the supplied key selector and underlying index.</returns>
        public PrefixExtender<TPrefix, TValue> CreateExtender<TPrefix>(Func<TPrefix, int> keySelector)
        {
            return new Extender<TPrefix>(result, keySelector);
        }

        private class FragmentBuilder : UnaryVertex<Pair<int, TValue>, Fragment, Epoch>
        {
            Fragment Index = new Fragment();

            public override void OnReceive(Message<Pair<int, TValue>, Epoch> message)
            {
                this.Index.AddRecords(message.payload, message.length);
                NotifyAt(message.time);
            }

            public override void OnNotify(Epoch time)
            {
                this.Output.GetBufferForTime(time).Send(this.Index.Build(this.Stage.Placement.Count));
            }

            public FragmentBuilder(int index, Stage<Epoch> stage) : base(index, stage) { }
        }

        private class Fragment
        {
            private int[] Offsets = new int[] { };
            private TValue[] Values;

            private int Parts;

            private List<Pair<int, TValue>> tempList = new List<Pair<int, TValue>>();

            // private HashSet<Pair<int, TValue>> tempHash = new HashSet<Pair<int, TValue>>();
            private TValue[] tempArray = new TValue[] { };

            internal void AddRecords(Pair<int, TValue>[] records, int count)
            {
                for (int i = 0; i < count; i++)
                    this.tempList.Add(records[i]);
            }

            internal Fragment Build(int parts)
            {
                this.Parts = parts;
                this.Offsets = new int[(this.tempList.Max(x => x.First) / this.Parts) + 2];

                // determine the number of values associated with each key
                foreach (var pair in this.tempList)
                    this.Offsets[pair.First / this.Parts] += 1;

                // determine offsets in the array of values
                for (int i = 1; i < this.Offsets.Length; i++)
                    this.Offsets[i] += this.Offsets[i - 1];

                // reset offsets 
                for (int i = this.Offsets.Length - 1; i > 0; i--)
                    this.Offsets[i] = this.Offsets[i - 1];
                this.Offsets[0] = 0;

                // allocate and insert values at offsets
                this.Values = new TValue[tempList.Count];
                foreach (var pair in this.tempList)
                    this.Values[this.Offsets[pair.First / this.Parts]++] = pair.Second;

                // reset offsets 
                for (int i = this.Offsets.Length - 1; i > 0; i--)
                    this.Offsets[i] = this.Offsets[i - 1];
                this.Offsets[0] = 0;

                // release temp storage
                this.tempList.Clear();
                this.tempList = null;

                // reset offset cursors, sort associated values
                for (int i = 0; i < this.Offsets.Length - 1; i++)
                    Array.Sort(this.Values, this.Offsets[i], this.Offsets[i + 1] - this.Offsets[i]);

                return this;
            }

            internal void Count<TPrefix>(TPrefix[] prefixes, int length, Func<TPrefix, int> keyFunc, VertexOutputBufferPerTime<ExtensionProposal<TPrefix>, Epoch> output, int identifier)
            {
                for (int i = 0; i < length; i++)
                {
                    var localName = keyFunc(prefixes[i]) / this.Parts;
                    if (localName < this.Offsets.Length - 1)
                        output.Send(new ExtensionProposal<TPrefix>(identifier, prefixes[i], this.Offsets[localName + 1] - this.Offsets[localName]));
                }
            }

            internal void Improve<TPrefix>(ExtensionProposal<TPrefix>[] proposals, int length, Func<TPrefix, int> key, VertexOutputBufferPerTime<ExtensionProposal<TPrefix>, Epoch> output, int identifier)
            {
                for (int i = 0; i < length; i++)
                {
                    var localName = key(proposals[i].Record) / this.Parts;
                    if (localName < this.Offsets.Length - 1)
                    {
                        var count = this.Offsets[localName + 1] - this.Offsets[localName];
                        if (count < proposals[i].Count)
                            output.Send(new ExtensionProposal<TPrefix>(identifier, proposals[i].Record, count));
                        else
                            output.Send(proposals[i]);
                    }
                }
            }

            internal void Extend<TPrefix>(TPrefix[] prefixes, int length, Func<TPrefix, int> keyFunc, VertexOutputBufferPerTime<Pair<TPrefix, TValue[]>, Epoch> output)
            {
                for (int i = 0; i < length; i++)
                {
                    var localName = keyFunc(prefixes[i]) / this.Parts;

                    if (localName < this.Offsets.Length - 1)
                    {
                        var result = new TValue[this.Offsets[localName + 1] - this.Offsets[localName]];
                        Array.Copy(this.Values, this.Offsets[localName], result, 0, result.Length);
                        output.Send(prefixes[i].PairWith(result));
                    }
                }
            }

            internal void Validate<TPrefix>(Pair<TPrefix, TValue[]>[] extensions, int length, Func<TPrefix, int> keyFunc, VertexOutputBufferPerTime<Pair<TPrefix, TValue[]>, Epoch> output)
            {
                for (int i = 0; i < length; i++)
                {
                    var localName = keyFunc(extensions[i].First) / this.Parts;
                    if (localName < this.Offsets.Length - 1)
                    {
                        // reports number of intersections, written back in to the second array.
                        var counter = IntersectSortedArrays(localName, extensions[i].Second);
                        if (counter > 0)
                        {
                            if (counter == extensions[i].Second.Length)
                            {
                                output.Send(extensions[i]);
                            }
                            else
                            {
                                var resultArray = new TValue[counter];
                                Array.Copy(extensions[i].Second, resultArray, counter);
                                output.Send(extensions[i].First.PairWith(resultArray));
                            }
                        }
                    }
                }
            }

            internal int IntersectSortedArrays(int localName, TValue[] array)
            {
                var cursor = this.Offsets[localName];
                var extent = this.Offsets[localName + 1];

                var counts = 0;
                var buffer = this.Values;

                for (int i = 0; i < array.Length; i++)
                {
                    var value = array[i];

                    var comparison = buffer[cursor].CompareTo(value);

                    if (comparison < 0)
                    {
                        cursor = AdvanceCursor(cursor, extent, value);
                        if (cursor >= extent)
                            return counts;

                        comparison = buffer[cursor].CompareTo(value);
                    }

                    if (comparison == 0 && cursor < extent)
                    {
                        array[counts++] = value;
                        cursor++;

                        if (cursor >= extent)
                            return counts;
                    }
                }

                return counts;
            }
#if false

            // returns the position of the first element at least as large as otherValue. 
            // If no such element exists, returns extent.
            internal int AdvanceCursor(int cursor, int extent, TValue otherValue)
            {
                var values = this.Values;

                int step = 1;
                while (cursor + step < extent && values[cursor + step].CompareTo(otherValue) < 0)
                {
                    cursor = cursor + step;
                    step *= 2;
                }

                var limit = Math.Min(extent, cursor + step);

                // now we do binary search in [cursor, limit)

                while (cursor + 1 < limit)
                { 
                    var mid = (cursor + limit) / 2;

                    var comparison = values[mid].CompareTo(otherValue);
                    if (comparison == 0)
                        return mid;
                    else
                    {
                        if (comparison < 0)
                            cursor = mid;
                        else
                            limit = mid;
                    }
                }

                return limit;
            }

#else
            // returns the position of the first element at least as large as otherValue. 
            // If no such element exists, returns extent.
            internal int AdvanceCursor(int cursor, int extent, TValue otherValue)
            {
                var values = this.Values;

                int step = 1;
                while (cursor + step < extent && values[cursor + step].CompareTo(otherValue) < 0)
                {
                    cursor = cursor + step;
                    step *= 2;
                }

                step = step / 2;

                // either cursor + step is off the end of the array, or points at a value larger than otherValue
                while (step > 0)
                {
                    if (cursor + step < extent)
                    {
                        var comparison = values[cursor + step].CompareTo(otherValue);
                        if (comparison == 0)
                            return cursor + step;

                        if (comparison < 0)
                            cursor = cursor + step;
                    }

                    step = step / 2;
                }

                return cursor + 1;
            }

#endif
        }

        private class Extender<TPrefix> : PrefixExtender<TPrefix, TValue>
        {
            // private readonly Stream<Index, Epoch> Index;
            private readonly InterGraphDataSink<Fragment> Index;

            private readonly Func<TPrefix, int> keySelector;

            public Extender(InterGraphDataSink<Fragment> index, Func<TPrefix, int> tKey)
            {
                this.Index = index;
                this.keySelector = tKey;
            }

            private Stream<TOutput, Epoch> FragmentJoin<TInput, TOutput>(Stream<TInput, Epoch> stream, Func<TInput, int> keyFunc, Action<Fragment, TInput[], int, VertexOutputBufferPerTime<TOutput, Epoch>> action)
            {
                return stream.ForStage.Computation.NewInput(Index.NewDataSource())
                             .IndexJoin<Fragment, TInput, int, TOutput>(stream, keyFunc, action);
            }

            public Stream<ExtensionProposal<TPrefix>, Epoch> CountExtensions(Stream<TPrefix, Epoch> stream, int identifier)
            {
                return this.FragmentJoin<TPrefix, ExtensionProposal<TPrefix>>(stream, keySelector, (index, prefixes, length, output) => index.Count(prefixes, length, keySelector, output, identifier));
            }

            public Stream<ExtensionProposal<TPrefix>, Epoch> ImproveExtensions(Stream<ExtensionProposal<TPrefix>, Epoch> stream, int identifier)
            {
                return this.FragmentJoin<ExtensionProposal<TPrefix>, ExtensionProposal<TPrefix>>(stream, proposal => keySelector(proposal.Record),
                                                                                                 (a, b, c, d) => a.Improve(b, c, keySelector, d, identifier));
            }

            public Stream<Pair<TPrefix, TValue[]>, Epoch> ExtendPrefixes(Stream<ExtensionProposal<TPrefix>, Epoch> stream, int identifier)
            {
                return this.FragmentJoin<TPrefix, Pair<TPrefix, TValue[]>>(stream.Where(x => x.Index == identifier).Select(x => x.Record),
                                                                           keySelector, (a, b, c, d) => a.Extend(b, c, this.keySelector, d));
            }

            public Stream<Pair<TPrefix, TValue[]>, Epoch> ValidateExtensions(Stream<Pair<TPrefix, TValue[]>, Epoch> stream)
            {
                return this.FragmentJoin<Pair<TPrefix, TValue[]>, Pair<TPrefix, TValue[]>>(stream, tuple => keySelector(tuple.First),
                                                                                           (a, b, c, d) => a.Validate(b, c, this.keySelector, d));
            }
        }
    }


}
