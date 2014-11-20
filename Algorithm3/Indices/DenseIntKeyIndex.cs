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
    public static class DenseIntKeyIndexExtensionMethods
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
        public static DenseIntKeyIndex<TRecord> ToDenseKeyIndex<TRecord>(this InterGraphDataSink<TRecord> source, Controller controller, Func<TRecord, int> keySelector, Func<TRecord, int> valueSelector)
        {
            return new DenseIntKeyIndex<TRecord>(source, controller, keySelector, valueSelector);
        }
    }


    /// <summary>
    /// Builds PrefixExtenders based on dense integer keys.
    /// </summary>
    /// <typeparam name="int">Type of value to extend with.</typeparam>
    /// <typeparam name="TRecord">Type of record the builder is based on.</typeparam>
    public class DenseIntKeyIndex<TRecord>
    {
        private readonly InterGraphDataSink<Fragment> result;

        /// <summary>
        /// Constructs a PrefixExtender factory keyed on dense integers.
        /// </summary>
        /// <param name="relation">Source of data for the prefix extender.</param>
        /// <param name="controller">Controller from which computation can be rooted.</param>
        /// <param name="keySelector">Map from records to relatively dense integers.</param>
        /// <param name="valueSelector">Map from records to extending values.</param>
        public DenseIntKeyIndex(InterGraphDataSink<TRecord> relation, Controller controller, Func<TRecord, int> keySelector, Func<TRecord, int> valueSelector)
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

        public DenseIntKeyIndex(string format, Controller controller, int parts)
        {
            using (var computation = controller.NewComputation())
            {
                var stream = Enumerable.Range(0, parts)
                                       .AsNaiadStream(computation)
                                       .PartitionBy(x => x)
                                       .Select(x => string.Format(format, x))
                                       .NewUnaryStage((i, s) => new FragmentLoader(i, s, parts), null, null, "IndexLoader");

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
        public PrefixExtender<TPrefix, int> CreateExtender<TPrefix>(Func<TPrefix, int> keySelector)
        {
            return new Extender<TPrefix>(result, keySelector);
        }

        private class FragmentBuilder : UnaryVertex<Pair<int, int>, Fragment, Epoch>
        {
            Fragment Index = new Fragment();

            public override void OnReceive(Message<Pair<int, int>, Epoch> message)
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


        private class FragmentLoader : UnaryVertex<string, Fragment, Epoch>
        {
            Fragment Index = new Fragment();

            int parts;
            public override void OnReceive(Message<string, Epoch> message)
            {
                // really shouldn't be called with more than one string.
                var output = this.Output.GetBufferForTime(message.time);
                for (int i = 0; i < message.length; i++)
                    output.Send(this.Index.Load(message.payload[i], parts));
            }

            public FragmentLoader(int index, Stage<Epoch> stage, int parts)
                : base(index, stage)
            {
                this.parts = parts;
            }
        }


        private class Fragment
        {
            private int[] Offsets = new int[] { };
            private int[] Values;

            private int Parts;

            private List<Pair<int, int>> tempList = new List<Pair<int, int>>();

            // private HashSet<Pair<int, TValue>> tempHash = new HashSet<Pair<int, TValue>>();
            private int[] tempArray = new int[] { };

            internal void AddRecords(Pair<int, int>[] records, int count)
            {
                for (int i = 0; i < count; i++)
                    this.tempList.Add(records[i]);
            }

            internal Fragment Load(string filename, int parts)
            {
                this.Parts = parts;

                var MaxIndex = 0;
                var ValueCnt = 0;

                var bytes = new byte[8];
                var ints = new int[bytes.Length / 4];

                using (var reader = System.IO.File.OpenRead(filename))
                {
                    reader.Read(bytes, 0, 8);
                    Buffer.BlockCopy(bytes, 0, ints, 0, 8);

                    var vertex = ints[0];
                    var degree = ints[1];

                    if (bytes.Length < 4 * degree)
                    {
                        bytes = new byte[4 * degree];
                        ints = new int[degree];
                    }

                    reader.Read(bytes, 0, 4 * degree);

                    if (MaxIndex < vertex)
                        MaxIndex = vertex;

                    ValueCnt += degree;
                }

                this.Offsets = new int[MaxIndex + 2];
                this.Values = new int[ValueCnt];

                var prevVertex = -1;
                var cursor = 0;

                using (var reader = System.IO.File.OpenRead(filename))
                {
                    reader.Read(bytes, 0, 8);
                    Buffer.BlockCopy(bytes, 0, ints, 0, 8);

                    var vertex = ints[0];
                    var degree = ints[1];

                    if (prevVertex != -1 && prevVertex > vertex)
                        throw new Exception("Loaded DenseIntKeyIndex from improper file format; vertices not in order");

                    this.Offsets[vertex] = cursor;

                    if (bytes.Length < 4 * degree)
                    {
                        bytes = new byte[4 * degree];
                        ints = new int[degree];
                    }

                    reader.Read(bytes, 0, 4 * degree);
                    Buffer.BlockCopy(bytes, 0, this.Values, cursor, 4 * degree);

                    this.Offsets[vertex + 1] = cursor + degree;
                    cursor += degree;
                }

                // fill in any gaps left behind.
                for (int i = 1; i < this.Offsets.Length; i++)
                    this.Offsets[i] = Math.Max(this.Offsets[i], this.Offsets[i - 1]);

                return this;
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
                this.Values = new int[tempList.Count];
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

                var hits = 0;
                var totError = 0L;
                var maxError = 0;
                for (int i = 0; i < this.Offsets.Length - 1; i++)
                {
                    if (this.Offsets[i] < this.Offsets[i + 1])
                    {
                        var divisor = ((this.Offsets.Length - 1) * this.Parts) / (this.Offsets[i + 1] - this.Offsets[i]);

                        for (int j = this.Offsets[i]; j < this.Offsets[i + 1]; j++)
                        {
                            var expected = this.Values[j] / divisor;
                            var error = Math.Abs(expected - j + this.Offsets[i]);
                            maxError = Math.Max(maxError, error);
                            totError += error;

                            if (error == 0)
                                hits++;
                        }
                    }
                }

                //Console.WriteLine("MaxError: {0}\tAvgError: {1:0.00}\tDirectHits: {2:0.00}", maxError, ((double)totError) / this.Values.Length, ((double) hits) / this.Values.Length);
                Console.WriteLine("Loaded {0} edges", this.Values.Length);

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

            internal void Extend<TPrefix>(TPrefix[] prefixes, int length, Func<TPrefix, int> keyFunc, VertexOutputBufferPerTime<Pair<TPrefix, int[]>, Epoch> output)
            {
                for (int i = 0; i < length; i++)
                {
                    var localName = keyFunc(prefixes[i]) / this.Parts;

                    if (localName < this.Offsets.Length - 1)
                    {
                        var result = new int[this.Offsets[localName + 1] - this.Offsets[localName]];
                        Array.Copy(this.Values, this.Offsets[localName], result, 0, result.Length);
                        output.Send(prefixes[i].PairWith(result));
                    }
                }
            }

            internal void Validate<TPrefix>(Pair<TPrefix, int[]>[] extensions, int length, Func<TPrefix, int> keyFunc, VertexOutputBufferPerTime<Pair<TPrefix, int[]>, Epoch> output)
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
                                var resultArray = new int[counter];
                                Array.Copy(extensions[i].Second, resultArray, counter);
                                output.Send(extensions[i].First.PairWith(resultArray));
                            }
                        }
                    }
                }
            }

            internal int IntersectSortedArrays(int localName, int[] array)
            {
                var cursor = (uint)this.Offsets[localName];
                var extent = (uint)this.Offsets[localName + 1];

                var counts = 0;
                var buffer = this.Values;

                for (int i = 0; i < array.Length; i++)
                {
                    var value = array[i];

                    var comparison = buffer[cursor] - value;

                    if (comparison < 0)
                    {
                        uint step = 1;
                        while (cursor + step < extent && buffer[cursor + step] < value)
                        {
                            cursor = cursor + step;
                            step = step << 1;
                        }

                        step = step >> 1;

                        // either cursor + step is off the end of the array, or points at a value larger than otherValue
                        while (step > 0)
                        {
                            if (cursor + step < extent)
                            {
                                if (buffer[cursor + step] < value)
                                    cursor = cursor + step;
                            }

                            step = step >> 1;
                        }

                        cursor++;

                        if (cursor >= extent)
                            return counts;

                        comparison = buffer[cursor] - value;
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
        }

        private class Extender<TPrefix> : PrefixExtender<TPrefix, int>
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

            public Stream<Pair<TPrefix, int[]>, Epoch> ExtendPrefixes(Stream<ExtensionProposal<TPrefix>, Epoch> stream, int identifier)
            {
                return this.FragmentJoin<TPrefix, Pair<TPrefix, int[]>>(stream.Where(x => x.Index == identifier).Select(x => x.Record),
                                                                           keySelector, (a, b, c, d) => a.Extend(b, c, this.keySelector, d));
            }

            public Stream<Pair<TPrefix, int[]>, Epoch> ValidateExtensions(Stream<Pair<TPrefix, int[]>, Epoch> stream)
            {
                return this.FragmentJoin<Pair<TPrefix, int[]>, Pair<TPrefix, int[]>>(stream, tuple => keySelector(tuple.First),
                                                                                           (a, b, c, d) => a.Validate(b, c, this.keySelector, d));
            }
        }
    }

}
