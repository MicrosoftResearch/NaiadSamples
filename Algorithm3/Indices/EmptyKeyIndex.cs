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
    public static class EmptyKeyIndexExtensionMethods
    {
        /// <summary>
        /// Creates an index with no key (only a collection of values)
        /// </summary>
        /// <typeparam name="TValue">The type of the values</typeparam>
        /// <typeparam name="TRecord">The type of the source records</typeparam>
        /// <param name="source">The source of records</param>
        /// <param name="controller">The controller</param>
        /// <param name="valueSelector">A function from record to value</param>
        /// <returns>An index of values with no key</returns>
        public static EmptyKeyIndex<TValue, TRecord> ToEmptyIndex<TValue, TRecord>(this InterGraphDataSink<TRecord> source, Controller controller, Func<TRecord, TValue> valueSelector)
            where TValue : IComparable<TValue>
        {
            return new EmptyKeyIndex<TValue, TRecord>(source, controller, valueSelector);
        }
    }

    public class EmptyKeyIndex<TValue, TRecord> where TValue : IComparable<TValue>
    {
        private readonly Int64 count;
        private readonly InterGraphDataSink<HashSet<TValue>> IndexFragmentStream;

        private class EmptyPrefixExtender<TPrefix> : PrefixExtender<TPrefix, TValue>
        {
            private readonly InterGraphDataSink<HashSet<TValue>> Stream;
            private readonly Int64 Count;

            public EmptyPrefixExtender(InterGraphDataSink<HashSet<TValue>> stream, Int64 count)
            {
                this.Stream = stream;
                this.Count = count;
            }

            public Stream<ExtensionProposal<TPrefix>, Epoch> CountExtensions(Stream<TPrefix, Epoch> stream, int identifier)
            {
                return stream.Select(prefix => new ExtensionProposal<TPrefix>(identifier, prefix, this.Count));
            }

            public Stream<Pair<TPrefix, TValue[]>, Epoch> ExtendPrefixes(Stream<ExtensionProposal<TPrefix>, Epoch> stream, int identifier)
            {
                var count = stream.ForStage.Placement.Count;

                var broadcast = stream.Where(x => x.Index == identifier)
                                       .Select(x => x.Record)
                                       .SelectMany(x => Enumerable.Range(0, count)
                                                                  .Select(vertexId => vertexId.PairWith(x)));

                return stream.ForStage.Computation.NewInput(this.Stream.NewDataSource())
                             .IndexJoin(broadcast, x => x.First, (hashset, prefix) => prefix.Second.PairWith(hashset.ToArray()));
            }

            public Stream<Pair<TPrefix, TValue[]>, Epoch> ValidateExtensions(Stream<Pair<TPrefix, TValue[]>, Epoch> stream)
            {
                var partitioned = stream.SelectMany(pair => pair.Second.Select(extension => pair.First.PairWith(extension)));

                return stream.ForStage.Computation.NewInput(this.Stream.NewDataSource())
                             .IndexJoin(partitioned, pair => pair.Second, (hashset, extension) => extension.First.PairWith(hashset.Contains(extension.Second) ? new TValue[] { extension.Second }
                                                                                                                                                                   : null))
                             .Where(x => x.Second != null);
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

        public EmptyKeyIndex(InterGraphDataSink<TRecord> relation, Controller controller, Func<TRecord, TValue> valueSelector)
        {
            using (var compuation = controller.NewComputation())
            {
                var stream = compuation.NewInput(relation.NewDataSource())
                                       .Select(valueSelector)
                                       .NewUnaryStage((i, s) => new HashSetBuilder(i, s), x => x.GetHashCode(), null, "HashSetBuilder");

                IndexFragmentStream = new InterGraphDataSink<HashSet<TValue>>(stream);

                var localcount = 0L;

                stream.Select(x => x.Count)
                      .Aggregate(x => true, x => (long)x, (x, y) => x + y, (k, c) => c)
                      .SelectMany(c => stream.ForStage.Placement.Select(p => p.VertexId.PairWith(c)))
                      .PartitionBy(x => x.First)
                      .Subscribe((a, b, c) => { localcount = c.Single().Second; });

                compuation.Activate();
                compuation.Join();

                count = localcount;
            }
        }

        public PrefixExtender<TPrefix, TValue> CreateExtender<TPrefix>()
        {
            return new EmptyPrefixExtender<TPrefix>(IndexFragmentStream, count);
        }

        private class HashSetBuilder : UnaryVertex<TValue, HashSet<TValue>, Epoch>
        {
            HashSet<TValue> HashSet = new HashSet<TValue>();

            public override void OnReceive(Message<TValue, Epoch> message)
            {
                for (int i = 0; i < message.length; i++)
                    this.HashSet.Add(message.payload[i]);

                NotifyAt(message.time);
            }

            public override void OnNotify(Epoch time)
            {
                this.Output.GetBufferForTime(time).Send(this.HashSet);
            }

            public HashSetBuilder(int index, Stage<Epoch> stage) : base(index, stage) { }
        }
    }

}
