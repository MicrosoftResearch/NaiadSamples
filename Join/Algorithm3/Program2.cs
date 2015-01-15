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

using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;

namespace Algorithm3
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            var config = Configuration.FromArgs(ref args);

            //config.InlineReporting = true;
            //config.AggregateReporting = true;

            using (var controller = NewController.FromConfig(config))// NewController.FromArgs(ref args))
            {
#if true
                var format = args[0];
                var parts = Int32.Parse(args[1]);

                //Console.WriteLine("Reading {0} parts from format string {1}", parts, format);

                //var format = @"C:\Users\mcsherry\Documents\Visual Studio 2010\Projects\Graph.NET\datasets\twitter_rv.bin";
                //var format = @"C:\Users\mcsherry\Desktop\livejournal-{0}.bin";
                //var parts = 4;

                // load up the graph file from a partitioned representation, discard self-loops, normalize edge direction.
                var graph = controller.NewInterGraphStream(Enumerable.Range(0, parts), 
                                                           stream => stream.PartitionBy(x => x)
                                                                           .SelectMany(x => string.Format(format, x).ReadEdges())
                                                                           .Where(x => x.First != x.Second)
                                                                           .Select(x => x.First < x.Second ? x : x.Second.PairWith(x.First))
                                                                           .Distinct());
#else
                var format = @"..\..\soc-Epinions1.txt";
                var parts = 1;

                // load up the graph file from a partitioned representation, discard self-loops, normalize edge direction.
                var graph = controller.NewInterGraphStream(Enumerable.Range(0, parts),
                                                           stream => stream.PartitionBy(x => x)
                                                                           .SelectMany(x => string.Format(format, x).ReadText())
                                                                           .Where(x => x.First != x.Second)
                                                                           .Select(x => x.First < x.Second ? x : x.Second.PairWith(x.First))
                                                                           .Distinct());
#endif

                Console.WriteLine("{0} Graph data are loaded and normalized", stopwatch.Elapsed);

                // we build two indices, one keyed on "no" attributes and one keyed on the first.
                var emptyKeyIndex = graph.ToEmptyIndex(controller, x => x.First);
                var denseKeyIndex = graph.ToDenseIndex(controller, x => x.First, x => x.Second);

                graph = null;   // perhaps release the memory associated with graph. better to make it disposable.

                Console.WriteLine("{0} Relation prefix indices built", stopwatch.Elapsed);
                Console.WriteLine("{0} Starting query construction", stopwatch.Elapsed);

                // prefix extenders take prefixes of various types and propose / validate new values.
                // each of these extender collections are of prefixes of length 0, 1, 2, respectively.
                // the length 1 extender is cheating a bit; it does not validate against G(b, *).
                var prefixExtenders0 = new[] { emptyKeyIndex.CreateExtender<bool>() };
                var prefixExtenders1 = new[] { denseKeyIndex.CreateExtender((int xA) => xA) };
                var prefixExtenders2 = new[] { denseKeyIndex.CreateExtender((Pair<int, int> xAB) => xAB.First),
                                               denseKeyIndex.CreateExtender((Pair<int, int> xAB) => xAB.Second) };

                using (var computation = controller.NewComputation())
                {
                    // we seed the computation with a single "true" record.
                    var seed = new BatchedDataSource<bool>();

                    // add new variables one layer at a time. we start with a bool, but discard it asap.
                    var triangles = computation.NewInput(seed)
                                               .GenericJoinLayer(prefixExtenders0).Select(x => x.Second)
                                               .GenericJoinLayer(prefixExtenders1)
                                               .GenericJoinLayer(prefixExtenders2);

                    // optional things to comment/uncomment, based on what sort of output we would like to see.
                    // triangles.Where(x => { Console.WriteLine("{0} Triangle observed: {1}", stopwatch.Elapsed, x); return true; });
                    // triangles.Select(x => true).Count().Subscribe(x => { foreach (var y in x) Console.WriteLine("Triangles: {0}", y.Second); });

#if false                    
                    var edgeSelectors = new Func<Pair<int, int>, int>[] { x => x.First, x => x.Second };

                    triangles.PairWith(edgeSelectors.ExtendSelectors())
                             .ExtendCliques(denseBuilder)
                             .ExtendCliques(denseBuilder)
                             .ExtendCliques(denseBuilder)
                           //.First
                           //.Where(x => { Console.WriteLine("{0} 6-clique observed: {1}", stopwatch.Elapsed, x); return true; })
                             ;
#endif

                    seed.OnNext();

                    // start computation and await confirmation that indices are in place.
                    computation.Activate();
                    computation.Sync(0);

                    Console.WriteLine("{0} Starting query execution", stopwatch.Elapsed);

                    // introduce query seed, starting computation.
                    seed.OnCompleted(true);

                    computation.Join();
                }

                controller.Join();
            }

            Console.WriteLine("{0} All triangles listed", stopwatch.Elapsed);
        }
    }


    public static class ExtensionMethods
    {
        /// <summary>
        /// One level of GenericJoin, taking functions acting on the stream of candidates as if they were the relations.
        /// </summary>
        /// <typeparam name="V">The new value type</typeparam>
        /// <typeparam name="T">The current tuple type</typeparam>
        /// <param name="Candidates">A stream of candidates of type T</param>
        /// <param name="CountExtensions">A sequence of functions, one per relation, transforming a stream of Ts into a stream of 
        ///                               indexed counts of Ts, corresponding to the number of extensions of each T in each relation.</param>
        /// <param name="ExtendPrefixes">A sequence of functions, one per relation, transforming a stream of Ts into a stream of 
        ///                              Pair(T, V) for each valid extension of T in the relation."/></param>
        /// <param name="ValidateCandidate">A sequence of functions, one per relation, intersecting a stream of Pair(T, V)s with 
        ///                                 those Pair(T, V)s found in the relation.</param>
        /// <returns>A stream of Pair(T, V) corresponding to extensions of Candidates validated by all relations.</returns>
        public static Stream<Pair<T, V>, Epoch> GenericJoinLayer<V, T>(this Stream<T, Epoch> Candidates,
                                                                       IEnumerable<PrefixExtender<T, V>> Relations)
            where V : IComparable<V>
        {
            // count the extensions in each relation, aggregate them together, and determine the minimum.
            var Smallest = Relations.Select((relation, id) => relation.CountExtensions(Candidates, id))
                                    .Aggregate((x, y) => x.Concat(y))
                                    .MinCount(Relations.Count());

            // collect the extensions proposed by each relation. validate each set against other relations.
            var Proposals = Relations.Select((relation1, id1) => relation1.ExtendPrefixes(Smallest, id1))
                                     .Select((proposals, id1) => Relations.Where((relation2, id2) => id1 != id2)
                                                                          .Aggregate(proposals, (props, rel) => rel.ValidateExtensions(props)))
                                     .Aggregate((x, y) => x.Concat(y))
                                     .Expand();

            return Proposals;
        }

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

        public static InterGraphDataSink<TOutput> NewInterGraphStream<TRecord, TOutput>(this Controller controller, IEnumerable<TRecord> source, Func<Stream<TRecord, Epoch>, Stream<TOutput, Epoch>> transformation)
        {
            InterGraphDataSink<TOutput> result;
            using (var computation = controller.NewComputation())
            {
                result = new InterGraphDataSink<TOutput>(transformation(source.AsNaiadStream(computation)));

                computation.Activate();
                computation.Join();
            }

            return result;
        }

        static public IEnumerable<Pair<int, int>> ReadEdges(this string filename)
        {
            var bytes = new byte[8]; // temp storage for bytes
            var ints = new int[2];   // temp storage for ints

            using (var reader = System.IO.File.OpenRead(filename))
            {
                var bytesToRead = reader.Length;

                while (bytesToRead > 0)
                {
                    // read vertex name and degree
                    bytesToRead -= reader.Read(bytes, 0, 8);
                    Buffer.BlockCopy(bytes, 0, ints, 0, 8);

                    var vertex = ints[0];
                    var degree = ints[1];

                    if (ints.Length < degree)
                    {
                        ints = new int[Math.Max(degree, ints.Length * 2)];
                        bytes = new byte[4 * ints.Length];
                    }

                    // read names of neighbors
                    bytesToRead -= reader.Read(bytes, 0, 4 * degree);
                    Buffer.BlockCopy(bytes, 0, ints, 0, 4 * degree);

                    for (int i = 0; i < degree; i++)
                        yield return vertex.PairWith(ints[i]);
                }
            }
        }

        static public IEnumerable<Pair<int, int>> ReadText(this string filename)
        {
            return System.IO.File.ReadAllLines(filename)
                                 .Where(x => !x.StartsWith("#"))
                                 .Select(x => x.Split())
                                 .Select(x => Int32.Parse(x[0]).PairWith(Int32.Parse(x[1])));
        }

        #region Extension methods for generating cliques of arbitrary order

        static public Stream<Pair<T, int>, Epoch> NextOrderCliques<T>(this Stream<T, Epoch> cliques, DenseKeyIndex<int, Pair<int, int>> index, IEnumerable<Func<T, int>> selectors)
        {
            return cliques.GenericJoinLayer(selectors.Select(selector => index.CreateExtender(selector)));
        }

        public static List<Func<Pair<T, int>, int>> ExtendSelectors<T>(this IEnumerable<Func<T, int>> selectors)
        {
            return selectors.Select(selector => { Func<Pair<T, int>, int> func = x => selector(x.First); return func; })
                            .Concat(new Func<Pair<T, int>, int>[] { x => x.Second })
                            .ToList();

        }

        static public Pair<Stream<Pair<T, int>, Epoch>, List<Func<Pair<T, int>, int>>> ExtendCliques<T>(this Pair<Stream<T, Epoch>, List<Func<T, int>>> cliques, DenseKeyIndex<int, Pair<int, int>> index)
        {
            var newSelectors = cliques.Second.ExtendSelectors();
            return cliques.First.NextOrderCliques<T>(index, cliques.Second).PairWith(newSelectors);
        }

        #endregion
    }

   

    /// <summary>
    /// A class capable of proposing and validating extensions of type TValue to prefixes of type TPrefix
    /// </summary>
    /// <typeparam name="TPrefix">The type of the prefix to extend</typeparam>
    /// <typeparam name="TValue">The type of the value to extend with</typeparam>
    public interface PrefixExtender<TPrefix, TValue> where TValue : IComparable<TValue>
    {
        /// <summary>
        /// Takes a stream of prefixes and an identifier, and returns a stream containing the count for each prefix of the number of extensions.
        /// </summary>
        /// <param name="stream">A stream of prefixes</param>
        /// <param name="identifier">An integer identifier</param>
        /// <returns>A stream containing a count for each prefix, with the identifier embedded</returns>
        Stream<ExtensionProposal<TPrefix>, Epoch> CountExtensions(Stream<TPrefix, Epoch> stream, int identifier);

        /// <summary>
        /// Produces the extensions for supplied prefixes whose identifier matches that supplied as input.
        /// </summary>
        /// <param name="stream">A stream of prefixes pair with identifiers</param>
        /// <param name="identifier">An identifier to use to filter the prefixes</param>
        /// <returns>A stream of extensions for each prefixes pair with the correct identifier</returns>
        Stream<Pair<TPrefix, TValue[]>, Epoch> ExtendPrefixes(Stream<ExtensionProposal<TPrefix>, Epoch> stream, int identifier);

        /// <summary>
        /// Intersects a stream of proposed extensions with those that would be proposed by this extender.
        /// </summary>
        /// <param name="stream">A stream of extensions</param>
        /// <returns>A stream of those extensions both in the input and proposable by this extender</returns>
        Stream<Pair<TPrefix, TValue[]>, Epoch> ValidateExtensions(Stream<Pair<TPrefix, TValue[]>, Epoch> stream);
    }

    /// <summary>
    /// A triple of prefix, relation index, and count.
    /// </summary>
    /// <typeparam name="TPrefix">The type of the prefix</typeparam>
    public struct ExtensionProposal<TPrefix>
    {
        /// <summary>
        /// The prefix.
        /// </summary>
        public readonly TPrefix Record;

        /// <summary>
        /// The associated index.
        /// </summary>
        public readonly Int32 Index;

        /// <summary>
        /// The count of the number of extensions of the prefix.
        /// </summary>
        public readonly Int64 Count;

        public ExtensionProposal(int index, TPrefix record, Int64 count) { this.Index = index; this.Record = record; this.Count = count; }

        public override string ToString()
        {
            return string.Format("[{0}, {1}, {2}]", this.Record, this.Index, this.Count);
        }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TValue"></typeparam>
    /// <typeparam name="TRecord"></typeparam>
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
            using (var compuation = controller.NewComputation())
            {
                var stream = compuation.NewInput(relation.NewDataSource())
                                       .Select(x => keySelector(x).PairWith(valueSelector(x)));

                result = new InterGraphDataSink<Fragment>(BuildDenseIndex(stream));
                compuation.Activate();
                compuation.Join();
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

        private static Stream<Fragment, Epoch> BuildDenseIndex(Stream<Pair<int, TValue>, Epoch> stream)
        {
            return stream.NewUnaryStage((i, s) => new DenseIndexBuilder(i, s), x => x.First, null, "IndexBuilder");
        }

        private class DenseIndexBuilder : UnaryVertex<Pair<int, TValue>, Fragment, Epoch>
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

            public DenseIndexBuilder(int index, Stage<Epoch> stage) : base(index, stage) { }
        }

        private class Fragment
        {
            private struct LinkedListNode<T>
            {
                public T Data;
                public int Next;

                public LinkedListNode(T data) { this.Data = data; this.Next = -1; }
                public LinkedListNode(T data, int next) { this.Data = data; this.Next = next; }
            }

            private Pair<int, int>[] OffsetCounts = new Pair<int, int>[] { };
            private TValue[] Values;
            private int Parts;

            private Dictionary<int, int> tempDict = new Dictionary<int, int>();
            private HashSet<Pair<int, TValue>> tempHash = new HashSet<Pair<int, TValue>>();
            private List<LinkedListNode<TValue>> tempList = new List<LinkedListNode<TValue>>();

            internal void AddRecords(Pair<int, TValue>[] records, int count)
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

            internal Fragment Build(int parts)
            {
                this.Parts = parts;

                this.OffsetCounts = new Pair<int, int>[(this.tempDict.Keys.Max() / this.Parts) + 1];

                this.Values = new TValue[tempList.Count];
                var cursor = 0;

                foreach (var key in tempDict.Keys)
                {
                    var start = cursor;

                    for (int next = tempDict[key]; next != -1; next = tempList[next].Next)
                    {
                        this.Values[cursor++] = tempList[next].Data;
                    }

                    this.OffsetCounts[key / this.Parts] = start.PairWith(cursor - start);
                }

                this.tempDict = null;
                this.tempList = null;
                this.tempHash = null;

                for (int i = 0; i < this.OffsetCounts.Length; i++)
                {
                    Array.Sort(this.Values, this.OffsetCounts[i].First, this.OffsetCounts[i].Second);
                }

                return this;
            }

            internal Int64 Count(int prefix)
            {
                var localName = prefix / this.Parts;
                if (localName < this.OffsetCounts.Length)
                    return this.OffsetCounts[localName].Second; 
                else
                    return 0;
            }

            internal ArraySegment<TValue> Extend(int prefix)
            {
                var localName = prefix / this.Parts;
                if (localName < this.OffsetCounts.Length)
                    return new ArraySegment<TValue>(this.Values, this.OffsetCounts[localName].First, this.OffsetCounts[localName].Second);
                else
                    return new ArraySegment<TValue>(this.Values, 0, 0);
            }

            private TValue[] tempArray = new TValue[] { };

            internal Pair<int, TValue[]> Validate(Pair<int, TValue[]> extensions)
            {
                var localName = extensions.First / this.Parts;

                if (localName < this.OffsetCounts.Length)
                {
                    var offsetCount = this.OffsetCounts[localName];

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

            internal int IntersectSortedArrays<T>(ArraySegment<T> segment1, ArraySegment<T> segment2, T[] destination) where T : IComparable<T>
            {
                int matchCount = 0;

                int cursor1 = segment1.Offset;
                int cursor2 = segment2.Offset;

                int extent1 = segment1.Offset + segment1.Count;
                int extent2 = segment2.Offset + segment2.Count;

                while (cursor1 < extent1 && cursor2 < extent2)
                {
                    var comparison = segment1.Array[cursor1].CompareTo(segment2.Array[cursor2]);
                    if (comparison == 0)
                    {
                        destination[matchCount] = segment1.Array[cursor1];
                        matchCount++;

                        cursor1++;
                        cursor2++;
                    }
                    else
                    {
                        if (comparison < 0)
                            cursor1 = AdvanceCursor(segment1.Array, cursor1, extent1, segment2.Array[cursor2]);
                        else
                            cursor2 = AdvanceCursor(segment2.Array, cursor2, extent2, segment1.Array[cursor1]);
                    }
                }

                return matchCount;
            }

            // returns the position of the first element at least as large as otherValue. 
            // If no such element exists, returns values.Offset + values.Count.
            internal int AdvanceCursor<T>(T[] values, int cursor, int extent, T otherValue) where T : IComparable<T>
            {
                int step = 1;

                while (cursor + step < extent && values[cursor + step].CompareTo(otherValue) <= 0)
                {
                    cursor = cursor + step;
                    step *= 2;
                }

                // either cursor + step is off the end of the array, or points at a value larger than otherValue
                while (step > 0)
                {
                    if (cursor + step < extent && values[cursor + step].CompareTo(otherValue) <= 0)
                        cursor = cursor + step;

                    step = step / 2;
                }

                // perhaps we didn't find the right value, but should still advance the cursor to/past the match
                if (values[cursor].CompareTo(otherValue) < 0)
                    cursor++;

                return cursor;
            }
        }

        private class Extender<TPrefix> : PrefixExtender<TPrefix, TValue>
        {
            // private readonly Stream<Index, Epoch> Index;
            private readonly InterGraphDataSink<Fragment> Index;

            private readonly Func<TPrefix, int> tupleKey;

            public Extender(InterGraphDataSink<Fragment> index, Func<TPrefix, int> tKey)
            {
                this.Index = index;
                this.tupleKey = tKey;
            }

            public Stream<ExtensionProposal<TPrefix>, Epoch> CountExtensions(Stream<TPrefix, Epoch> stream, int identifier)
            {
                return stream.ForStage.Computation.NewInput(Index.NewDataSource())
                             .IndexJoin(stream, tupleKey, (index, tuple) => new ExtensionProposal<TPrefix>(identifier, tuple, index.Count(tupleKey(tuple))));
            }

            public Stream<Pair<TPrefix, TValue[]>, Epoch> ExtendPrefixes(Stream<ExtensionProposal<TPrefix>, Epoch> stream, int identifier)
            {
                return stream.ForStage.Computation.NewInput(Index.NewDataSource())
                           .IndexJoin(stream.Where(x => x.Index == identifier)
                                            .Select(x => x.Record),
                                      tupleKey, (index, tuple) => tuple.PairWith(index.Extend(tupleKey(tuple)).ToArray()));
            }

            public Stream<Pair<TPrefix, TValue[]>, Epoch> ValidateExtensions(Stream<Pair<TPrefix, TValue[]>, Epoch> stream)
            {
                return stream.ForStage.Computation.NewInput(Index.NewDataSource())
                           .IndexJoin(stream, tuple => tupleKey(tuple.First), (index, tuple) => tuple.First.PairWith(index.Validate(tupleKey(tuple.First).PairWith(tuple.Second)).Second))
                           .Where(x => x.Second != null && x.Second.Length != 0);
            }
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

    /// <summary>
    /// These custom operators should probably be integrated in to some Naiad library. 
    /// They do relatively mundane things, including:
    /// 
    ///   IndexJoin:    Loads its first input up, and then streams over the second calling a result selector for each record.
    ///   Expand:       Takes a stream of Pair(A, B[]) to a stream of Pair(A, B) without allocating an Enumerator object.
    ///   MinCount:     Aggregates a stream (using the Min combiner) with a known number of records for each key.
    ///   
    /// </summary>
    public static class CustomOperators
    {
        #region IndexJoin operator joining indices with tuples and extensions

        public static Stream<TOutput, TTime> IndexJoin<TInput1, TInput2, TKey, TOutput, TTime>(this Stream<TInput1, TTime> stream1, Stream<TInput2, TTime> stream2, Func<TInput2, TKey> keySelector, Func<TInput1, TInput2, TOutput> resultSelector) where TTime : Time<TTime>
        {
            return stream1.NewBinaryStage(stream2, (i, s) => new IndexJoinVertex<TInput1, TInput2, TKey, TOutput, TTime>(i, s, resultSelector), null, x => keySelector(x).GetHashCode(), null, "Join");
        }

        internal class IndexJoinVertex<TInput1, TInput2, TKey, TOutput, TTime> : BinaryVertex<TInput1, TInput2, TOutput, TTime> where TTime : Time<TTime>
        {
            private TInput1 value;
            private List<TInput2> enqueued = new List<TInput2>();

            private readonly Func<TInput1, TInput2, TOutput> resultSelector;

            public override void OnReceive1(Message<TInput1, TTime> message)
            {
                var output = this.Output.GetBufferForTime(message.time);

                for (int i = 0; i < message.length; i++)
                {
                    this.value = message.payload[i];

                    if (this.enqueued != null)
                    {
                        foreach (var element in this.enqueued)
                            output.Send(resultSelector(this.value, element));
                    }

                    this.enqueued.Clear();
                    this.enqueued = null;
                }
            }

            public override void OnReceive2(Message<TInput2, TTime> message)
            {
                var output = this.Output.GetBufferForTime(message.time);
                for (int i = 0; i < message.length; i++)
                {
                    if (this.enqueued == null)
                    {
                        output.Send(resultSelector(this.value, message.payload[i]));
                    }
                    else
                    {
                        this.enqueued.Add(message.payload[i]);
                    }
                }
            }

            public IndexJoinVertex(int index, Stage<TTime> stage, Func<TInput1, TInput2, TOutput> result)
                : base(index, stage)
            {
                this.resultSelector = result;
            }
        }

        #endregion

        #region Expand operator, converting Pair<T, V[]> to Pair<T, V>[]. For some reason, different from SelectMany...

        public static Stream<Pair<T, V>, TTime> Expand<T, V, TTime>(this Stream<Pair<T, V[]>, TTime> stream)
            where TTime : Time<TTime>
        {
            return stream.NewUnaryStage((i, s) => new Expander<T, V, TTime>(i, s), null, null, "Expander");
        }

        private class Expander<T, V, TTime> : UnaryVertex<Pair<T, V[]>, Pair<T, V>, TTime>
            where TTime : Time<TTime>
        {
            public override void OnReceive(Message<Pair<T, V[]>, TTime> message)
            {
                var output = this.Output.GetBufferForTime(message.time);
                for (int ii = 0; ii < message.length; ii++)
                {
                    var record = message.payload[ii];
                    for (int i = 0; i < record.Second.Length; i++)
                        output.Send(record.First.PairWith(record.Second[i]));
                }
            }

            public Expander(int index, Stage<TTime> stage) : base(index, stage) { }
        }

        #endregion

        #region MinCount operator, for determining the relation with minimum count.

        public static Stream<ExtensionProposal<T>, Epoch> MinCount<T>(this Stream<ExtensionProposal<T>, Epoch> stream, int expected)
        {
            return stream.NewUnaryStage((i, s) => new MinCountVertex<T>(i, s, expected), x => x.Record.GetHashCode(), x => x.Record.GetHashCode(), "MinCount");
        }

        private class MinCountVertex<T> : UnaryVertex<ExtensionProposal<T>, ExtensionProposal<T>, Epoch>
        {
            private readonly int Expected;
            private readonly Dictionary<T, Pair<Int64, int>> states;

            private readonly int Bits = 8;

            private int highWaterMark = 256;

            public override void OnReceive(Message<ExtensionProposal<T>, Epoch> message)
            {
                //this.REMOVEME(message.length, message.time);

                var output = this.Output.GetBufferForTime(message.time);

                ProcessArray(message.payload, message.length, this.states, output);

#if true
                var count = states.Count;
                if (count > 2 * this.highWaterMark)
                {
                    if (count > 256)
                        Console.WriteLine("Worker {0}: Queued MinCount now at {1}", this.VertexId, count);

                    this.highWaterMark = count;
                }
                if (count < this.highWaterMark / 2)
                {
                    if (count > 256)
                        Console.WriteLine("Worker {0}: Queued MinCount now at {1}", this.VertexId, count);

                    this.highWaterMark = count;
                }

#endif


                NotifyAt(message.time);
            }

            private void ProcessArray(ExtensionProposal<T>[] array, int count, Dictionary<T, Pair<Int64, int>> dictionary, VertexOutputBufferPerTime<ExtensionProposal<T>, Epoch> output)
            {
                for (int position = 0; position < count; position++)
                {
                    var record = array[position];
                    var key = record.Record;

                    Pair<Int64, int> state;
                    if (!dictionary.TryGetValue(key, out state))
                    {
                        if (this.Expected == 1)
                            output.Send(record);
                        else
                            dictionary.Add(key, ((record.Count << this.Bits) + record.Index).PairWith(1));
                    }
                    else
                    {
                        if ((state.First >> this.Bits) > record.Count)
                            state = ((record.Count << this.Bits) + record.Index).PairWith(state.Second + 1);
                        else
                            state = state.First.PairWith(state.Second + 1);

                        if (state.Second == this.Expected)
                        {
                            dictionary.Remove(key);
                            output.Send(new ExtensionProposal<T>((int)(state.First & ((1 << this.Bits) - 1)), key, state.First >> this.Bits));
                        }
                        else
                        {
                            dictionary[key] = state;
                        }
                    }
                }
            }

            public override void OnNotify(Epoch time)
            {
            }

            public MinCountVertex(int index, Stage<Epoch> stage, int expected)
                : base(index, stage)
            {
                this.Expected = expected;
                this.states = new Dictionary<T, Pair<long, int>>();
            }
        }

        #endregion
    }
}
