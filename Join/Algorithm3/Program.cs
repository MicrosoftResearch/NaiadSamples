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

using Microsoft.Research.Naiad.Frameworks.Azure;

using Algorithm3.Indices;

namespace Algorithm3
{
    public class Program
    {
        public static IEnumerable<Pair<Int32, Int32>> ReadEdges(System.IO.Stream stream, int limit = Int32.MaxValue)
        {
            Console.WriteLine("{0}\tStarting read edges", System.DateTime.Now);

            var bytesToRead = stream.Length;
            var returned = 0;

            var threshold = 0;

            using (var reader = new System.IO.BinaryReader(stream))
            {
                while (bytesToRead > 0 && returned < limit)
                {
                    var vertex = reader.ReadInt32();
                    var degree = reader.ReadInt32();

                    for (int i = 0; i < degree; i++)
                        yield return vertex.PairWith(reader.ReadInt32());

                    bytesToRead -= 4 * (1 + 1 + degree);

                    returned += degree;

                    if (returned > threshold + 1000000)
                    {
                        Console.WriteLine("{1}\tRead {0}", returned, System.DateTime.Now);
                        threshold += 1000000;
                    }
                }
            }

            Console.WriteLine("{0}\tRead and returned {1} edges", System.DateTime.Now, returned);
        }

        public static void Main(string[] args)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            using (var controller = NewController.FromArgs(ref args))
            {
                Console.WriteLine("{0}\tController constructed", stopwatch.Elapsed);

                InterGraphDataSink<Pair<int, int>> graph;

                var useAzure = false;
                if (useAzure)
                {
                    var container = args[0];
                    var prefix = args[1];

                    #if false

                                graph = controller.NewAzureInterGraphStream(container, prefix, stream => stream.Select(x => x.ParseString()));
                    #else
                        
#if true

                        graph = controller.NewNaiadAzureInterGraphStream<Pair<Int32, Int32>>(container, prefix);

#else
                    using (var computation = controller.NewComputation())
                    {
                        //controller.SetConsoleOut(computation.DefaultBlobContainer("naiad-output"), "output-{0}.txt");
                        //controller.SetConsoleError(computation.DefaultBlobContainer("naiad-output"), "error-{0}.txt");

                        var graphContainer = computation.DefaultBlobContainer(container);

                        var data = computation.ReadFromAzureBlobs(graphContainer, prefix, stream => ReadEdges(stream, Int32.MaxValue))
                                              //.Where(x => x.First != x.Second)
                                              //.Select(x => x.First < x.Second ? x : x.Second.PairWith(x.First))
                                              //.Distinct()
                                              .WriteBinaryToAzureBlobs(graphContainer, "twitter-oriented/part-{0:000}-{1:000}.bin");

                        computation.Activate();
                        computation.Join();
                    }

                    controller.Join();

                    return;
#endif
#endif
                }
                else
                {
                    var format = args[0];
                    var parts = Int32.Parse(args[1]);

                    graph = controller.NewInterGraphStream(Enumerable.Range(0, parts), stream => stream.Distinct()
                                                                                                       .SelectMany(x => string.Format(format, x).ReadEdges()));
                }

                Console.WriteLine("{0} Graph data loaded.", stopwatch.Elapsed);

                // normalize the graph to remove self-loops, point from smaller id to larger, and remove duplicates.
#if true          
                graph = controller.NewInterGraphStream(graph, edges => edges.Where(x => x.First != x.Second)
                                                                            .Select(x => x.First < x.Second ? x : x.Second.PairWith(x.First))
                                                                          //.GroupBy(x => true, (k, l) => l.RandomlyRename())
                                                                            .Distinct());
#endif

                // symmetrize the graph, just because...
                // graph = controller.NewInterGraphStream(graph, edges => edges.Select(x => x.Second.PairWith(x.First)).Concat(edges));

                Console.WriteLine("{0} Graph data normalized", stopwatch.Elapsed);

                // re-orient graph edges from lower undirected degree to larger undirected degree.
                // graph = controller.NewInterGraphStream(graph, edges => edges.GroupBy(x => true, (k,l) => l.RenameByDegree()));
                // graph = controller.NewInterGraphStream(graph, edges => edges.OrientFromLowToHighDegree());

                Console.WriteLine("{0} Graph data oriented from low to high degree", stopwatch.Elapsed);

                // we build two indices, one keyed on "no" attributes and one keyed on the first.
                var emptyKeyIndex = graph.ToEmptyIndex(controller, x => x.First);
                var denseKeyIndex = graph.ToDenseKeyIndex(controller, x => x.First, x => x.Second);

                graph.Seal();   // perhaps release the memory associated with graph. probably better to make it disposable.

                Console.WriteLine("{0} Relation prefix indices built", stopwatch.Elapsed);
                Console.WriteLine("{0} Starting query construction", stopwatch.Elapsed);

                using (var computation = controller.NewComputation())
                {
                    // we seed the computation with a single "true" record.
                    var queryseed = new BatchedDataSource<bool>();

                    var triangles = computation.NewInput(queryseed).Distinct()
                                               .Triangles(emptyKeyIndex, denseKeyIndex);

                    // optional things to comment/uncomment, based on what sort of output we would like to see.
                    // triangles.Where(x => false).Subscribe();
                    // triangles.Expand().Where(x => { Console.WriteLine("{0} Triangle observed: {1}", stopwatch.Elapsed, x); return true; });
                    triangles.Select(x => x.Second.Length).Aggregate(x => true, y => y, (x, y) => x + y, (k, sum) => sum, true).Subscribe(x => { foreach (var y in x) Console.WriteLine("Triangles: {0}", y); });

                    queryseed.OnNext();

                    computation.Activate();
                    computation.Sync(0);

                    Console.WriteLine("{0} Synchronized", stopwatch.Elapsed);
                    System.Threading.Thread.Sleep(1000);
                    Console.WriteLine("{0} Starting query execution", stopwatch.Elapsed);

                    queryseed.OnCompleted(true);

                    computation.Join();
                }

                controller.Join();
            }

            Console.WriteLine("{0} All triangles listed", stopwatch.Elapsed);
            Console.Out.Close();
        }
    }


    public static class ExtensionMethods
    {
        public static T AllReduce<T>(this Controller controller, T value, Func<T, T, T> reducer)
        {
            var result = new T[1];

            using (var computation = controller.NewComputation())
            {
                var input = new T[] { value }.AsNaiadStream(computation)
                                             .Aggregate(x => true, x => x, reducer, (b, x) => x)
                                             .SelectMany(x => Enumerable.Range(0, controller.Configuration.Processes)
                                                                        .Select(i => x.PairWith(i)))
                                             .PartitionBy(x => x.Second * controller.Configuration.WorkerCount)
                                             .Subscribe((i, j, x) => { foreach (var y in x) result[0] = y.First; });

                computation.Activate();
                computation.Join();
            }

            return result[0];
        }

        /// <summary>
        /// Enumerates triangles as triples (a, b, array of c), where each element of the list makes a triangle with edge (a,b).
        /// </summary>
        /// <param name="source">An empty bool stream off of which to base the computation; introducing true starts the computation.</param>
        /// <param name="emptyIndex">The un-keyed index built off of the graph.</param>
        /// <param name="denseIndex">The int-keyed index built off of the graph.</param>
        /// <returns>Stream of triangles represented as triples (a, b, cs) of values c completing triangles (a,b).</returns>
        public static Stream<Pair<Pair<int, int>, int[]>, Epoch> Triangles(this Stream<bool, Epoch> source, 
                                                                           Indices.EmptyKeyIndex<int, Pair<int, int>> emptyIndex, 
                                                                           DenseIntKeyIndex<Pair<int, int>> denseIndex)
        { 
            // prefix extenders take prefixes of various types and propose / validate new values.
            // each of these extender collections are of prefixes of length 0, 1, 2, respectively.
            // the length 1 extender is cheating a bit; it does not validate against G(b, *).
            var prefixExtenders0 = new[] { emptyIndex.CreateExtender<bool>() };
            var prefixExtenders1 = new[] { denseIndex.CreateExtender((int xA) => xA) };
            var prefixExtenders2 = new[] { denseIndex.CreateExtender((Pair<int, int> xAB) => xAB.First),
                                           denseIndex.CreateExtender((Pair<int, int> xAB) => xAB.Second) };

            // add new variables one layer at a time. we start with a bool, but discard it asap.
            var triangles = source.GenericJoinLayer(prefixExtenders0).Expand().Select(x => x.Second)
                                  .GenericJoinLayer(prefixExtenders1).Expand()
                                  .GenericJoinLayer(prefixExtenders2);

            return triangles;
        }

        public static Stream<Pair<Pair<Pair<int, int>, int>, int>, Epoch> Rectangles(this Stream<bool, Epoch> source,
                                                                                        Indices.EmptyKeyIndex<int, Pair<int, int>> emptyIndex,
                                                                                        DenseIntKeyIndex<Pair<int, int>> denseIndex)
        {
            // prefix extenders take prefixes of various types and propose / validate new values.
            var prefixExtenders0 = new[] { emptyIndex.CreateExtender<bool>() };
            var prefixExtenders1 = new[] { denseIndex.CreateExtender((int xA) => xA) };
            var prefixExtenders2 = new[] { denseIndex.CreateExtender((Pair<int, int> xAB) => xAB.First) };
            var prefixExtenders3 = new[] { denseIndex.CreateExtender((Pair<Pair<int, int>, int> xABC) => xABC.First.Second), 
                                            denseIndex.CreateExtender((Pair<Pair<int, int>, int> xABC) => xABC.Second) };

            // add new variables one layer at a time. we start with a bool, but discard it asap.
            var rectangles = source.GenericJoinLayer(prefixExtenders0).Expand().Select(x => x.Second)
                                    .GenericJoinLayer(prefixExtenders1).Expand()
                                    .GenericJoinLayer(prefixExtenders2).Expand().Where(x => x.First.Second != x.Second)
                                    .GenericJoinLayer(prefixExtenders3).Expand().Where(x => x.First.First.First != x.Second);

            return rectangles;
        }

        public static Stream<Pair<Pair<Pair<int, int>, int>, int>, Epoch> Flags(this Stream<bool, Epoch> source,
                                                                                     Indices.EmptyKeyIndex<int, Pair<int, int>> emptyIndex,
                                                                                     DenseIntKeyIndex<Pair<int, int>> denseIndex)
        {
            // prefix extenders take prefixes of various types and propose / validate new values.
            var prefixExtenders0 = new[] { emptyIndex.CreateExtender<bool>() };
            var prefixExtenders1 = new[] { denseIndex.CreateExtender((int xA) => xA) };
            var prefixExtenders2 = new[] { denseIndex.CreateExtender((Pair<int, int> xAB) => xAB.First), 
                                           denseIndex.CreateExtender((Pair<int, int> xAB) => xAB.Second) };
            var prefixExtenders3 = new[] { denseIndex.CreateExtender((Pair<Pair<int, int>, int> xABC) => xABC.First.Second), 
                                           denseIndex.CreateExtender((Pair<Pair<int, int>, int> xABC) => xABC.Second) };

            // add new variables one layer at a time. we start with a bool, but discard it asap.
            var flags = source.GenericJoinLayer(prefixExtenders0).Expand().Select(x => x.Second)
                              .GenericJoinLayer(prefixExtenders1).Expand()
                              .GenericJoinLayer(prefixExtenders2).Expand()
                              .GenericJoinLayer(prefixExtenders3).Expand().Where(x => x.First.First.First != x.Second);

            return flags;
        }

        /// <summary>
        /// One level of GenericJoin, taking functions acting on the stream of candidates as if they were the relations.
        /// </summary>
        /// <typeparam name="V">The new value type</typeparam>
        /// <typeparam name="T">The current tuple type</typeparam>
        /// <param name="Candidates">A stream of candidates of type T</param>
        /// <param name="Relations">Representatives of each relation</param>
        /// <returns>A stream of Pair(T, V[]) corresponding to extensions of Candidates validated by all relations.</returns>
        public static Stream<Pair<T, V[]>, Epoch> GenericJoinLayer<V, T>(this Stream<T, Epoch> Candidates,
                                                                            PrefixExtender<T, V>[] Relations)
            where V : IComparable<V>
        {
            // count the extensions in each relation, maintain the smallest.
            var Smallest = Relations[0].CountExtensions(Candidates, 0);
            foreach (var index in Enumerable.Range(1, Relations.Length - 1))
                Smallest = Relations[index].ImproveExtensions(Smallest, index);

            // propose extensions for each relation, filter by other relations.
            var Proposals = new Stream<Pair<T, V[]>, Epoch>[Relations.Length];
            foreach (var index in Enumerable.Range(0, Relations.Length))
            {
                Proposals[index] = Relations[index].ExtendPrefixes(Smallest, index);
                foreach (var otherIndex in Enumerable.Range(0, Relations.Length))
                    if (otherIndex != index)
                        Proposals[index] = Relations[otherIndex].ValidateExtensions(Proposals[index]);
            }

            // return all proposals which passed all filtering steps.
            return Proposals.Aggregate((x, y) => x.Concat(y));
        }

        #region InterGraphDataSink construction / manipulation extension methods

        public static InterGraphDataSink<TOutput> NewInterGraphStream<TRecord, TOutput>(this Controller controller, InterGraphDataSink<TRecord> source, Func<Stream<TRecord, Epoch>, Stream<TOutput, Epoch>> transformation)
        {
            InterGraphDataSink<TOutput> result;
            using (var computation = controller.NewComputation())
            {
                result = new InterGraphDataSink<TOutput>(transformation(computation.NewInput(source.NewDataSource())));

                computation.Activate();
                computation.Join();
            }

            return result;
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

        public static InterGraphDataSink<TOutput> NewAzureInterGraphStream<TOutput>(this Controller controller, string containerName, string prefix, Func<Stream<string, Epoch>, Stream<TOutput, Epoch>> transformation)
        {
            InterGraphDataSink<TOutput> result;
            using (var computation = controller.NewComputation())
            {
                controller.SetConsoleOut(computation.DefaultBlobContainer("naiad-output"), "output-{0}.txt");
                controller.SetConsoleError(computation.DefaultBlobContainer("naiad-output"), "error-{0}.txt");

                var source = computation.ReadTextFromAzureBlobs(computation.DefaultBlobContainer(containerName), prefix);

                result = new InterGraphDataSink<TOutput>(transformation(source));

                computation.Activate();
                computation.Join();
            }

            return result;
        }

        public static InterGraphDataSink<TOutput> NewBinaryAzureInterGraphStream<TInput, TOutput>(this Controller controller, string containerName, string prefix, Func<System.IO.Stream, IEnumerable<TInput>> transformation, Func<Stream<TInput, Epoch>, Stream<TOutput, Epoch>> trans)
        {
            InterGraphDataSink<TOutput> result;
            using (var computation = controller.NewComputation())
            {
                var graphContainer = computation.DefaultBlobContainer(containerName);

                var data = computation.ReadFromAzureBlobs(graphContainer, prefix, transformation);

                result = new InterGraphDataSink<TOutput>(trans(data));

                computation.Activate();
                computation.Join();
            }

            return result;
        }

        public static InterGraphDataSink<TOutput> NewNaiadAzureInterGraphStream<TOutput>(this Controller controller, string containerName, string prefix)
        {
            InterGraphDataSink<TOutput> result;
            using (var computation = controller.NewComputation())
            {
                var graphContainer = computation.DefaultBlobContainer(containerName);

                var data = computation.ReadBinaryFromAzureBlobs<TOutput>(graphContainer, prefix);

                result = new InterGraphDataSink<TOutput>(data);

                computation.Activate();
                computation.Join();
            }

            return result;
        }

        #endregion 

        public static Pair<int, int> ParseString(this string input)
        {
            var values = input.Split();

            var result1 = 0;
            var result2 = 0;

            if (!Int32.TryParse(values[0], out result1))
                Console.WriteLine("Failed to parse: {0}", values[0]);
            if (!Int32.TryParse(values[1], out result2))
                Console.WriteLine("Failed to parse: {0}", values[1]);

            return result1.PairWith(result2);
        }

        static public IEnumerable<Pair<int, int>> ReadEdges(this string filename)
        {
            var bytes = new byte[8]; // temp storage for bytes
            var ints = new int[2];   // temp storage for ints

            Console.WriteLine("Reading file {0}", filename);

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
            return cliques.GenericJoinLayer(selectors.Select(selector => index.CreateExtender(selector)).ToArray()).Expand();
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

        static public IEnumerable<Pair<int, int>> RandomlyRename(this IEnumerable<Pair<int, int>> edges)
        {
            var graph = edges.ToArray();

            var nodes = graph.Max(edge => Math.Max(edge.First, edge.Second)) + 1;

            var random = new Random(0);
            var names = Enumerable.Range(0, nodes).ToArray();

            var positions = names.Select(i => random.NextDouble())
                                 .ToArray();

            Array.Sort(positions, names);
            var newNames = new int[nodes];

            for (int i = 0; i < names.Length; i++)
                newNames[names[i]] = i;

            foreach (var edge in graph)
                yield return newNames[edge.First].PairWith(newNames[edge.Second]);
        }

        static public IEnumerable<Pair<int, int>> RenameByDegree(this IEnumerable<Pair<int, int>> edges)
        {
            var graph = edges.ToArray();

            var nodes = graph.Max(edge => Math.Max(edge.First, edge.Second)) + 1;

            var degrs = new Int32[nodes];
            foreach (var edge in graph)
            {
                degrs[edge.First]++;
                degrs[edge.Second]++;
            }

            foreach (var edge in graph)
            {
                if (degrs[edge.First] > degrs[edge.Second])
                    yield return edge.Second.PairWith(edge.First);
                else
                    yield return edge;
            }
        }

        static public void RenameTwitterByDegree(this IEnumerable<Pair<int, int>> edges, string filename)
        {
            var nodes = 65000000;

            var degrs = new Int32[nodes];
            foreach (var edge in edges)
            {
                degrs[edge.First]++;
                degrs[edge.Second]++;
            }

            var outdg = new Int32[nodes + 1];
            foreach (var edge in edges.OrientBy(degrs))
                outdg[edge.First]++;

            for (int i = 1; i < outdg.Length; i++)
                outdg[i] += outdg[i - 1];

            var values = new Int32[outdg[outdg.Length - 1]];

            for (int i = outdg.Length - 1; i > 0; i--)
                outdg[i] = outdg[i - 1];

            outdg[0] = 0;

            foreach (var edge in edges.OrientBy(degrs))
                values[outdg[edge.First]++] = edge.Second;

            for (int i = outdg.Length - 1; i > 0; i--)
                outdg[i] = outdg[i - 1];

            outdg[0] = 0;

            using (var writer = new System.IO.BinaryWriter(System.IO.File.OpenWrite(filename)))
            {
                for (int i = 0; i < outdg.Length - 1; i++)
                {
                    if (outdg[i + 1] > outdg[i])
                    {
                        Array.Sort(values, outdg[i], outdg[i + 1] - outdg[i]);

                        var degree = 1;
                        for (int j = outdg[i] + 1; j < outdg[i + 1]; j++)
                            if (values[j] > values[j - 1])
                                degree++;

                        writer.Write(i);
                        writer.Write(degree);

                        writer.Write(values[outdg[i]]);
                        for (int j = outdg[i] + 1; j < outdg[i + 1]; j++)
                            if (values[j] > values[j - 1])
                                writer.Write(values[j]);
                    }
                }
            }
        }

        public static IEnumerable<Pair<int, int>> OrientBy(this IEnumerable<Pair<int, int>> edges, int[] values)
        {
            foreach (var edge in edges)
            {
                var result = Math.Min(edge.First, edge.Second).PairWith(Math.Max(edge.First, edge.Second));
                if (values[edge.First] < values[edge.Second])
                    result = edge;
                else
                    result = edge.Second.PairWith(edge.First);

                yield return result;
            }
        }

        public static Stream<Pair<int, int>, Epoch> OrientFromLowToHighDegree(this Stream<Pair<int, int>, Epoch> graph)
        {
            var edges = graph.Select(x => new Edge(new Node(x.First), new Node(x.Second)));

            var degrees = edges.Select(x => x.source)
                               .Concat(edges.Select(x => x.target))
                               .CountNodes();

            return edges.Select(x => x.source.WithValue(x.target))
                        .NodeJoin(degrees, (target, degree) => target.WithValue(degree))
                        .Select(x => x.value.node.WithValue(x.node.WithValue(x.value.value)))   // (src, (dst, sdeg)) => (dst, (src, sdeg))
                        .NodeJoin(degrees, (sdeg, degree) => sdeg.node.WithValue(sdeg.value.PairWith(degree)))
                        .Select(x => x.value.value.First == x.value.value.Second ? (x.node.index < x.value.node.index ? new Edge(x.node, x.value.node)
                                                                                                                      : new Edge(x.value.node, x.node))
                                                                                 : x.value.value.First < x.value.value.Second ? new Edge(x.value.node, x.node)
                                                                                                                              : new Edge(x.node, x.value.node))
                        .Select(x => x.source.index.PairWith(x.target.index));
        }

        public static Stream<Pair<int, long>, Epoch> AssessWork(this Stream<Pair<int, int>, Epoch> graph)
        {
            var edges = graph.Select(x => new Edge(new Node(x.First), new Node(x.Second)));

            var degrees = edges.Select(x => x.source)
                               .Concat(edges.Select(x => x.target))
                               .CountNodes();

            var vals = edges.Select(x => x.source.WithValue(x.target))
                        .NodeJoin(degrees, (target, degree) => target.WithValue(degree))
                        .Select(x => x.value.node.WithValue(x.node.WithValue(x.value.value)))   // (src, (dst, sdeg)) => (dst, (src, sdeg))
                        .NodeJoin(degrees, (sdeg, degree) => sdeg.node.WithValue(sdeg.value.PairWith(degree)))  // (dst, (src, (sdeg, ddeg))
                        .Select(x => x.value.value.First == x.value.value.Second ? (x.node.index < x.value.node.index ? x.node.WithValue(x.value.value.First)
                                                                                                                      : x.value.node.WithValue(x.value.value.Second))
                                                                                 : x.value.value.First < x.value.value.Second ? x.value.node.WithValue(x.value.value.Second)
                                                                                                                              : x.node.WithValue(x.value.value.First));

            return vals.NodeAggregate((x, y) => x + y)
                       .Max(x => 0, x => x.value)
                       .Where(x => { Console.WriteLine("Maximum per-vertex work: " + x.Second); return true; });
        }
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
        /// Takes a stream of extension proposals and replaces any which are improved by this extender.
        /// </summary>
        /// <param name="stream">A stream of extensions</param>
        /// <param name="identifier">An identifier to use for the extender</param>
        /// <returns>A stream of extension proposals reflecting the smallest from stream and the extender</returns>
        Stream<ExtensionProposal<TPrefix>, Epoch> ImproveExtensions(Stream<ExtensionProposal<TPrefix>, Epoch> stream, int identifier);

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
        public TPrefix Record;

        /// <summary>
        /// The associated index.
        /// </summary>
        public Int32 Index;

        /// <summary>
        /// The count of the number of extensions of the prefix.
        /// </summary>
        public Int64 Count;

        public ExtensionProposal(int index, TPrefix record, Int64 count) { this.Index = index; this.Record = record; this.Count = count; }

        public override string ToString()
        {
            return string.Format("[{0}, {1}, {2}]", this.Record, this.Index, this.Count);
        }
    }



    /// <summary>
    /// These custom operators should probably be integrated in to some Naiad library. 
    /// They do relatively mundane things, including:
    /// 
    ///   IndexJoin:    Loads its first input up, and then streams over the second calling a result selector for each record.
    ///   Expand:       Takes a stream of Pair(A, B[]) to a stream of Pair(A, B) without allocating an Enumerator object.
    ///   
    /// </summary>
    public static class CustomOperators
    {
        #region IndexJoin operator joining indices with tuples and extensions

        public static Stream<TOutput, TTime> IndexJoin<TInput1, TInput2, TKey, TOutput, TTime>(this Stream<TInput1, TTime> stream1, 
                                                                                                    Stream<TInput2, TTime> stream2, 
                                                                                                    Func<TInput2, TKey> keySelector, 
                                                                                                    Func<TInput1, TInput2, TOutput> resultSelector) 
            where TTime : Time<TTime>
        {
            return stream1.NewBinaryStage(stream2, (i, s) => new IndexJoinVertex<TInput1, TInput2, TKey, TOutput, TTime>(i, s, resultSelector), null, x => keySelector(x).GetHashCode(), null, "Join");
        }

        internal class IndexJoinVertex<TInput1, TInput2, TKey, TOutput, TTime> : BinaryVertex<TInput1, TInput2, TOutput, TTime> where TTime : Time<TTime>
        {
            private TInput1 value;

            private readonly Func<TInput1, TInput2, TOutput> resultSelector;

            public override void OnReceive1(Message<TInput1, TTime> message)
            {
                for (int i = 0; i < message.length; i++)
                    this.value = message.payload[i];
            }

            public override void OnReceive2(Message<TInput2, TTime> message)
            {
                if (this.value == null)
                    throw new Exception("IndexJoin received data before receiving an index");

                var output = this.Output.GetBufferForTime(message.time);
                for (int i = 0; i < message.length; i++)
                    output.Send(resultSelector(this.value, message.payload[i]));
            }

            public IndexJoinVertex(int index, Stage<TTime> stage, Func<TInput1, TInput2, TOutput> result)
                : base(index, stage)
            {
                this.resultSelector = result;
            }
        }

        public static Stream<TOutput, Epoch> IndexJoin<TInput1, TInput2, TKey, TOutput>(this Stream<TInput1, Epoch> stream1,
                                                                                             Stream<TInput2, Epoch> stream2,
                                                                                             Func<TInput2, TKey> keySelector,
                                                                                             Action<TInput1, TInput2[], int, VertexOutputBufferPerTime<TOutput, Epoch>> action)
        {
            return stream1.NewBinaryStage(stream2, (i, s) => new IndexJoinVertex2<TInput1, TInput2, TKey, TOutput>(i, s, action), null, x => keySelector(x).GetHashCode(), null, "Join");
        }

        internal class IndexJoinVertex2<TInput1, TInput2, TKey, TOutput> : BinaryVertex<TInput1, TInput2, TOutput, Epoch>
        {
            private TInput1 value;

            private readonly Action<TInput1, TInput2[], int, VertexOutputBufferPerTime<TOutput, Epoch>> action;

            public override void OnReceive1(Message<TInput1, Epoch> message)
            {
                for (int i = 0; i < message.length; i++)
                    this.value = message.payload[i];
            }

            public override void OnReceive2(Message<TInput2, Epoch> message)
            {
                if (this.value == null)
                    throw new Exception("IndexJoin received data before receiving an index");

                this.action(this.value, message.payload, message.length, this.Output.GetBufferForTime(message.time));
            }

            public IndexJoinVertex2(int index, Stage<Epoch> stage, Action<TInput1, TInput2[], int, VertexOutputBufferPerTime<TOutput, Epoch>> action)
                : base(index, stage)
            {
                this.action = action;
            }
        }

        #endregion

        #region Expand operator, converting Pair<T, V[]> to Pair<T, V>[]. For some reason, different from SelectMany...

        public static Stream<Pair<T, V>, TTime> Expand<T, V, TTime>(this Stream<Pair<T, ArraySegment<V>>, TTime> stream)
            where TTime : Time<TTime>
        {
            return stream.NewUnaryStage((i, s) => new Expander<T, V, TTime>(i, s), null, null, "Expander");
        }

        public static Stream<Pair<T, V>, TTime> Expand<T, V, TTime>(this Stream<Pair<T, V[]>, TTime> stream)
            where TTime : Time<TTime>
        {
            return stream.Select(x => x.First.PairWith(new ArraySegment<V>(x.Second)))
                         .NewUnaryStage((i, s) => new Expander<T, V, TTime>(i, s), null, null, "Expander");
        }

        private class Expander<T, V, TTime> : UnaryVertex<Pair<T, ArraySegment<V>>, Pair<T, V>, TTime>
            where TTime : Time<TTime>
        {
            public override void OnReceive(Message<Pair<T, ArraySegment<V>>, TTime> message)
            {
                var output = this.Output.GetBufferForTime(message.time);
                for (int ii = 0; ii < message.length; ii++)
                {
                    var record = message.payload[ii];
                    for (int i = 0; i < record.Second.Count; i++)
                        output.Send(record.First.PairWith(record.Second.Array[i + record.Second.Offset]));
                }
            }

            public Expander(int index, Stage<TTime> stage) : base(index, stage) { }
        }

        #endregion
    }
}
