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

#define UseLargePages

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.IO;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Frameworks.GraphLINQ;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Dataflow.PartitionBy;

using Microsoft.Research.Naiad.Dataflow.StandardVertices;
using Microsoft.Research.Naiad.Frameworks.Hdfs;

using System.Threading;

namespace COST
{
    public static class Program
    {
        #region file writing code
        public static Subscription WriteToFiles<TRecord>(this Stream<TRecord, Epoch> source, string format, Func<Stream, IObserver<TRecord>> writer)
        {
            var writers = new Dictionary<int, IObserver<TRecord>>();

            return source.Subscribe((message, workerid) =>
            {
                IObserver<TRecord> observer;

                lock (writers)
                {
                    if (!writers.ContainsKey(workerid))
                        writers.Add(workerid, writer(new FileStream(
                            string.Format(format, source.ForStage.Computation.Controller.Configuration.ProcessID, workerid),
                            FileMode.Create, FileAccess.Read)));

                    observer = writers[workerid];
                }

                for (int i = 0; i < message.length; i++)
                    observer.OnNext(message.payload[i]);
            },

            (epoch, workerid) => { },

            workerid =>
            {
                lock (writers)
                {
                    if (writers.ContainsKey(workerid))
                        writers[workerid].OnCompleted();
                };
            });

        }

        public static Subscription WriteBinaryToFiles<TRecord>(this Stream<TRecord, Epoch> source, string format)
        {
            return source.WriteToFiles(format, stream => Microsoft.Research.Naiad.Frameworks.Storage.Utils.GetNaiadWriterObserver<TRecord>(stream, source.ForStage.Computation.Controller.SerializationFormat));
        }
        #endregion

        #region file reading code
        public static IEnumerable<Edge> ReadEdges(this System.IO.Stream stream)
        {
            var ints = new int[2];
            var bytes = new byte[8];

            var bytesToRead = stream.Length;

            // read vertex name and degree
            bytesToRead -= stream.Read(bytes, 0, 8);
            Buffer.BlockCopy(bytes, 0, ints, 0, 8);

            var vertex = ints[0];
            var degree = ints[1];

            while (bytesToRead > 0)
            {
                // allocate space if needed
                if (ints.Length < degree + 2)
                {
                    ints = new int[degree + 2];
                    bytes = new byte[ints.Length * 4];
                }

                // read names of neighbors, plus next vertex and degree.
                bytesToRead -= stream.Read(bytes, 0, 4 * (degree + 2));
                Buffer.BlockCopy(bytes, 0, ints, 0, 4 * (degree + 2));

                // run the user-supplied vertex logic
                for (int i = 0; i < degree; i++)
                    yield return new Edge(new Node(vertex), new Node(ints[i]));

                vertex = ints[degree];
                degree = ints[degree + 1];
            }
        }

        public static void ConvertFromText(string input, string output)
        {
            using (var reader = new System.IO.StreamReader(input))
            {
                var vertex = -1;
                var targets = new List<int>();

                reader.ReadLine();

                using (var writer = new System.IO.BinaryWriter(System.IO.File.OpenWrite(output)))
                {
                    while (!reader.EndOfStream)
                    {
                        var line = reader.ReadLine();

                        if (!line.StartsWith("#"))
                        {
                            var fields = line.Split();

                            var source = Convert.ToInt32(fields[0]);
                            var target = Convert.ToInt32(fields[1]);

                            if (source != vertex)
                            {
                                if (targets.Count() > 0)
                                {
                                    writer.Write(vertex);
                                    writer.Write(targets.Count);
                                    for (int i = 0; i < targets.Count; i++)
                                        writer.Write(targets[i]);

                                    targets.Clear();
                                }


                                vertex = source;
                            }

                            targets.Add(target);
                        }
                    }

                    if (targets.Count > 0)
                    {
                        writer.Write(vertex);
                        writer.Write(targets.Count);
                        for (int i = 0; i < targets.Count; i++)
                            writer.Write(targets[i]);
                    }
                }
            }
        }

        public static void ConvertFromUK2007(string input, string output)
        {
            using (var reader = new System.IO.StreamReader(input))
            {
                var firstLine = reader.ReadLine().Split();

                Console.WriteLine("Nodes: {0}\tEdges: {1}", firstLine[0], firstLine[1]);

                Int64 maxNode = 0;
                Int64 edgeCount = 0;


                using (var writer = new System.IO.BinaryWriter(System.IO.File.OpenWrite(output)))
                {
                    for (int i = 1; !reader.EndOfStream; i++)
                    {
                        var neighbors = reader.ReadLine().Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

                        if (neighbors.Length > 0)
                        {
                            writer.Write(i);
                            writer.Write(neighbors.Length);
                            for (int j = 0; j < neighbors.Length; j++)
                                writer.Write(Int32.Parse(neighbors[j]));

                            edgeCount += neighbors.Length;
                        }

                        maxNode = i;
                    }
                }

                Console.WriteLine("Discovered {0} nodes, {1} edges", maxNode, edgeCount);
            }
        }
        #endregion

        static void ExecuteSingleThreaded(string[] args, string dataDir)
        {
            string ukFile = Path.Combine(dataDir, @"uk-2007-05");
            string twitterFile = Path.Combine(dataDir, @"twitter_rv.bin");
            string livejournalFile = Path.Combine(dataDir, @"livejournal.bin");

            if (args.Length < 3)
                throw new Exception("Three arguments required: system, algorithm, dataset");

            var algorithm = args[1];
            var dataset = args[2];

            #region file conversions
            if (algorithm == "convert" && dataset == "twitter")
            {
                SingleThreaded.ConvertGraph(twitterFile);
            }

            if (algorithm == "partition" && dataset == "twitter")
            {
                SingleThreaded.PartitionGraph(twitterFile, 4, (s, t) => (s & 1) + 2 * (t & 1), dataDir + @"twitter-part-{0}-of-{1}");
            }

            if (algorithm == "transpose" && dataset == "twitter")
            {
                var stopwatch = System.Diagnostics.Stopwatch.StartNew();
                SingleThreaded.TransposeGraph(dataDir + @"twitterfollowers\twitter_rv.bin", 65000000);
                Console.WriteLine(stopwatch.Elapsed);
            }
            #endregion

            #region hilbert layout
            if (algorithm == "hilbertlayout" && dataset == "twitter")
            {
                var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                uint nodes = 0;
                var names = new uint[65000000];
                for (int i = 0; i < names.Length; i++)
                    names[i] = uint.MaxValue;

                var idegrees = new int[65000000];
                var odegrees = new int[65000000];

                var edges = 0L;
                SingleThreaded.ScanGraph(twitterFile, (vertex, degree, offset, neighbors) =>
                {
                    if (names[vertex] == uint.MaxValue)
                        names[vertex] = nodes++;

                    for (int i = 0; i < degree; i++)
                        if (names[neighbors[offset + i]] == uint.MaxValue)
                            names[neighbors[offset + i]] = nodes++;

                    edges += degree;
                });

                Console.WriteLine("{2}\tNodes: {0}\tEdges: {1}", nodes, edges, stopwatch.Elapsed);

                // allocate enough space for all the edges.
                var hilbertTransformed = new uint[edges];

                var counts = new uint[1 << 20];
                SingleThreaded.ScanGraph(twitterFile, (vertex, degree, offset, neighbors) =>
                {
                    for (int i = 0; i < degree; i++)
                        counts[HilbertCurve.xy2dByte(names[vertex], names[neighbors[offset + i]]) >> 32]++;
                });

                Console.WriteLine("{0}\tHilbert regions sized", stopwatch.Elapsed);
                for (int i = 1; i < counts.Length; i++)
                    counts[i + 1] += counts[i];

                for (int i = counts.Length - 1; i > 0; i--)
                    counts[i] = counts[i] - 1;

                counts[0] = 0;

                var Trie = new BufferTrie<uint>(20, (array, offset, length) =>
                {
                    for (int i = offset; i < offset + length; i++)
                        hilbertTransformed[counts[array[i].Index]++] = array[i].Value;
                });

                for (int i = counts.Length - 1; i > 0; i--)
                    counts[i] = counts[i] - 1;

                counts[0] = 0;

                var buffer = new BufferTrie<uint>.Pair[5000000];

                SingleThreaded.ScanGraph(twitterFile, (vertex, degree, offset, neighbors) =>
                {
                    for (int i = 0; i < degree; i++)
                    {
                        var result = HilbertCurve.xy2dByte(names[vertex], names[neighbors[offset + i]]);

                        buffer[i] = new BufferTrie<uint>.Pair((int)(result >> 32), (uint)(result & 0xFFFF));
                    }

                    Trie.Insert(buffer, 0, degree);
                });

                Trie.Flush();

                Console.WriteLine("{0}\tEdges partitioned", stopwatch.Elapsed);

                using (var upper = new System.IO.BinaryWriter(System.IO.File.OpenWrite("twitter-hilbert-upper")))
                {
                    for (uint i = 0; i < counts.Length - 1; i++)
                    {
                        if (counts[i] < counts[i + 1])
                        {
                            uint x = 0, y = 0;
                            HilbertCurve.d2xyByte((i << 32), out x, out y);

                            upper.Write(x);
                            upper.Write(y);
                            upper.Write(counts[i + 1] - counts[i]);
                        }
                    }
                }

                using (var lower = new System.IO.BinaryWriter(System.IO.File.OpenWrite("twitter-hilbert-lower")))
                {
                    for (uint i = 0; i < counts.Length - 1; i++)
                    {
                        Array.Sort(hilbertTransformed, (int)counts[i], (int)(counts[i + 1] - counts[i]));

                        for (uint j = counts[i]; j < counts[i + 1]; j++)
                        {
                            uint x = 0, y = 0;
                            HilbertCurve.d2xyByte((i << 32) + hilbertTransformed[j], out x, out y);

                            lower.Write((UInt16)(x & 0xFFFF));
                            lower.Write((UInt16)(y & 0xFFFF));
                        }
                    }
                }
            }
            #endregion

            #region hilbert pagerank
            if (algorithm == "hilbertpagerank" && dataset == "livejournal")
            {
                unsafe
                {
                    var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                    var nodes = (uint)42000000;

#if UseLargePages
                    var srcRanks = LargePages.AllocateFloats(nodes);
                    var dstRanks = LargePages.AllocateFloats(nodes);
#else
                    var srcRanks = new float[nodes];
                    var dstRanks = new float[nodes];
#endif
                    for (int i = 0; i < nodes; i++)
                        srcRanks[i] = 1.0f;

                    SingleThreaded.HilbertPagerank(@"livejournal-hilbert", dstRanks, srcRanks, nodes, 0.85f);

                    Console.WriteLine(stopwatch.Elapsed);
                }

            }

            if (algorithm == "hilbertpagerank" && dataset == "twitter")
            {
                unsafe
                {
                    var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                    var nodes = (uint)42000000;

#if UseLargePages
                    var srcRanks = LargePages.AllocateFloats(nodes);
                    var dstRanks = LargePages.AllocateFloats(nodes);
#else
                    var srcRanks = new float[nodes];
                    var dstRanks = new float[nodes];
#endif
                    for (int i = 0; i < nodes; i++)
                        srcRanks[i] = 1.0f;

                    SingleThreaded.HilbertPagerank(@"twitter-hilbert", dstRanks, srcRanks, nodes, 0.85f);

                    Console.WriteLine(stopwatch.Elapsed);
                }
            }

            if (algorithm == "hilbertpagerank" && dataset == "uk-2007-05")
            {
                unsafe
                {
                    var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                    var nodes = (uint)106000000;

                    var srcRanks = LargePages.AllocateFloats(nodes);
                    var dstRanks = LargePages.AllocateFloats(nodes);

                    for (int i = 0; i < nodes; i++)
                        srcRanks[i] = 1.0f;

                    SingleThreaded.MultiHilbertPagerank(@"uk-2007-05-hilbert", dstRanks, srcRanks, nodes, 0.85f);

                    Console.WriteLine(stopwatch.Elapsed);
                }

            }
            #endregion

            #region hilbert union find
            if (algorithm == "hilbertunionfind" && dataset == "twitter")
            {
                unsafe
                {
                    var nodes = (uint)42000000;

                    var stopwatch = System.Diagnostics.Stopwatch.StartNew();
                    SingleThreaded.HilbertUnionFind2(@"twitter-hilbert", nodes);
                    Console.WriteLine(stopwatch.Elapsed);
                }
            }

            if (algorithm == "hilbertunionfind" && dataset == "uk-2007-05")
            {
                unsafe
                {
                    var nodes = (uint)106000000;

                    var stopwatch = System.Diagnostics.Stopwatch.StartNew();
                    SingleThreaded.MultiHilbertCC(@"uk-2007-05-hilbert", nodes);
                    Console.WriteLine(stopwatch.Elapsed);
                }
            }

            if (algorithm == "hilbertunionfind" && dataset == "livejournal")
            {
                unsafe
                {
                    var nodes = (uint)42000000;

                    var stopwatch = System.Diagnostics.Stopwatch.StartNew();
                    SingleThreaded.HilbertUnionFind(@"livejournal-hilbert-upper", "livejournal-hilbert-lower", nodes);
                    Console.WriteLine(stopwatch.Elapsed);
                }
            }
            #endregion

            #region page rank
            if (algorithm == "pagerank" && dataset == "uk-2007-05")
            {
                unsafe
                {
                    var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                    var nodes = (uint)106000000;

#if UseLargePages
                    var srcRanks = LargePages.AllocateFloats(nodes);
                    var dstRanks = LargePages.AllocateFloats(nodes);
#else
                    var srcRanks = new float[nodes];
                    var dstRanks = new float[nodes];
#endif
                    SingleThreaded.PageRankFromDisk(ukFile, dstRanks, srcRanks, nodes, 0.85f);

                    Console.WriteLine(stopwatch.Elapsed);
                }
            }

            if (algorithm == "pagerank" && dataset == "twitter")
            {
                unsafe
                {
                    var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                    var nodes = (uint)65000000;

#if UseLargePages
                    var srcRanks = LargePages.AllocateFloats(nodes);
                    var dstRanks = LargePages.AllocateFloats(nodes);
#else
                    var srcRanks = new float[nodes];
                    var dstRanks = new float[nodes];
#endif
                    SingleThreaded.PageRankFromDisk(twitterFile, dstRanks, srcRanks, nodes, 0.85f);

                    Console.WriteLine(stopwatch.Elapsed);
                }
            }

            if (algorithm == "pagerank" && dataset == "livejournal")
            {
                var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                unsafe
                {
                    var nodes = (uint)65000000;

#if UseLargePages
                    var srcRanks = LargePages.AllocateFloats(nodes);
                    var dstRanks = LargePages.AllocateFloats(nodes);
#else
                    var srcRanks = new float[nodes];
                    var dstRanks = new float[nodes];
#endif
                    for (int i = 0; i < 20; i++)
                    {
                        SingleThreaded.PageRankStep(livejournalFile, dstRanks, srcRanks, nodes, 0.85f);
                        Console.WriteLine("{0}\tIteration {1}", stopwatch.Elapsed, i);
                    }
                }

                Console.WriteLine(stopwatch.Elapsed);
            }
            #endregion

            #region connected components
            if (algorithm == "connectedcomponents" && dataset == "twitter")
            {
                var stopwatch = System.Diagnostics.Stopwatch.StartNew();
                SingleThreaded.ConnectedComponents(twitterFile, 65000000);
                Console.WriteLine(stopwatch.Elapsed);
            }

            if (algorithm == "connectedcomponents" && dataset == "uk-2007-05")
            {
                var stopwatch = System.Diagnostics.Stopwatch.StartNew();
                SingleThreaded.ClumsyCC(ukFile, 106000000);
                Console.WriteLine(stopwatch.Elapsed);
            }

            if (algorithm == "connectedcomponents" && dataset == "livejournal")
            {
                var stopwatch = System.Diagnostics.Stopwatch.StartNew();
                SingleThreaded.ConnectedComponents(livejournalFile, 6500000);
                Console.WriteLine(stopwatch.Elapsed);
            }
            #endregion

            #region maximal independent set
            if (algorithm == "maximalindependentset" && dataset == "twitter")
            {
                var stopwatch = System.Diagnostics.Stopwatch.StartNew();
                SingleThreaded.MaximalIndependentSet(twitterFile, 65000000);
                Console.WriteLine(stopwatch.Elapsed);
            }

            if (algorithm == "maximalindependentset" && dataset == "livejournal")
            {
                var stopwatch = System.Diagnostics.Stopwatch.StartNew();
                SingleThreaded.MaximalIndependentSet(livejournalFile, 6500000);
                Console.WriteLine(stopwatch.Elapsed);
            }
            #endregion
        }

        static void ExecuteNaiad(string[] args, string dataDir, string uriBase)
        {
            string ukFile = Path.Combine(dataDir, @"uk-2007-05");
            string twitterFile = Path.Combine(dataDir, @"twitter_rv.bin");
            string livejournalFile = Path.Combine(dataDir, @"livejournal.bin");

            var configuration = Configuration.FromArgs(ref args);

            var algorithm = args[1];
            var dataset = args[2];

            #region file partitioning
            if (algorithm == "partition" && dataset == "twitter")
            {
                var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                using (var computation = NewComputation.FromConfig(configuration))
                {
                    int parts = Int32.Parse(args[3]);
                    var format = Path.Combine(dataDir, @"twitter-part-{0}-of-" + (parts * parts).ToString());

                    computation.LoadGraph(twitterFile)
                               .Partition(parts, parts)
                               .WriteBinaryToFiles(format);

                    computation.Activate();
                    computation.Join();
                }

                Console.WriteLine(stopwatch.Elapsed);
            }

            if (algorithm == "repartition" && dataset == "twitter")
            {
                var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                using (var computation = NewComputation.FromConfig(configuration))
                {
                    int parts = Int32.Parse(args[3]);

                    computation.ReadHdfsBinaryCollection<Edge>(new Uri(uriBase + "twitter-10"))
                               .Partition(parts, parts)
                               .WriteHdfsBinary(new Uri(uriBase + "twitter-" + parts), 1024 * 1024, -1L, 100L * 1024L * 1024L * 1024L);

                    computation.Activate();
                    computation.Join();
                }

                Console.WriteLine(stopwatch.Elapsed);
            }

            if (algorithm == "compact" && dataset == "twitter")
            {
                using (var computation = NewComputation.FromConfig(configuration))
                {
                    var edges = System.IO.File.OpenRead(twitterFile)
                                              .ReadEdges()
                                              .AsNaiadStream(computation);

                    using (var renamer = new AutoRenamer<Int32>())
                    {
                        var newEdges = edges.RenameUsing(renamer, edge => edge.source)
                                            .Select(x => new Edge(x.node, x.value.target))
                                            .RenameUsing(renamer, edge => edge.target)
                                            .Select(x => new Edge(x.value.source, x.node));

                        edges = newEdges.FinishRenaming(renamer);
                    }

                    computation.Activate();
                    computation.Join();
                }
            }
            #endregion

            #region page rank
            if (algorithm == "pagerank" && dataset == "twitter")
            {
                var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                using (var computation = NewComputation.FromConfig(configuration))
                {
                    computation.OnFrontierChange += (x, y) => { Console.WriteLine(System.DateTime.Now + "\t" + string.Join(", ", y.NewFrontier)); System.GC.GetTotalMemory(true); };

                    var edges = System.IO.File.OpenRead(twitterFile)
                                              .ReadEdges()
                                              .AsNaiadStream(computation);

                    edges.PageRank(20, "twitter").Subscribe();

                    computation.Activate();
                    computation.Join();
                }

                Console.WriteLine(stopwatch.Elapsed);
            }

            if (algorithm == "pagerank" && dataset == "livejournal")
            {
                var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                using (var computation = NewComputation.FromConfig(configuration))
                {
                    computation.OnFrontierChange += (x, y) => { Console.WriteLine(System.DateTime.Now + "\t" + string.Join(", ", y.NewFrontier)); };

                    var edges = System.IO.File.OpenRead(livejournalFile)
                                              .ReadEdges()
                                              .AsNaiadStream(computation);

                    edges.PageRank(20, "livejournal").Subscribe();

                    computation.Activate();
                    computation.Join();
                }

                Console.WriteLine(stopwatch.Elapsed);
            }
            #endregion

            #region connected components
            if (algorithm == "connectedcomponents" && dataset == "uk-2007-05")
            {
                var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                using (var computation = NewComputation.FromConfig(configuration))
                {
                    var format = Path.Combine(dataDir, @"uk-2007-05-part-{0}-of-{1}");

                    var extraInput = new[] { string.Format(format, 3, 4) }.AsNaiadStream(computation)
                                                   .PartitionBy(x => 3)
                                                   .ReadGraph();

                    computation.LoadGraph(format, 3, 4)
                               .UnionFind(106000000)
                               .PartitionBy(x => 3)
                               .Concat(extraInput)
                               .UnionFind(106000000);

                    computation.Activate();
                    computation.Join();
                }

                Console.WriteLine(stopwatch.Elapsed);
            }

            if (algorithm == "connectedcomponents" && dataset == "twitter")
            {
                using (Microsoft.Research.Peloponnese.Hdfs.HdfsInstance hdfs = new Microsoft.Research.Peloponnese.Hdfs.HdfsInstance(new Uri(uriBase)))
                {
                    // HDFS needs to be initialized from the main thread before distributed use
                    bool exists = hdfs.IsFileExists("/dummy");
                }

                var readWatch = System.Diagnostics.Stopwatch.StartNew();

                using (var controller = NewController.FromConfig(configuration))
                {
                    using (var readComputation = controller.NewComputation())
                    {
                        int parts = (args.Length > 4) ? Int32.Parse(args[4]) : 1;
                        int machines = (args.Length > 5) ? Int32.Parse(args[5]) : 1;
                        int another = (args.Length > 6) ? Int32.Parse(args[6]) : 1;
                        var format = new Uri(@uriBase + "twitter-40");
                        var collection = readComputation
                            .ReadHdfsBinaryCollection<Edge>(format);

                        Stream<int[], Epoch> readStuff = null;

                        switch (args[3])
                        {
                            case "sp":
                                readStuff = collection.GroupEdgesSingleProcess(parts, parts);
                                break;

                            case "pp":
                                readStuff = collection.GroupEdgesPartsPerProcess(parts, parts, 16);
                                break;

                            case "op":
                                readStuff = collection.GroupEdgesOnePerProcess(parts, parts, 16);
                                break;

                            case "hp":
                                readStuff = collection.GroupEdgesHierarchyPerProcess(parts, machines, 16);
                                break;

                            case "hhp":
                                readStuff = collection.GroupEdgesHierarchyPerProcess(parts, machines * another, 16);
                                break;

                            default:
                                throw new ApplicationException("Grouping type must be sp, pp, op, hp or hpp");
                        }

                        var sink = new InterGraphDataSink<int[]>(readStuff);

                        readComputation.Activate();
                        readComputation.Join();

                        Console.WriteLine("Reading done: " + readWatch.Elapsed);

                        for (int i = 0; i < 20; ++i)
                        {
                            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                            using (var computation = controller.NewComputation())
                            {
                                var firstStage = computation.NewInput(sink.NewDataSource())
                                    .ReformatInts();

                                if (parts * machines * another > 1)
                                {
                                    firstStage = firstStage
                                        .UnionFindStruct(65000000, parts * machines * another, machines * another);
                                }

                                switch (args[3])
                                {
                                    case "sp":
                                        firstStage
                                            .PartitionBy(x => parts * parts)
                                            .UnionFind(65000000);
                                        break;

                                    case "pp":
                                        firstStage
                                            .PartitionBy(x => 16 * parts)
                                            .UnionFind(65000000);
                                        break;

                                    case "op":
                                        firstStage
                                            .PartitionBy(x => 16 * (parts * parts))
                                            .UnionFind(65000000);
                                        break;

                                    case "hp":
                                        if (parts * parts < 16)
                                        {
                                            firstStage
                                                .PartitionBy(x => 16 * x.destination + (parts * parts))
                                                .UnionFindStruct(65000000, 0, 0)
                                                .PartitionBy(x => 16 * (machines * machines))
                                                .UnionFind(65000000);
                                        }
                                        else
                                        {
                                            firstStage
                                                .PartitionBy(x => 16 * (x.destination + (machines * machines)))
                                                .UnionFindStruct(65000000, 0, 0)
                                                .PartitionBy(x => 16 * ((machines * machines) + (machines * machines)))
                                                .UnionFind(65000000);
                                        }
                                        break;

                                    case "hhp":
                                        firstStage
                                            .PartitionBy(x => 16 * ((x.destination / (machines * machines)) + (machines * machines * another * another)) + (x.destination % (machines * machines)))
                                            .UnionFindStruct(65000000, -machines * another, another)
                                            .PartitionBy(x => 16 * (x.destination + (another * another) + (machines * machines * another * another)))
                                            .UnionFindStruct(65000000, -another, 1)
                                            .PartitionBy(x => 16 * ((another * another) + (another * another) + (machines * machines * another * another)))
                                            .UnionFind(65000000);
                                        break;

                                    default:
                                        throw new ApplicationException("Grouping type must be sp, pp, op, hp or hhp");
                                }

                                computation.Activate();
                                computation.Join();
                            }

                            Console.WriteLine(stopwatch.Elapsed);
                        }
                    }

                    controller.Join();
                }
            }

            if (algorithm == "hashtablecc" && dataset == "twitter")
            {
                using (Microsoft.Research.Peloponnese.Hdfs.HdfsInstance hdfs = new Microsoft.Research.Peloponnese.Hdfs.HdfsInstance(new Uri(uriBase)))
                {
                    // HDFS needs to be initialized from the main thread before distributed use
                    bool exists = hdfs.IsFileExists("/dummy");
                }

                var readWatch = System.Diagnostics.Stopwatch.StartNew();

                using (var controller = NewController.FromConfig(configuration))
                {
                    using (var readComputation = controller.NewComputation())
                    {
                        int parts = (args.Length > 4) ? Int32.Parse(args[4]) : 1;
                        int machines = (args.Length > 5) ? Int32.Parse(args[5]) : 1;
                        int another = (args.Length > 6) ? Int32.Parse(args[6]) : 1;
                        var format = new Uri(@uriBase + "twitter-40");
                        var collection = readComputation
                            .ReadHdfsBinaryCollection<Edge>(format);

                        Stream<int[], Epoch> readStuff = null;

                        switch (args[3])
                        {
                            case "sp":
                                readStuff = collection.GroupEdgesSingleProcess(parts, parts);
                                break;

                            case "pp":
                                readStuff = collection.GroupEdgesPartsPerProcess(parts, parts, 16);
                                break;

                            case "op":
                                readStuff = collection.GroupEdgesOnePerProcess(parts, parts, 16);
                                break;

                            case "hp":
                                readStuff = collection.GroupEdgesHierarchyPerProcess(parts, machines, 16);
                                break;

                            case "hhp":
                                readStuff = collection.GroupEdgesHierarchyPerProcess(parts, machines * another, 16);
                                break;

                            default:
                                throw new ApplicationException("Grouping type must be sp, pp, op, hp or hpp");
                        }

                        var sink = new InterGraphDataSink<int[]>(readStuff);

                        readComputation.Activate();
                        readComputation.Join();

                        Console.WriteLine("Reading done: " + readWatch.Elapsed);

                        for (int i = 0; i < 20; ++i)
                        {
                            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                            using (var computation = controller.NewComputation())
                            {
                                var firstStage = computation.NewInput(sink.NewDataSource())
                                           .ReformatInts()
                                           .UnionFindHashTable(65000000, parts * machines * another, machines * another);

                                switch (args[3])
                                {
                                    case "sp":
                                        firstStage
                                            .PartitionBy(x => parts * parts)
                                            .UnionFind(65000000);
                                        break;

                                    case "pp":
                                        firstStage
                                            .PartitionBy(x => 16 * parts)
                                            .UnionFind(65000000);
                                        break;

                                    case "op":
                                        firstStage
                                            .PartitionBy(x => 16 * (parts * parts))
                                            .UnionFind(65000000);
                                        break;

                                    case "hp":
                                        if (parts * parts < 16)
                                        {
                                            firstStage
                                                .PartitionBy(x => 16 * x.destination + (parts * parts))
                                                .UnionFindStruct(65000000, 0, 0)
                                                .PartitionBy(x => 16 * (machines * machines))
                                                .UnionFind(65000000);
                                        }
                                        else
                                        {
                                            firstStage
                                                .PartitionBy(x => 16 * (x.destination + (machines * machines)))
                                                .UnionFindStruct(65000000, 0, 0)
                                                .PartitionBy(x => 16 * ((machines * machines) + (machines * machines)))
                                                .UnionFind(65000000);
                                        }
                                        break;

                                    case "hhp":
                                        firstStage
                                            .PartitionBy(x => 16 * ((x.destination / (machines * machines)) + (machines * machines * another * another)) + (x.destination % (machines * machines)))
                                            .UnionFindStruct(65000000, -machines * another, another)
                                            .PartitionBy(x => 16 * (x.destination + (another * another) + (machines * machines * another * another)))
                                            .UnionFindStruct(65000000, -another, 1)
                                            .PartitionBy(x => 16 * ((another * another) + (another * another) + (machines * machines * another * another)))
                                            .UnionFind(65000000);
                                        break;

                                    default:
                                        throw new ApplicationException("Grouping type must be sp, pp, op, hp or hpp");
                                }

                                computation.Activate();
                                computation.Join();
                            }

                            Console.WriteLine(stopwatch.Elapsed);
                        }
                    }

                    controller.Join();
                }
            }

            if (algorithm == "hashtableonlycc" && dataset == "twitter")
            {
                using (Microsoft.Research.Peloponnese.Hdfs.HdfsInstance hdfs = new Microsoft.Research.Peloponnese.Hdfs.HdfsInstance(new Uri(uriBase)))
                {
                    // HDFS needs to be initialized from the main thread before distributed use
                    bool exists = hdfs.IsFileExists("/dummy");
                }

                var readWatch = System.Diagnostics.Stopwatch.StartNew();

                using (var controller = NewController.FromConfig(configuration))
                {
                    using (var readComputation = controller.NewComputation())
                    {
                        int parts = (args.Length > 4) ? Int32.Parse(args[4]) : 1;
                        int machines = (args.Length > 5) ? Int32.Parse(args[5]) : 1;
                        int another = (args.Length > 6) ? Int32.Parse(args[6]) : 1;
                        var format = new Uri(@uriBase + "twitter-40");
                        var collection = readComputation
                            .ReadHdfsBinaryCollection<Edge>(format);

                        Stream<int[], Epoch> readStuff = null;

                        switch (args[3])
                        {
                            case "sp":
                                readStuff = collection.GroupEdgesSingleProcess(parts, parts);
                                break;

                            case "pp":
                                readStuff = collection.GroupEdgesPartsPerProcess(parts, parts, 16);
                                break;

                            case "op":
                                readStuff = collection.GroupEdgesOnePerProcess(parts, parts, 16);
                                break;

                            case "hp":
                                readStuff = collection.GroupEdgesHierarchyPerProcess(parts, machines, 16);
                                break;

                            case "hhp":
                                readStuff = collection.GroupEdgesHierarchyPerProcess(parts, machines * another, 16);
                                break;

                            default:
                                throw new ApplicationException("Grouping type must be sp, pp, op, hp or hpp");
                        }

                        var sink = new InterGraphDataSink<int[]>(readStuff);

                        readComputation.Activate();
                        readComputation.Join();

                        Console.WriteLine("Reading done: " + readWatch.Elapsed);

                        for (int i = 0; i < 20; ++i)
                        {
                            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                            using (var computation = controller.NewComputation())
                            {
                                var firstStage = computation.NewInput(sink.NewDataSource())
                                    .ReformatInts();

                                if (parts * machines * another > 1)
                                {
                                    firstStage = firstStage
                                        .UnionFindHashTable(65000000, parts * machines * another, machines * another);
                                }

                                switch (args[3])
                                {
                                    case "sp":
                                        firstStage
                                            .PartitionBy(x => parts * parts)
                                            .UnionFindHashTable(65000000);
                                        break;

                                    case "pp":
                                        firstStage
                                            .PartitionBy(x => 16 * parts)
                                            .UnionFindHashTable(65000000);
                                        break;

                                    case "op":
                                        firstStage
                                            .PartitionBy(x => 16 * (parts * parts))
                                            .UnionFindHashTable(65000000);
                                        break;

                                    case "hp":
                                        if (parts * parts < 16)
                                        {
                                            firstStage
                                                .PartitionBy(x => 16 * x.destination + (parts * parts))
                                                .UnionFindHashTable(65000000, 0, 0)
                                                .PartitionBy(x => 16 * (machines * machines))
                                                .UnionFindHashTable(65000000);
                                        }
                                        else
                                        {
                                            firstStage
                                                .PartitionBy(x => 16 * (x.destination + (machines * machines)))
                                                .UnionFindHashTable(65000000, 0, 0)
                                                .PartitionBy(x => 16 * ((machines * machines) + (machines * machines)))
                                                .UnionFindHashTable(65000000);
                                        }
                                        break;

                                    case "hhp":
                                        firstStage
                                            .PartitionBy(x => 16 * ((x.destination / (machines * machines)) + (machines * machines * another * another)) + (x.destination % (machines * machines)))
                                            .UnionFindHashTable(65000000, -machines * another, another)
                                            .PartitionBy(x => 16 * (x.destination + (another * another) + (machines * machines * another * another)))
                                            .UnionFindHashTable(65000000, -another, 1)
                                            .PartitionBy(x => 16 * ((another * another) + (another * another) + (machines * machines * another * another)))
                                            .UnionFindHashTable(65000000);
                                        break;

                                    default:
                                        throw new ApplicationException("Grouping type must be sp, pp, op, hp or hpp");
                                }

                                computation.Activate();
                                computation.Join();
                            }

                            Console.WriteLine(stopwatch.Elapsed);
                        }
                    }

                    controller.Join();
                }
            }


            if (algorithm == "connectedcomponents" && dataset == "livejournal")
            {
                var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                using (var computation = NewComputation.FromConfig(configuration))
                {
                    var edges = System.IO.File.OpenRead(livejournalFile)
                                              .ReadEdges()
                                              .AsNaiadStream(computation);

                    edges.UnionFind(5000000)
                         .PartitionBy(x => 0)
                         .UnionFind(5000000);

                    computation.Activate();
                    computation.Join();
                }

                Console.WriteLine(stopwatch.Elapsed);
            }
            #endregion
        }

        static void Main(string[] args)
        {
            var dataDir = @"c:\data\";
            var uriBase = "hdfs://namenode/graphs/";

            if (args.Length < 3)
                throw new Exception("Three arguments required: system, algorithm, dataset");

            if (args[0] == "singlethreaded")
            {
                ExecuteSingleThreaded(args, dataDir);
            }

            if (args[0] == "naiad")
            {
                ExecuteNaiad(args, dataDir, uriBase);
            }
        }
    }
}
