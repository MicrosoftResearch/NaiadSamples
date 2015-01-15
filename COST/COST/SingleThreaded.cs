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

namespace COST
{
    public interface GraphScanner
    {
        void ForEach(Action<int, int> perVertexLogic);
        void ForEach(Action<int, int, int, int[]> perVertexLogic);
    }

    public class FileGraphScanner : GraphScanner
    {
        private readonly string filePrefix;

        public FileGraphScanner(string prefix) { this.filePrefix = prefix; }

        public void ForEach(Action<int, int, int, int[]> perVertexLogic)
        {
            var metadata = UnbufferedIO.ReadFile<int>(this.filePrefix + "-nodes");

            var maxDegree = 0;
            for (int i = 0; i < metadata.Length; i += 2)
                maxDegree = Math.Max(maxDegree, metadata[i + 1]);

            var neighbors = new int[2 * maxDegree];
            var neighborsOffset = 0;

            using (var reader = new UnbufferedIO.SequentialReader(this.filePrefix + "-edges"))
            {
                reader.Read(neighbors, 0, neighbors.Length);

                for (int i = 0; i < metadata.Length; i += 2)
                {
                    var vertex = metadata[i + 0];
                    var degree = metadata[i + 1];

                    if (neighborsOffset + degree > neighbors.Length - 1)
                    {
                        Array.Copy(neighbors, neighborsOffset, neighbors, 0, neighbors.Length - neighborsOffset);
                        reader.Read(neighbors, neighbors.Length - neighborsOffset, neighborsOffset);
                        neighborsOffset = 0;
                    }

                    perVertexLogic(vertex, degree, neighborsOffset, neighbors);
                    neighborsOffset += degree;
                }
            }
        }

        public void ForEach(Action<int, int> perVertexLogic)
        {
            var metadata = UnbufferedIO.ReadFile<int>(this.filePrefix + "-nodes");
            for (int i = 0; i < metadata.Length; i += 2)
                perVertexLogic(metadata[i], metadata[i + 1]);
        }
    }

    public class MemoryGraphScanner : GraphScanner
    {
        private readonly int[] metadata;
        private readonly int[] neighbors;

        public MemoryGraphScanner(string prefix)
        {
            this.metadata = UnbufferedIO.ReadFile<int>(prefix + "-nodes");
            this.neighbors = UnbufferedIO.ReadFile<int>(prefix + "-edges");
        }

        public void ForEach(Action<int, int> perVertexLogic)
        {
            for (int i = 0; i < this.metadata.Length; i += 2)
                perVertexLogic(this.metadata[i], this.metadata[i + 1]);
        }

        public void ForEach(Action<int, int, int, int[]> perVertexLogic)
        {
            var offset = 0;

            for (int i = 0; i < metadata.Length; i += 2)
            {
                var vertex = metadata[i + 0];
                var degree = metadata[i + 1];

                perVertexLogic(vertex, degree, offset, neighbors);
                offset += degree;
            }
        }
    }

    class SingleThreaded
    {
        #region graph scanning patterns

        public static void ScanGraph(string filename, Action<int, int, int, int[]> perVertexLogic)
        {
            var metadata = UnbufferedIO.ReadFile<int>(filename + "-nodes");

            var maxDegree = 0;
            for (int i = 0; i < metadata.Length; i += 2)
                maxDegree = Math.Max(maxDegree, metadata[i + 1]);

            var neighbors = new int[2 * maxDegree];
            var neighborsOffset = 0;

            using (var reader = new UnbufferedIO.SequentialReader(filename + "-edges"))
            {
                reader.Read(neighbors, 0, neighbors.Length);

                for (int i = 0; i < metadata.Length; i += 2)
                {
                    var vertex = metadata[i + 0];
                    var degree = metadata[i + 1];

                    if (neighborsOffset + degree > neighbors.Length - 1)
                    {
                        Array.Copy(neighbors, neighborsOffset, neighbors, 0, neighbors.Length - neighborsOffset);
                        reader.Read(neighbors, neighbors.Length - neighborsOffset, neighborsOffset);
                        neighborsOffset = 0;
                    }

                    perVertexLogic(vertex, degree, neighborsOffset, neighbors);
                    neighborsOffset += degree;
                }
            }
        }

        public static void ScanGraph(string filename, Action<int, int> perVertexLogic)
        {
            var metadata = UnbufferedIO.ReadFile<int>(filename + "-nodes");
            for (int i = 0; i < metadata.Length; i += 2)
                perVertexLogic(metadata[i], metadata[i + 1]);
        }

        public static void ScanGraph(int[] metadata, int[] neighbors, Action<int, int, int, int[]> perVertexLogic)
        {
            var offset = 0;

            for (int i = 0; i < metadata.Length; i += 2)
            {
                var vertex = metadata[i + 0];
                var degree = metadata[i + 1];

                perVertexLogic(vertex, degree, offset, neighbors);
                offset += degree;
            }
        }

        public static void ScanGraphHilbert(string filename, Action<uint, uint, uint, uint, uint[]> perVertexLogic)
        {
            var metadata = UnbufferedIO.ReadFile<uint>(filename + "-upper");

            var maxCount = (uint)0;
            for (int i = 0; i < metadata.Length; i += 3)
                maxCount = Math.Max(maxCount, metadata[i + 2]);

            var edges = new uint[2 * maxCount];
            var edgesOffset = (uint)0;

            using (var reader = new UnbufferedIO.SequentialReader(filename + "-lower"))
            {
                reader.Read(edges, 0, edges.Length);

                for (int i = 0; i < metadata.Length; i += 3)
                {
                    var sourceUpper = metadata[i + 0] << 16;
                    var targetUpper = metadata[i + 1] << 16;

                    var count = metadata[i + 2];

                    if (edgesOffset + count > edges.Length - 1)
                    {
                        Array.Copy(edges, edgesOffset, edges, 0, edges.Length - edgesOffset);
                        reader.Read(edges, (int)(edges.Length - edgesOffset), (int)edgesOffset);
                        edgesOffset = 0;
                    }

                    perVertexLogic(sourceUpper, targetUpper, edgesOffset, count, edges);
                    edgesOffset += count;
                }
            }
        }

        public static void ScanGraphHilbert(uint[] metadata, uint[] edges, Action<uint, uint, uint, uint, uint[]> perVertexLogic)
        {
            var edgesOffset = (uint)0;

            for (int i = 0; i < metadata.Length; i += 3)
            {
                var sourceUpper = metadata[i + 0] << 16;
                var targetUpper = metadata[i + 1] << 16;

                var count = metadata[i + 2];

                perVertexLogic(sourceUpper, targetUpper, edgesOffset, count, edges);
                edgesOffset += count;
            }
        }

        #endregion

        #region graph manipulation / transformation

        public static void PartitionGraph(string filename, int parts, Func<int, int, int> partitionFunction, string newFormat)
        {
            var nodeStreams = new System.IO.BinaryWriter[parts];
            var edgeStreams = new System.IO.BinaryWriter[parts];

            for (int i = 0; i < parts; i++)
            {
                nodeStreams[i] = new System.IO.BinaryWriter(System.IO.File.OpenWrite(string.Format(newFormat + "-nodes", i, parts)));
                edgeStreams[i] = new System.IO.BinaryWriter(System.IO.File.OpenWrite(string.Format(newFormat + "-edges", i, parts)));
            }

            var counters = new int[parts];

            ScanGraph(filename, (vertex, degree, offset, edges) =>
            {
                for (int i = 0; i < parts; i++)
                    counters[i] = 0;

                for (int i = 0; i < degree; i++)
                    counters[partitionFunction(vertex, edges[offset + i]) % parts]++;

                for (int i = 0; i < parts; i++)
                {
                    if (counters[i] > 0)
                    {
                        nodeStreams[i].Write(vertex);
                        nodeStreams[i].Write(counters[i]);
                    }
                }

                for (int i = 0; i < degree; i++)
                    edgeStreams[partitionFunction(vertex, edges[offset + i]) % parts].Write(edges[offset + i]);
            });

            for (int i = 0; i < parts; i++)
            {
                nodeStreams[i].Close();
                edgeStreams[i].Close();
            }
        }

        public static void TransposeGraph(string filename, int nodecount)
        {
            var nodes = new int[nodecount + 1];

            ScanGraph(filename, (vertex, degree, offset, neighbors) =>
            {
                for (int i = 0; i < degree; i++)
                    nodes[neighbors[offset + i]]++;
            });

            // accumulate to determine ending offsets
            for (int i = 1; i < nodes.Length; i++)
                nodes[i] += nodes[i - 1];

            // shift to determine starting offsets
            for (int i = nodes.Length - 1; i > 0; i--)
                nodes[i] = nodes[i - 1];

            nodes[0] = 0;

            var edges = new int[nodes[nodecount - 1]];

            var buffer = new BufferTrie<int>.Pair[5000000];

            var edgeTrie = new BufferTrie<int>(26, (array, offset, length) =>
            {
                for (int i = 0; i < length; i++)
                    edges[nodes[array[i].Index]++] = array[i].Value;
            });


            ScanGraph(filename, (vertex, degree, offset, neighbors) =>
            {
                for (int i = 0; i < degree; i++)
                    buffer[i] = new BufferTrie<int>.Pair(neighbors[offset + i], vertex);

                edgeTrie.Insert(buffer, 0, degree);
            });

            edgeTrie.Flush();
        }

        public static void ConvertGraph(string filename)
        {
            using (var nodeWriter = new BinaryWriter(File.OpenWrite(filename + "-nodes")))
            {
                using (var edgeWriter = new BinaryWriter(File.OpenWrite(filename + "-edges")))
                {
                    using (var reader = new UnbufferedIO.SequentialReader(filename))
                    {
                        var ints = new int[2];

                        var bytesToRead = reader.Length - 8;

                        // read vertex name and degree

                        reader.Read(ints, 0, 2);

                        var vertex = ints[0];
                        var degree = ints[1];

                        while (bytesToRead > 0)
                        {
                            // allocate space if needed
                            if (ints.Length < degree + 2)
                                ints = new int[degree + 2];

                            bytesToRead -= 4 * reader.Read(ints, 0, (degree + 2));

                            nodeWriter.Write(vertex);
                            nodeWriter.Write(degree);

                            for (int i = 0; i < degree; i++)
                                edgeWriter.Write(ints[i]);

                            vertex = ints[degree];
                            degree = ints[degree + 1];
                        }
                    }
                }
            }
        }

        #endregion

        public static unsafe void MultiHilbertPagerank(string filename, float* a, float* b, uint nodes, float reset)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            var degrees = LargePages.AllocateInts(nodes);

            var scanner = new MultiHilbertScanner(filename);

            scanner.Scan((xUpper, yUpper, count, offset, edges) =>
            {
                for (var j = offset; j < offset + count; j++)
                    degrees[xUpper + ((edges[j] & 0xF0) >> 4)]++;
            });

            Console.WriteLine("{0}\tDegrees calculated", stopwatch.Elapsed);

            for (int iteration = 0; iteration < 20; iteration++)
            {
                var startTime = stopwatch.Elapsed;

                for (int i = 0; i < nodes; i++)
                {
                    b[i] = (1.0f - reset) * a[i] / degrees[i];
                    a[i] = reset;
                }

                scanner.Scan((xUpper, yUpper, count, offset, edges) =>
                {
                        for (var j = offset; j < offset + count; j++)
                            a[yUpper + (edges[j] & 0x0F)] += b[xUpper + ((edges[j] & 0xF0) >> 4)];
                });

                Console.WriteLine("{0}\tIteration {1} in {2}", stopwatch.Elapsed, iteration, stopwatch.Elapsed - startTime);
            }
        }

        public static unsafe void MultiHilbertCC(string filename, uint nodes)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            var roots = LargePages.AllocateInts(nodes);
            var ranks = LargePages.AllocateBytes(nodes);

            for (int i = 0; i < nodes; i++)
                roots[i] = i;

            var scanner = new MultiHilbertScanner(filename);
            
            scanner.Scan((xUpper, yUpper, count, offset, edges) =>
            {
                for (int j = 0; j < count; j++)
                {
                    var source = (int)(xUpper + ((edges[offset + j] & 0xF0) >> 4));
                    var target = (int)(yUpper + ((edges[offset + j] & 0x0F) >> 0));

                    if (source != target)
                    {
                        while (source != roots[source])
                            source = roots[source];

                        while (target != roots[target])
                            target = roots[target];

                        // union(source, target)
                        if (source != target)
                        {
                            // there may be a tie in ranks
                            if (ranks[source] == ranks[target])
                            {
                                // break ties towards lower ids
                                if (source < target)
                                {
                                    ranks[source]++;
                                    roots[target] = source;
                                }
                                else
                                {
                                    ranks[target]++;
                                    roots[source] = target;
                                }
                            }
                            else
                            {
                                // attatch lower rank to higher
                                if (ranks[source] < ranks[target])
                                    roots[source] = target;
                                else
                                    roots[target] = source;
                            }
                        }
                    }
                }
            });

            // path compress all vertices to roots.
            for (int i = 0; i < nodes; i++)
                while (roots[i] != roots[roots[i]])
                    roots[i] = roots[roots[i]];

            var counter = 0;
            for (int i = 0; i < nodes; i++)
                if (roots[i] != i)
                    counter++;

            Console.WriteLine("{1}\tEdges found: {0}", counter, stopwatch.Elapsed);
        }

        public static unsafe void HilbertPagerank(string filename, float* a, float* b, uint nodes, float reset)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            var degrees = LargePages.AllocateInts(nodes);
            //var degrees = new int[nodes];

            var metadata = UnbufferedIO.ReadFile<uint>(filename + "-upper");
            var edges = UnbufferedIO.ReadFile<uint>(filename + "-lower");

            Console.WriteLine("{2}\tRead header of {0} blocks, for {1} edges", metadata.Length / 3, edges.Length, stopwatch.Elapsed);

            ScanGraphHilbert(metadata, edges, (srcUpper, tgtUpper, offset, count, edgeArray) =>
            {
                for (var j = offset; j < offset + count; j++)
                    degrees[srcUpper + (edges[j] & 0xFFFF)]++;
            });

            Console.WriteLine("{0}\tDegrees calculated", stopwatch.Elapsed);

            for (int iteration = 0; iteration < 20; iteration++)
            {
                var startTime = stopwatch.Elapsed;

                for (int i = 0; i < nodes; i++)
                {
                    b[i] = (1.0f - reset) * a[i] / degrees[i];
                    a[i] = reset;
                }

                ScanGraphHilbert(metadata, edges, (sourceUpper, targetUpper, offset, count, edgeArray) =>
                {
                    for (var j = offset; j < offset + count; j++)
                        a[targetUpper + (edges[j] >> 16)] += b[sourceUpper + (edges[j] & 0xFFFF)];
                });

                Console.WriteLine("{0}\tIteration {1} in {2}", stopwatch.Elapsed, iteration, stopwatch.Elapsed - startTime);
            }
        }

        public static unsafe void HilbertPagerankFromDisk(string filename, float[] a, float[] b, uint nodes, float reset)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

#if UseLargePages
            var degrees = LargePages.AllocateInts(nodes);
#else
            var degrees = new int[nodes];
#endif

            ScanGraphHilbert(filename, (srcUpper, tgtUpper, offset, count, edges) =>
            {
                for (int i = 0; i < count; i++)
                    degrees[srcUpper + (edges[offset + i] >> 16)]++;
            });

            Console.WriteLine("{0}\tDegrees calculated", stopwatch.Elapsed);

            for (int iteration = 0; iteration < 20; iteration++)
            {
                var startTime = stopwatch.Elapsed;

                ScanGraphHilbert(filename, (srcUpper, tgtUpper, offset, count, edges) =>
                {
                    for (int i = 0; i < count; i++)
                        a[tgtUpper + (edges[offset + i] >> 16)] += b[srcUpper + (edges[offset + i] & 0xFFFF)];
                });

                for (int i = 0; i < nodes; i++)
                    a[i] = a[i] / degrees[i];

                Console.WriteLine("{0}\tIteration {1} in {2}", stopwatch.Elapsed, iteration, stopwatch.Elapsed - startTime);
            }
        }

        public static unsafe void HilbertUnionFind(string filenameUpper, string filenameLower, uint nodes)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            using (var upper = System.IO.File.OpenRead(filenameUpper))
            {
                using (var lower = System.IO.File.OpenRead(filenameLower))
                {
                    var bytes = new byte[upper.Length];
                    upper.Read(bytes, 0, bytes.Length);

                    var uppers = new int[bytes.Length / 4];
                    Buffer.BlockCopy(bytes, 0, uppers, 0, bytes.Length);

                    var maxEdges = 0;
                    for (int i = 0; i < uppers.Length; i += 3)
                        maxEdges = Math.Max(maxEdges, uppers[i + 2]);

                    Console.WriteLine("{1}\tRead header of {0} blocks", uppers.Length / 3, stopwatch.Elapsed);

                    bytes = new byte[8 * maxEdges];
                    var lowers = new UInt16[2 * maxEdges];

                    var edges = new Int32[2 * maxEdges];

#if UseLargePages
                    var roots = LargePages.AllocateInts(nodes);
                    var ranks = LargePages.AllocateBytes(nodes);
#else
                    var roots = new int[nodes];
                    var ranks = new byte[nodes];
#endif


                    for (int i = 0; i < nodes; i++)
                        roots[i] = i;

                    for (int i = 0; i < uppers.Length; i += 3)
                    {
                        var read = lower.Read(bytes, 0, 4 * uppers[i + 2]);
                        Buffer.BlockCopy(bytes, 0, lowers, 0, read);

                        var sourceUpper = uppers[i + 0] << 16;
                        var targetUpper = uppers[i + 1] << 16;

                        var count = uppers[i + 2];

                        for (int j = 0; j < count; j++)
                        {
                            edges[2 * j + 0] = roots[sourceUpper + lowers[2 * j + 0]];
                            edges[2 * j + 1] = roots[targetUpper + lowers[2 * j + 1]];
                        }

                        for (int j = 0; j < count; j++)
                        {
                            var source = edges[2 * j + 0];
                            var target = edges[2 * j + 1];

                            if (source != target)
                            {
                                while (source != roots[source])
                                    source = roots[source];

                                while (target != roots[target])
                                    target = roots[target];

                                // union(source, target)
                                if (source != target)
                                {
                                    // there may be a tie in ranks
                                    if (ranks[source] == ranks[target])
                                    {
                                        // break ties towards lower ids
                                        if (source < target)
                                        {
                                            ranks[source]++;
                                            roots[target] = source;
                                        }
                                        else
                                        {
                                            ranks[target]++;
                                            roots[source] = target;
                                        }
                                    }
                                    else
                                    {
                                        // attatch lower rank to higher
                                        if (ranks[source] < ranks[target])
                                            roots[source] = target;
                                        else
                                            roots[target] = source;
                                    }
                                }
                            }
                        }
                    }

                    // path compress all vertices to roots.
                    for (int i = 0; i < nodes; i++)
                        while (roots[i] != roots[roots[i]])
                            roots[i] = roots[roots[i]];

                    var counter = 0;
                    for (int i = 0; i < nodes; i++)
                        if (roots[i] != i)
                            counter++;

                    Console.WriteLine("Edges found: {0}", counter);
                }
            }
        }

        public static unsafe void HilbertUnionFind2(string filename, uint nodes)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

#if UseLargePages
            var roots = LargePages.AllocateInts(nodes);
            var ranks = LargePages.AllocateBytes(nodes);
#else
            var roots = new int[nodes];
            var ranks = new byte[nodes];
#endif

            for (int i = 0; i < nodes; i++)
                roots[i] = i;

            ScanGraphHilbert(filename, (sourceUpper, targetUpper, offset, count, edges) =>
            {
                for (int j = 0; j < count; j++)
                {
                    var source = (int)(sourceUpper + (edges[offset + j] & 0xFFFF));
                    var target = (int)(targetUpper + (edges[offset + j] >> 16));

                    if (source != target)
                    {
                        while (source != roots[source])
                            source = roots[source];

                        while (target != roots[target])
                            target = roots[target];

                        // union(source, target)
                        if (source != target)
                        {
                            // there may be a tie in ranks
                            if (ranks[source] == ranks[target])
                            {
                                // break ties towards lower ids
                                if (source < target)
                                {
                                    ranks[source]++;
                                    roots[target] = source;
                                }
                                else
                                {
                                    ranks[target]++;
                                    roots[source] = target;
                                }
                            }
                            else
                            {
                                // attatch lower rank to higher
                                if (ranks[source] < ranks[target])
                                    roots[source] = target;
                                else
                                    roots[target] = source;
                            }
                        }
                    }
                }
            });

            // path compress all vertices to roots.
            for (int i = 0; i < nodes; i++)
                while (roots[i] != roots[roots[i]])
                    roots[i] = roots[roots[i]];

            var counter = 0;
            for (int i = 0; i < nodes; i++)
                if (roots[i] != i)
                    counter++;

            Console.WriteLine("Edges found: {0}", counter);
        }

        public static void PageRankStep(string filename, float[] a, float[] b, uint nodes, float reset)
        {
            for (int i = 0; i < a.Length; i++)
                a[i] = reset;

            // apply per-vertex pagerank logic across the graph
            ScanGraph(filename, (vertex, degree, offset, neighbors) =>
            {
                var update = (1.0f - reset) * b[vertex] / degree;
                for (int i = 0; i < degree; i++)
                    a[neighbors[offset + i]] += update;
            });
        }

        public unsafe static void PageRankStepFromDisk(string filename, float* a, float* b, uint nodes, float reset)
        {
            for (int i = 0; i < nodes; i++)
                a[i] = reset;

            // apply per-vertex pagerank logic across the graph
            ScanGraph(filename, (vertex, degree, offset, neighbors) =>
            {
                var update = (1.0f - reset) * b[vertex] / degree;
                for (int i = 0; i < degree; i++)
                    a[neighbors[offset + i]] += update;
            });
        }

        public unsafe static void PageRankFromDisk(string filename, float* a, float* b, uint nodes, float reset)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            for (int i = 0; i < nodes; i++)
                a[i] = reset;

            var graph = new FileGraphScanner(filename);

            for (int iteration = 0; iteration < 20; iteration++)
            {
                var startTime = stopwatch.Elapsed;

                graph.ForEach((vertex, degree) =>
                {
                    b[vertex] = (1.0f - reset) * a[vertex] / degree;
                    a[vertex] = reset;
                });

                graph.ForEach((vertex, degree, offset, neighbors) =>
                {
                    for (int i = 0; i < degree; i++)
                        a[neighbors[offset + i]] += b[vertex];
                });

                Console.WriteLine("{0}\tIteration {1}", stopwatch.Elapsed - startTime, iteration);
            }

            Console.WriteLine(stopwatch.Elapsed);
        }

        public unsafe static void PageRankStep(string filename, float* a, float* b, uint nodes, float reset)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            var nodeArray = UnbufferedIO.ReadFile<int>(filename + "-nodes");
            var edgeArray = UnbufferedIO.ReadFile<int>(filename + "-edges");

            for (int i = 0; i < nodes; i++)
                a[i] = reset;

            for (int iteration = 0; iteration < 20; iteration++)
            {
                var startTime = stopwatch.Elapsed;

                for (int i = 0; i < nodes; i++)
                {
                    b[i] = a[i];
                    a[i] = 0.0f;
                }

                // apply per-vertex pagerank logic across the graph
                ScanGraph(nodeArray, edgeArray, (vertex, degree, offset, neighbors) =>
                {
                    if (b[vertex] > 0.0f)
                    {
                        var update = (1.0f - reset) * b[vertex] / degree;
                        for (int i = 0; i < degree; i++)
                            a[neighbors[offset + i]] += update;
                    }
                });

                Console.WriteLine("{0}\tIteration {1}", stopwatch.Elapsed - startTime, iteration);
            }

            Console.WriteLine(stopwatch.Elapsed);
        }



        public unsafe static void ClumsyCC(string filename, uint nodes)
        {
            var labels = LargePages.AllocateInts(nodes);

            for (int i = 0; i < nodes; i++)
                labels[i] = i;

            var oldSum = Int64.MaxValue;
            var newSum = 0L;
            for (int i = 0; i < nodes; i++)
                newSum += labels[i];

            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            while (newSum < oldSum)
            {
                var startTime = stopwatch.Elapsed;

                // apply per-vertex pagerank logic across the graph
                ScanGraph(filename, (vertex, degree, offset, neighbors) =>
                {
                    var label = labels[vertex];
                    for (int i = 0; i < degree; i++)
                        label = Math.Min(label, labels[neighbors[offset + i]]);

                    labels[vertex] = label;
                    for (int i = 0; i < degree; i++)
                        labels[neighbors[offset + i]] = label;

                });

                oldSum = newSum;
                newSum = 0L;
                for (int i = 0; i < nodes; i++)
                    newSum += labels[i];

                Console.WriteLine("{0}", stopwatch.Elapsed - startTime);
            }
        }


        public unsafe static void ConnectedComponents(string filename, uint nodes)
        {
#if UseLargePages
            var roots = LargePages.AllocateInts(nodes);
            var ranks = LargePages.AllocateBytes(nodes);
#else
            var roots = new int[nodes];
            var ranks = new byte[nodes];
#endif
            for (int i = 0; i < nodes; i++)
                roots[i] = i;

            // apply per-vertex union-find logic across the graph
            ScanGraph(filename, (vertex, degree, offset, neighbors) =>
            {
                // find(vertex)
                var source = roots[vertex];
                while (source != roots[source])
                    source = roots[source];

                for (int i = 0; i < degree; i++)
                {
                    // find(neighbors[i])
                    var target = roots[neighbors[offset + i]];
                    while (target != roots[target])
                        target = roots[target];

                    // union(source, target)
                    if (source != target)
                    {

                        // there may be a tie in ranks
                        if (ranks[source] == ranks[target])
                        {
                            // break ties towards lower ids
                            if (source < target)
                            {
                                ranks[source]++;
                                roots[target] = source;
                            }
                            else
                            {
                                ranks[target]++;
                                roots[source] = target;
                                source = target;
                            }
                        }
                        else
                        {
                            // attatch lower rank to higher
                            if (ranks[source] < ranks[target])
                            {
                                roots[source] = target;
                                source = target;
                            }
                            else
                            {
                                roots[target] = source;
                            }
                        }
                    }
                }
            });

            var counter = 0;
            for (int i = 0; i < nodes; i++)
                if (roots[i] != i)
                    counter++;

            Console.WriteLine("Edges found: {0}", counter);
        }

        public static void MaximalIndependentSet(string filename, int nodes)
        {
            var active = new bool[nodes];

            ScanGraph(filename, (vertex, degree, offset, neighbors) =>
            {
                active[vertex] = true;
                for (int i = 0; i < degree; i++)
                    if (neighbors[offset + i] < i && active[neighbors[offset + i]])
                        active[vertex] = false;
            });
        }
    }
}
