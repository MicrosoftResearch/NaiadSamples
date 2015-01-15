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

//#define UseLargePages

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;


using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Frameworks.GraphLINQ;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Dataflow.PartitionBy;

using Microsoft.Research.Naiad.Dataflow.StandardVertices;

namespace COST
{
    public static class NaiadImplementations
    {
        #region input / output
        public static Stream<Edge, Epoch> LoadGraph(this Computation computation, string prefix, int count, int outof)
        {
            return Enumerable.Range(0, count)
                             .AsNaiadStream(computation)
                             .Distinct()
                             .Select(x => string.Format(prefix, x, outof))
                             .ReadGraph();
        }

        public static Stream<Edge, Epoch> LoadGraph(this Computation computation, string filename)
        {
            return new[] { filename }.AsNaiadStream(computation)
                                     .ReadGraph();
        }

        public static Stream<int, Epoch> LoadVertexStream(this Computation computation, string prefix, int count, int outof)
        {
            return Enumerable.Range(0, count)
                             .AsNaiadStream(computation)
                             .Distinct()
                             .Select(x => string.Format(prefix, x, outof))
                             .ReadVertexStream();
        }

        public static Stream<int, Epoch> LoadVertexStream(this Computation computation, string filename)
        {
            return new[] { filename }.AsNaiadStream(computation)
                                     .ReadVertexStream();
        }

        public static Stream<Edge, Epoch> ReadGraph(this Stream<string, Epoch> formats)
        {
            return formats.NewUnaryStage((i, s) => new GraphReader(i, s), null, null, "Reader");
        }

        internal class GraphReader : UnaryVertex<string, Edge, Epoch>
        {
            private Message<Edge, Epoch> sendBuffer;

            public override void OnReceive(Message<string, Epoch> message)
            {
                sendBuffer.time = message.time;

                var output = this.Output.GetBufferForTime(message.time);
                for (int i = 0; i < message.length; i++)
                {
                    SingleThreaded.ScanGraph(message.payload[i], (vertex, degree, offset, edges) =>
                    {
                        for (int neighbor = 0; neighbor < degree; neighbor++)
                        {
                            sendBuffer.payload[sendBuffer.length].source.index = vertex;
                            sendBuffer.payload[sendBuffer.length].target.index = edges[neighbor + offset];
                            sendBuffer.length++;

                            if (sendBuffer.payload.Length == sendBuffer.length)
                            {
                                this.Output.Send(sendBuffer);
                                sendBuffer.length = 0;
                            }
                        }
                    });
                }

                if (sendBuffer.length > 0)
                    this.Output.Send(sendBuffer);
            }

            public GraphReader(int index, Stage<Epoch> stage)
                : base(index, stage)
            {
                this.sendBuffer.payload = new Edge[256];
            }
        }

        internal class GraphStreamReader : UnaryVertex<string, int, Epoch>
        {
            private Message<int, Epoch> sendBuffer;

            public override void OnReceive(Message<string, Epoch> message)
            {
                sendBuffer.time = message.time;

                for (int i = 0; i < message.length; i++)
                {
                    SingleThreaded.ScanGraph(message.payload[i], (vertex, degree, offset, edges) =>
                    {
                        var edgesSent = 0;
                        while (edgesSent < degree)
                        {
                            // if not enough space for an edge, flush.
                            if (sendBuffer.payload.Length - sendBuffer.length < 3)
                            {
                                this.Output.Send(sendBuffer);
                                sendBuffer.length = 0;
                            }

                            var edgesToSend = Math.Min(sendBuffer.payload.Length - sendBuffer.length - 2, degree - edgesSent);

                            sendBuffer.payload[sendBuffer.length++] = vertex;
                            sendBuffer.payload[sendBuffer.length++] = edgesToSend;
                            Array.Copy(edges, offset + edgesSent, sendBuffer.payload, sendBuffer.length, edgesToSend);
                            sendBuffer.length += edgesToSend;
                            edgesSent += edgesToSend;
                        }
                    });
                }

                if (sendBuffer.length > 0)
                    this.Output.Send(sendBuffer);
            }

            public GraphStreamReader(int index, Stage<Epoch> stage)
                : base(index, stage)
            {
                this.sendBuffer.payload = new int[256];
            }
        }

        public static Stream<int, Epoch> ReadVertexStream(this Stream<string, Epoch> formats)
        {
            return formats.NewUnaryStage((i, s) => new GraphStreamReader(i, s), null, null, "Reader");
        }
        internal class VertexStreamReader : UnaryVertex<string, int, Epoch>
        {
            public override void OnReceive(Message<string, Epoch> message)
            {
                var output = this.Output.GetBufferForTime(message.time);

                var part = this.VertexId;
                var parts = this.Stage.Placement.Count;

                var sendBuffer = new Message<int, Epoch>();

                sendBuffer.payload = new int[256];
                sendBuffer.time = message.time;

                for (int g = 0; g < message.length; g++)
                {
                    var prefix = string.Format(message.payload[g], part, parts);

                    using (var nodeReader = System.IO.File.OpenRead(prefix + "-nodes"))
                    {
                        using (var edgeReader = System.IO.File.OpenRead(prefix + "-edges"))
                        {
                            var bytes = new byte[256];
                            var nodes = new int[(bytes.Length / 4) + 1];
                            var edges = new int[(bytes.Length / 4)];

                            var vertex = part - parts;

                            var nodebytesToRead = nodeReader.Length;
                            while (nodebytesToRead > 0)
                            {
                                // read names of neighbors, plus next vertex and degree.
                                var nodebytesRead = nodeReader.Read(bytes, 0, 4 * (nodes.Length - 1));

                                nodebytesToRead -= nodebytesRead;
                                Buffer.BlockCopy(bytes, 0, nodes, 4, nodebytesRead);

                                var edgesToRead = nodes[nodes.Length - 1] - nodes[0];
                                if (edges.Length < edgesToRead)
                                {
                                    bytes = new byte[4 * edgesToRead];
                                    edges = new int[edgesToRead];
                                }

                                edgeReader.Read(bytes, 0, 4 * edgesToRead);
                                Buffer.BlockCopy(bytes, 0, edges, 0, 4 * edgesToRead);

                                for (int i = 0; i < nodebytesRead / 4; i++)
                                {
                                    var degree = (nodes[i + 1] - nodes[i]);

                                    if (degree > 0)
                                    {
                                        var offset = nodes[i] - nodes[0];

                                        var edgesSent = 0;
                                        while (edgesSent < degree)
                                        {
                                            // if no room for an edge, just ship it
                                            if (sendBuffer.payload.Length - sendBuffer.length < 3)
                                            {
                                                this.Output.Send(sendBuffer);
                                                sendBuffer.length = 0;
                                            }

                                            var edgesToSend = Math.Min(sendBuffer.payload.Length - sendBuffer.length - 2, (degree - edgesSent));
                                            sendBuffer.payload[sendBuffer.length++] = vertex;
                                            sendBuffer.payload[sendBuffer.length++] = edgesToSend;

                                            Array.Copy(edges, offset + edgesSent, sendBuffer.payload, sendBuffer.length, edgesToSend);
                                            sendBuffer.length += edgesToSend;

                                            edgesSent += edgesToSend;
                                        }
                                    }

                                    vertex += parts;
                                }

                                nodes[0] = nodes[nodes.Length - 1];
                            }
                        }
                    }
                }

                if (sendBuffer.length > 0)
                    this.Output.Send(sendBuffer);
            }

            public VertexStreamReader(int index, Stage<Epoch> stage) : base(index, stage) { }
        }
        #endregion

        #region graph partitioning
        public static Stream<Edge, Epoch> Partition(this Stream<Edge, Epoch> edges, int sourceParts, int targetParts)
        {
            return edges
                .PartitionBy(e => (e.source % sourceParts) + sourceParts * (e.target % targetParts))
                .NewUnaryStage((i, s) => new SortEdgeVertex(i, s), null, null, "SortEdges");
        }

        public class SortEdgeVertex : UnaryBufferingVertex<Edge, Edge, Epoch>
        {
            public SortEdgeVertex(int index, Stage<Epoch> stage) : base(index, stage, records => records.OrderBy(e => e.source.index))
            {
            }
        }

        public static Stream<int[], Epoch> GroupEdgesSingleProcess(this Stream<Edge, Epoch> edges, int sourceParts, int targetParts)
        {
            return edges
                .PartitionBy(e => ((e.source % sourceParts) + sourceParts * (e.target % targetParts)))
                .NewUnaryStage((i, s) => new GroupEdgeVertex(i, s, sourceParts, targetParts), null, null, "GroupEdges");
        }

        public static Stream<int[], Epoch> GroupEdgesPartsPerProcess(this Stream<Edge, Epoch> edges, int sourceParts, int targetParts, int threads)
        {
            return edges
                .PartitionBy(e => ((e.source % sourceParts) + threads * (e.target % targetParts)))
                .NewUnaryStage((i, s) => new GroupEdgeVertex(i, s, sourceParts, targetParts), null, null, "GroupEdges");
        }

        public static Stream<int[], Epoch> GroupEdgesHierarchyPerProcess(this Stream<Edge, Epoch> edges, int localParts, int globalParts, int threads)
        {
            return edges
                .PartitionBy(e =>
                    ((e.source % globalParts) + globalParts * (e.target % globalParts)) * threads +
                    (((e.source / globalParts) % localParts) + localParts * ((e.target / globalParts) % localParts)))
                .NewUnaryStage((i, s) => new GroupEdgeVertex(i, s, localParts * globalParts, localParts * globalParts), null, null, "GroupEdges");
        }

        public static Stream<int[], Epoch> GroupEdgesOnePerProcess(this Stream<Edge, Epoch> edges, int sourceParts, int targetParts, int threads)
        {
            return edges
                .PartitionBy(e => threads * ((e.source % sourceParts) + sourceParts * (e.target % targetParts)))
                .NewUnaryStage((i, s) => new GroupEdgeVertex(i, s, sourceParts, targetParts), null, null, "GroupEdges");
        }

        public class GroupEdgeVertex : UnaryBufferingVertex<Edge, int[], Epoch>
        {
            private readonly int sourceParts;
            private readonly int targetParts;

            public override void OnNotify(Epoch time)
            {
                var records = this.Input.GetRecordsAt(time);

                Edge[] fetched;

                if (this.sourceParts == 1)
                {
                    // the .Net array growing code throws an exception since it tries to hit 2^31 entries, so explictly allocate an array smaller
                    // than that which will fit all the edges, then copy out of it
                    var preFetch = new Edge[1900000000];
                    int length = 0;
                    foreach (var edge in records)
                    {
                        preFetch[length] = edge;
                        ++length;
                    }

                    fetched = new Edge[length];
                    for (int i=0; i<fetched.Length; ++i)
                    {
                        fetched[i] = preFetch[i];
                    }
                }
                else
                {
                    fetched = records.ToArray();
                }

                int part = (fetched[0].source % sourceParts) + sourceParts * (fetched[0].target % targetParts);
                Console.WriteLine("Worker " + this.VertexId + " part " + (fetched[0].source % sourceParts) + "," + (fetched[0].target % targetParts) + " " + part);

                Dictionary<int, int> counts = new Dictionary<int, int>();
                foreach (var edge in fetched)
                {
                    if (counts.ContainsKey(edge.source.index))
                    {
                        counts[edge.source.index] = counts[edge.source.index] + 1;
                    }
                    else
                    {
                        counts[edge.source.index] = 1;
                    }
                }

                var sourceIndices = counts.Keys.ToArray();

                Dictionary<int, int> lookup = new Dictionary<int, int>();
                int lookupIndex = 0;
                for (int i = 0; i < sourceIndices.Length; ++i)
                {
                    lookup[sourceIndices[i]] = lookupIndex;
                    ++lookupIndex;
                }

                var cursors = new int[sourceIndices.Length];
                int bufferLength = 0;
                for (int i = 0; i < cursors.Length; ++i)
                {
                    cursors[i] = bufferLength;
                    bufferLength = cursors[i] + 2 + counts[sourceIndices[i]];
                }

                Console.WriteLine("Worker " + this.VertexId + " counted " + counts.Count + " sources " + bufferLength + " buffer");

                int[] sendBuffer = new int[bufferLength];

                for (int i = 0; i < cursors.Length; ++i)
                {
                    sendBuffer[cursors[i]] = sourceIndices[i];
                    sendBuffer[cursors[i] + 1] = counts[sourceIndices[i]];
                    cursors[i] += 2;
                }

                for (int i = 0; i < fetched.Length; ++i)
                {
                    int index = lookup[fetched[i].source.index];
                    sendBuffer[cursors[index]] = fetched[i].target.index;
                    ++cursors[index];
                }

                Console.WriteLine("Worker " + this.VertexId + " laid out buffer");

                var output = this.Output.GetBufferForTime(time);
                output.Send(sendBuffer);
            }

            public GroupEdgeVertex(int index, Stage<Epoch> stage, int sourceParts, int targetParts)
                : base(index, stage, null)
            {
                this.sourceParts = sourceParts;
                this.targetParts = targetParts;
            }
        }

        public static Stream<BatchStruct, Epoch> ReformatInts(this Stream<int[], Epoch> ints)
        {
            return ints
                .NewUnaryStage((i, s) => new ReformatVertex(i, s), null, null, "ReformatEdges");
        }

        public class ReformatVertex : UnaryVertex<int[], BatchStruct, Epoch>
        {
            private int[] batchBufferNoSharing = new int[256];

            public override void OnReceive(Message<int[], Epoch> message)
            {
                var output = this.Output.GetBufferForTime(message.time);

                unsafe
                {
                    int bufferLength = message.payload[0].Length;
                    BatchStruct batchStruct = new BatchStruct();
                    int* batchBuffer = &batchStruct.i00;
                    fixed (int* sendBuffer = &message.payload[0][0])
                    {
                        int thisMessageIndex = 0;
                        int cursor = 0;
                        while (cursor < bufferLength)
                        {
                            var source = sendBuffer[cursor];
                            var edgesLeft = sendBuffer[cursor + 1];
                            cursor += 2;

                            while (edgesLeft > 0)
                            {
                                if (thisMessageIndex > 253)
                                {
                                    if (thisMessageIndex < 256)
                                    {
                                        batchBuffer[thisMessageIndex] = -1;
                                    }
                                    output.Send(batchStruct);
                                    thisMessageIndex = 0;
                                }

                                var maxDegree = 254 - thisMessageIndex;

                                var thisDegree = Math.Min(edgesLeft, maxDegree);

                                batchBuffer[thisMessageIndex] = source;
                                batchBuffer[thisMessageIndex + 1] = thisDegree;
                                thisMessageIndex += 2;
                                for (int i = 0; i < thisDegree; ++i)
                                {
                                    batchBuffer[thisMessageIndex + i] = sendBuffer[cursor + i];
                                }
                                edgesLeft -= thisDegree;
                                cursor += thisDegree;
                                thisMessageIndex += thisDegree;
                            }
                        }

                        if (thisMessageIndex > 0)
                        {
                            if (thisMessageIndex < 256)
                            {
                                batchBuffer[thisMessageIndex] = -1;
                            }
                            output.Send(batchStruct);
                        }
                    }
                }
            }

            public ReformatVertex(int index, Stage<Epoch> stage) : base(index, stage)
            {
            }
        }
        #endregion

        #region struct to batch edges for serialization
        public struct BatchStruct
        {
            public int i00;
            public int i01;
            public int i02;
            public int i03;
            public int i04;
            public int i05;
            public int i06;
            public int i07;
            public int i08;
            public int i09;
            public int i0a;
            public int i0b;
            public int i0c;
            public int i0d;
            public int i0e;
            public int i0f;
            public int i10;
            public int i11;
            public int i12;
            public int i13;
            public int i14;
            public int i15;
            public int i16;
            public int i17;
            public int i18;
            public int i19;
            public int i1a;
            public int i1b;
            public int i1c;
            public int i1d;
            public int i1e;
            public int i1f;
            public int i20;
            public int i21;
            public int i22;
            public int i23;
            public int i24;
            public int i25;
            public int i26;
            public int i27;
            public int i28;
            public int i29;
            public int i2a;
            public int i2b;
            public int i2c;
            public int i2d;
            public int i2e;
            public int i2f;
            public int i30;
            public int i31;
            public int i32;
            public int i33;
            public int i34;
            public int i35;
            public int i36;
            public int i37;
            public int i38;
            public int i39;
            public int i3a;
            public int i3b;
            public int i3c;
            public int i3d;
            public int i3e;
            public int i3f;
            public int i40;
            public int i41;
            public int i42;
            public int i43;
            public int i44;
            public int i45;
            public int i46;
            public int i47;
            public int i48;
            public int i49;
            public int i4a;
            public int i4b;
            public int i4c;
            public int i4d;
            public int i4e;
            public int i4f;
            public int i50;
            public int i51;
            public int i52;
            public int i53;
            public int i54;
            public int i55;
            public int i56;
            public int i57;
            public int i58;
            public int i59;
            public int i5a;
            public int i5b;
            public int i5c;
            public int i5d;
            public int i5e;
            public int i5f;
            public int i60;
            public int i61;
            public int i62;
            public int i63;
            public int i64;
            public int i65;
            public int i66;
            public int i67;
            public int i68;
            public int i69;
            public int i6a;
            public int i6b;
            public int i6c;
            public int i6d;
            public int i6e;
            public int i6f;
            public int i70;
            public int i71;
            public int i72;
            public int i73;
            public int i74;
            public int i75;
            public int i76;
            public int i77;
            public int i78;
            public int i79;
            public int i7a;
            public int i7b;
            public int i7c;
            public int i7d;
            public int i7e;
            public int i7f;
            public int i80;
            public int i81;
            public int i82;
            public int i83;
            public int i84;
            public int i85;
            public int i86;
            public int i87;
            public int i88;
            public int i89;
            public int i8a;
            public int i8b;
            public int i8c;
            public int i8d;
            public int i8e;
            public int i8f;
            public int i90;
            public int i91;
            public int i92;
            public int i93;
            public int i94;
            public int i95;
            public int i96;
            public int i97;
            public int i98;
            public int i99;
            public int i9a;
            public int i9b;
            public int i9c;
            public int i9d;
            public int i9e;
            public int i9f;
            public int ia0;
            public int ia1;
            public int ia2;
            public int ia3;
            public int ia4;
            public int ia5;
            public int ia6;
            public int ia7;
            public int ia8;
            public int ia9;
            public int iaa;
            public int iab;
            public int iac;
            public int iad;
            public int iae;
            public int iaf;
            public int ib0;
            public int ib1;
            public int ib2;
            public int ib3;
            public int ib4;
            public int ib5;
            public int ib6;
            public int ib7;
            public int ib8;
            public int ib9;
            public int iba;
            public int ibb;
            public int ibc;
            public int ibd;
            public int ibe;
            public int ibf;
            public int ic0;
            public int ic1;
            public int ic2;
            public int ic3;
            public int ic4;
            public int ic5;
            public int ic6;
            public int ic7;
            public int ic8;
            public int ic9;
            public int ica;
            public int icb;
            public int icc;
            public int icd;
            public int ice;
            public int icf;
            public int id0;
            public int id1;
            public int id2;
            public int id3;
            public int id4;
            public int id5;
            public int id6;
            public int id7;
            public int id8;
            public int id9;
            public int ida;
            public int idb;
            public int idc;
            public int idd;
            public int ide;
            public int idf;
            public int ie0;
            public int ie1;
            public int ie2;
            public int ie3;
            public int ie4;
            public int ie5;
            public int ie6;
            public int ie7;
            public int ie8;
            public int ie9;
            public int iea;
            public int ieb;
            public int iec;
            public int ied;
            public int iee;
            public int ief;
            public int if0;
            public int if1;
            public int if2;
            public int if3;
            public int if4;
            public int if5;
            public int if6;
            public int if7;
            public int if8;
            public int if9;
            public int ifa;
            public int ifb;
            public int ifc;
            public int ifd;
            public int ife;
            public int iff;
            public int destination;
        }
        #endregion

        #region connected compoments
        public static Stream<Edge, Epoch> UnionFind(this Stream<Edge, Epoch> edges, uint nodes)
        {
            return edges.NewUnaryStage((i, s) => new UnionFindVertex(i, s, nodes), null, null, "UnionFind");
        }

        internal unsafe class UnionFindVertex : UnaryVertex<Edge, Edge, Epoch>
        {
            private readonly int part;
            private readonly int parts;

            private readonly uint nodes;
#if UseLargePages
            private int* roots;
            private byte* ranks;
#else
            private int[] roots;
            private byte[] ranks;
#endif
            private System.Diagnostics.Stopwatch stopwatch;

            private readonly Edge[] buffer = new Edge[256];

            public override void OnReceive(Message<Edge, Epoch> message)
            {
                if (this.stopwatch == null)
                {

#if UseLargePages
                    this.roots = LargePages.AllocateInts(nodes);
                    this.ranks = LargePages.AllocateBytes(nodes);
#else
                    this.roots = new int[nodes];
                    this.ranks = new byte[nodes];
#endif
                    for (int i = 0; i < this.nodes; i++)
                        this.roots[i] = i;

                    this.stopwatch = System.Diagnostics.Stopwatch.StartNew();
                    this.NotifyAt(message.time);
                }

                var roots = this.roots;

                var buffer = this.buffer;
                for (int i = 0; i < message.length; i++)
                {
                    buffer[i].source.index = roots[message.payload[i].source.index];
                    buffer[i].target.index = roots[message.payload[i].target.index];
                }

                var output = this.Output.GetBufferForTime(message.time);
                for (int i = 0; i < message.length; i++)
                {
                    var source = buffer[i].source.index;
                    var target = buffer[i].target.index;

                    // find(vertex) 
                    while (source != roots[source])
                        source = roots[source];

                    // find(vertex) 
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

            public override void OnNotify(Epoch time)
            {
                this.ranks = null;

                var edges = 0;

                var list = new List<Pair<int,int>>();

                var output = this.Output.GetBufferForTime(time);
                for (int i = 0; i < this.nodes; i++)
                {
                    var target = roots[i];
                    if (target != i)
                    {
                        while (target != roots[target])
                            target = roots[target];

                        edges++;
                        output.Send(new Edge(new Node(i), new Node(target)));

                        list.Add(target.PairWith(i));
                    }
                }

                var grouped = list.GroupBy(p => p.First);
                int groups = grouped.Count();
                int payloads = grouped.Select(g => g.Count()).Sum();

                Console.WriteLine("Worker[{2}]: UnionedFound {1} edges in {0} {3} groups {4} payload", this.stopwatch.Elapsed, edges, this.VertexId, groups, payloads);

                this.roots = null;
            }

            public UnionFindVertex(int index, Stage<Epoch> stage, uint nodes)
                : base(index, stage)
            {
                this.part = index;
                this.parts = stage.Placement.Count;
                this.nodes = nodes;
            }
        }

        public static Stream<BatchStruct, Epoch> UnionFindStruct(this Stream<BatchStruct, Epoch> edges, uint nodes, int graphPartsIn, int graphPartsOut)
        {
            return edges.NewUnaryStage((i, s) => new UnionFindVertexInts(i, s, nodes, graphPartsIn, graphPartsOut), null, null, "UnionFind");
        }

        internal unsafe class UnionFindVertexInts : UnaryVertex<BatchStruct, BatchStruct, Epoch>
        {
            private readonly int part;
            private readonly int parts;

            private readonly int graphPartsIn;
            private readonly int graphPartsOut;
            private int sourcePart;
            private int targetPart;

            private int edgesIn;
            private int edgesOut;
            private int sourcesIn;
            private int sourcesOut;

            private readonly uint nodes;
#if UseLargePages
            private int* rootsMaybeShared;
            private byte* ranksMaybeShared;
            private int* countsMaybeShared;
            private int* usedCountsMaybeShared;
            private int* bufferMaybeShared;
            private int* countStashMaybeShared;
            private int* sendBufferMaybeShared;
            private int* followedBufferMaybeShared;
#else
            private int[] rootsMaybeShared;
            private byte[] ranksMaybeShared;
            private int[] countsMaybeShared;
            private int[] usedCountsMaybeShared;
            private int[] bufferMaybeShared;
            private int[] countStashMaybeShared;
            private int[] sendBufferMaybeShared;
            private int[] followedBufferMaybeShared;
#endif
            private System.Diagnostics.Stopwatch stopwatch;

            private int bufferValid = 0;
            private int numberOfUsedCounts;
            private TimeSpan lastOnRecv;

            private void SendBuffer(Epoch time)
            {
                BatchStruct batchStruct = new BatchStruct();

                int targetOffset = 0;
                if (this.graphPartsIn > 0)
                {
                    targetOffset = (int)(this.nodes / this.graphPartsIn);
                }

                if (this.graphPartsOut > 0)
                {
                    batchStruct.destination = this.sourcePart % this.graphPartsOut + this.graphPartsOut * (this.targetPart % this.graphPartsOut);
                }

                unsafe
                {
#if UseLargePages
                    int* buffer = this.bufferMaybeShared;
                    {
                        int* usedCounts = this.usedCountsMaybeShared;
                        {
                            int* counts = this.countsMaybeShared;
                            {
                                int* countStash = this.countStashMaybeShared;
                                {
                                    int* sendBuffer = this.sendBufferMaybeShared;
                                    {
#else
                    fixed (int* buffer = &this.bufferMaybeShared[0])
                    {
                        fixed (int* usedCounts = &this.usedCountsMaybeShared[0])
                        {
                            fixed (int* counts = &this.countsMaybeShared[0])
                            {
                                fixed (int* countStash = &this.countStashMaybeShared[0])
                                {
                                    fixed (int* sendBuffer = &this.sendBufferMaybeShared[0])
                                    {
#endif
                                        int bufferLength = 0;
                                        for (int i = 0; i < numberOfUsedCounts; ++i)
                                        {
                                            var source = usedCounts[i];
                                            var thisCount = counts[source];
                                            countStash[i] = thisCount;
                                            counts[source] = bufferLength;
                                            bufferLength += thisCount + 2;
                                        }

                                        sourcesOut += numberOfUsedCounts;

                                        for (int i = 0; i < numberOfUsedCounts; ++i)
                                        {
                                            var ssource = usedCounts[i];
                                            var source = ssource;
                                            if (this.graphPartsIn > 0)
                                            {
                                                if (source >= targetOffset)
                                                {
                                                    source = (source - targetOffset) * this.graphPartsIn + this.targetPart;
                                                }
                                                else
                                                {
                                                    source = source * this.graphPartsIn + this.sourcePart;
                                                }
                                            }
                                            sendBuffer[counts[ssource]] = source;
                                            sendBuffer[counts[ssource] + 1] = countStash[i];
                                            counts[ssource] += 2;
                                        }

                                        edgesOut += bufferValid;

                                        for (int i = 0; i < bufferValid; ++i)
                                        {
                                            var source = buffer[i * 2];
                                            var target = buffer[i * 2 + 1];
                                            if (this.graphPartsIn > 0)
                                            {
                                                if (target >= targetOffset)
                                                {
                                                    target = (target - targetOffset) * this.graphPartsIn + this.targetPart;
                                                }
                                                else
                                                {
                                                    target = target * this.graphPartsIn + this.sourcePart;
                                                }
                                            }
                                            sendBuffer[counts[source]] = target;
                                            ++counts[source];
                                        }

                                        var output = this.Output.GetBufferForTime(time);
                                        int* batchBuffer = &batchStruct.i00;
                                        int thisMessageIndex = 0;
                                        int cursor = 0;
                                        while (cursor < bufferLength)
                                        {
                                            var source = sendBuffer[cursor];
                                            var edgesLeft = sendBuffer[cursor + 1];
                                            cursor += 2;

                                            while (edgesLeft > 0)
                                            {
                                                if (thisMessageIndex > 253)
                                                {
                                                    if (thisMessageIndex < 256)
                                                    {
                                                        batchBuffer[thisMessageIndex] = -1;
                                                    }
                                                    output.Send(batchStruct);
                                                    thisMessageIndex = 0;
                                                }

                                                var maxDegree = 254 - thisMessageIndex;

                                                var thisDegree = Math.Min(edgesLeft, maxDegree);

                                                batchBuffer[thisMessageIndex] = source;
                                                batchBuffer[thisMessageIndex + 1] = thisDegree;
                                                thisMessageIndex += 2;
                                                for (int i = 0; i < thisDegree; ++i)
                                                {
                                                    batchBuffer[thisMessageIndex + i] = sendBuffer[cursor + i];
                                                }
                                                edgesLeft -= thisDegree;
                                                cursor += thisDegree;
                                                thisMessageIndex += thisDegree;
                                            }
                                        }

                                        if (thisMessageIndex < 256)
                                        {
                                            batchBuffer[thisMessageIndex] = -1;
                                        }
                                        output.Send(batchStruct);

                                        this.bufferValid = 0;

                                        for (int i = 0; i < numberOfUsedCounts; ++i)
                                        {
                                            counts[usedCounts[i]] = 0;
                                        }

                                        this.numberOfUsedCounts = 0;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            public override void OnReceive(Message<BatchStruct, Epoch> message)
            {
                if (this.stopwatch == null)
                {
                    this.stopwatch = System.Diagnostics.Stopwatch.StartNew();
                    this.NotifyAt(message.time);

                    var firstSource = message.payload[0].i00;
                    var firstTarget = message.payload[0].i02;

                    long arraySize;
                    if (this.graphPartsIn > 0)
                    {
                        this.sourcePart = firstSource % this.graphPartsIn;
                        this.targetPart = firstTarget % this.graphPartsIn;
                        Console.WriteLine("Worker " + part + " doing part " + sourcePart + "," + targetPart + " of " + this.graphPartsIn);

                        if (this.sourcePart == this.targetPart)
                        {
                            arraySize = this.nodes / this.graphPartsIn;
                        }
                        else
                        {
                            arraySize = 2 * this.nodes / this.graphPartsIn;
                        }
                    }
                    else if (this.graphPartsIn < 0)
                    {
                        int gpIn = -this.graphPartsIn;
                        this.sourcePart = firstSource % gpIn;
                        this.targetPart = firstTarget % gpIn;
                        Console.WriteLine("Worker " + part + " doing interim part " + sourcePart + "," + targetPart + " of " + gpIn);
                        arraySize = this.nodes;
                    }
                    else
                    {
                        Console.WriteLine("Worker " + part + " starting " + firstSource + "," + firstTarget);
                        arraySize = this.nodes;
                    }

#if UseLargePages
                    this.rootsMaybeShared = LargePages.AllocateInts((uint)arraySize);
                    this.ranksMaybeShared = LargePages.AllocateBytes((uint)arraySize);
                    this.countsMaybeShared = LargePages.AllocateInts((uint)arraySize);
                    this.usedCountsMaybeShared = LargePages.AllocateInts(200000);
                    this.bufferMaybeShared = LargePages.AllocateInts(1200000);
                    this.countStashMaybeShared = LargePages.AllocateInts(200000);
                    this.sendBufferMaybeShared = LargePages.AllocateInts(600000);
                    this.followedBufferMaybeShared = LargePages.AllocateInts(256);
#else
                    this.rootsMaybeShared = new int[arraySize];
                    this.ranksMaybeShared = new byte[arraySize];
                    this.countsMaybeShared = new int[arraySize];
                    this.usedCountsMaybeShared = new int[200000];
                    this.bufferMaybeShared = new int[1200000];
                    this.countStashMaybeShared = new int[200000];
                    this.sendBufferMaybeShared = new int[600000];
                    this.followedBufferMaybeShared = new int[256];
#endif
                    for (int i = 0; i < arraySize; i++)
                        this.rootsMaybeShared[i] = i;
                }

                int targetOffset;
                if (this.sourcePart == this.targetPart)
                {
                    targetOffset = 0;
                }
                else
                {
                    targetOffset = (int)(this.nodes / this.graphPartsIn);
                }

                unsafe
                {
                    for (int p = 0; p < message.length; ++p)
                    {
#if UseLargePages
                        int* buffer = this.bufferMaybeShared;
                        {
                            int* roots = this.rootsMaybeShared;
                            {
                                byte* ranks = this.ranksMaybeShared;
                                {
                                    int* counts = this.countsMaybeShared;
                                    {
                                        int* usedCounts = this.usedCountsMaybeShared;
                                        {
                                            fixed (int* payload = &message.payload[p].i00)
                                            {
                                                int* followedPayload = this.followedBufferMaybeShared;
                                                {
#else
                        fixed (int* buffer = &this.bufferMaybeShared[0])
                        {
                            fixed (int* roots = &this.rootsMaybeShared[0])
                            {
                                fixed (byte* ranks = &this.ranksMaybeShared[0])
                                {
                                    fixed (int* counts = &this.countsMaybeShared[0])
                                    {
                                        fixed (int* usedCounts = &this.usedCountsMaybeShared[0])
                                        {
                                            fixed (int* payload = &message.payload[p].i00)
                                            {
                                                fixed (int* followedPayload = &this.followedBufferMaybeShared[0])
                                                {
#endif
                                                    int cursor = 0;
                                                    while (cursor < 256 && payload[cursor] >= 0)
                                                    {
                                                        var ssource = payload[cursor];
                                                        if (this.graphPartsIn > 0)
                                                        {
                                                            ssource = ssource / this.graphPartsIn;
                                                        }
                                                        payload[cursor] = ssource;
                                                        followedPayload[cursor] = roots[ssource];

                                                        var degree = payload[cursor + 1];

                                                        cursor += 2;

                                                        for (int i = 0; i < degree; ++i)
                                                        {
                                                            var ttarget = payload[cursor + i];
                                                            if (this.graphPartsIn > 0)
                                                            {
                                                                ttarget = (ttarget / this.graphPartsIn) + targetOffset;
                                                            }
                                                            payload[cursor + i] = ttarget;
                                                            followedPayload[cursor + i] = roots[ttarget];
                                                        }

                                                        cursor += degree;
                                                    }

                                                    cursor = 0;
                                                    while (cursor < 256 && payload[cursor] >= 0)
                                                    {
                                                        var ssource = payload[cursor];
                                                        var source = followedPayload[cursor];

                                                        // find(vertex) 
                                                        while (source != roots[source])
                                                            source = roots[source];

                                                        var degree = payload[cursor + 1];

                                                        edgesIn += degree;
                                                        ++sourcesIn;

                                                        cursor += 2;

                                                        for (int i = 0; i < degree; ++i)
                                                        {
                                                            var ttarget = payload[cursor + i];
                                                            var target = followedPayload[cursor + i];

                                                            // find(vertex) 
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
                                                                        buffer[this.bufferValid * 2] = source;
                                                                        buffer[this.bufferValid * 2 + 1] = ttarget;
                                                                        ++this.bufferValid;
                                                                        if (counts[source] == 0)
                                                                        {
                                                                            usedCounts[this.numberOfUsedCounts] = source;
                                                                            ++this.numberOfUsedCounts;
                                                                            counts[source] = 1;
                                                                        }
                                                                        else
                                                                        {
                                                                            ++counts[source];
                                                                        }
                                                                    }
                                                                    else
                                                                    {
                                                                        ranks[target]++;
                                                                        roots[source] = target;
                                                                        source = target;
                                                                        buffer[this.bufferValid * 2] = target;
                                                                        buffer[this.bufferValid * 2 + 1] = ssource;
                                                                        ++this.bufferValid;
                                                                        if (counts[target] == 0)
                                                                        {
                                                                            usedCounts[this.numberOfUsedCounts] = target;
                                                                            ++this.numberOfUsedCounts;
                                                                            counts[target] = 1;
                                                                        }
                                                                        else
                                                                        {
                                                                            ++counts[target];
                                                                        }
                                                                    }
                                                                }
                                                                else
                                                                {
                                                                    // attatch lower rank to higher
                                                                    if (ranks[source] < ranks[target])
                                                                    {
                                                                        roots[source] = target;
                                                                        source = target;
                                                                        buffer[this.bufferValid * 2] = target;
                                                                        buffer[this.bufferValid * 2 + 1] = ssource;
                                                                        ++this.bufferValid;
                                                                        if (counts[target] == 0)
                                                                        {
                                                                            usedCounts[this.numberOfUsedCounts] = target;
                                                                            ++this.numberOfUsedCounts;
                                                                            counts[target] = 1;
                                                                        }
                                                                        else
                                                                        {
                                                                            ++counts[target];
                                                                        }
                                                                    }
                                                                    else
                                                                    {
                                                                        roots[target] = source;
                                                                        buffer[this.bufferValid * 2] = source;
                                                                        buffer[this.bufferValid * 2 + 1] = ttarget;
                                                                        ++this.bufferValid;
                                                                        if (counts[source] == 0)
                                                                        {
                                                                            usedCounts[this.numberOfUsedCounts] = source;
                                                                            ++this.numberOfUsedCounts;
                                                                            counts[source] = 1;
                                                                        }
                                                                        else
                                                                        {
                                                                            ++counts[source];
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }

                                                        cursor += degree;

                                                        if (this.bufferValid > 100 * 1024)
                                                        {
                                                            this.SendBuffer(message.time);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                this.lastOnRecv = this.stopwatch.Elapsed;
            }

            public override void OnNotify(Epoch time)
            {
                Console.WriteLine("Worker[{1}]: Entering OnNotify {0} last OnRecv {2}",
                    this.stopwatch.Elapsed, this.VertexId, this.lastOnRecv);

                this.ranksMaybeShared = null;
                this.rootsMaybeShared = null;

                if (this.bufferValid > 0)
                {
                    this.SendBuffer(time);
                }

                Console.WriteLine("Worker[{1}]: UnionedFound {0} {2},{3} -> {4},{5}",
                    this.stopwatch.Elapsed, this.VertexId, sourcesIn, edgesIn, sourcesOut, edgesOut);
            }

            public UnionFindVertexInts(int index, Stage<Epoch> stage, uint nodes, int graphPartsIn, int graphPartsOut)
                : base(index, stage)
            {
                this.part = index;
                this.parts = stage.Placement.Count;

                this.nodes = nodes;
                this.graphPartsIn = graphPartsIn;
                this.graphPartsOut = graphPartsOut;
            }
        }

        public static Stream<BatchStruct, Epoch> UnionFindHashTable(this Stream<BatchStruct, Epoch> edges, uint nodes, int graphPartsIn, int graphPartsOut)
        {
            return edges.NewUnaryStage((i, s) => new UnionFindVertexHashTable(i, s, nodes, graphPartsIn, graphPartsOut), null, null, "UnionFind");
        }

        internal unsafe class UnionFindVertexHashTable : UnaryVertex<BatchStruct, BatchStruct, Epoch>
        {
            private readonly int part;
            private readonly int parts;

            private readonly int graphPartsIn;
            private readonly int graphPartsOut;
            private int sourcePart;
            private int targetPart;

            private int edgesIn;
            private int edgesOut;
            private int sourcesIn;
            private int sourcesOut;

            private readonly uint nodes;

            private Dictionary<int, int> roots;
            private Dictionary<int, byte> ranks;
            private Dictionary<int, int> counts;
            private List<Pair<int,int>> buffer;
            private Dictionary<int, int> countStash;
            private int[] sendBuffer;

            private System.Diagnostics.Stopwatch stopwatch;

            private TimeSpan lastOnRecv;

            private void SendBuffer(Epoch time)
            {
                BatchStruct batchStruct = new BatchStruct();

                if (this.graphPartsOut > 0)
                {
                    batchStruct.destination = this.sourcePart % this.graphPartsOut + this.graphPartsOut * (this.targetPart % this.graphPartsOut);
                }

                int bufferLength = 0;
                foreach (KeyValuePair<int, int> count in this.counts)
                {
                    var source = count.Key;
                    var thisCount = count.Value;
                    countStash[source] = bufferLength;
                    bufferLength += thisCount + 2;
                }

                sourcesOut += this.counts.Count;

                foreach (KeyValuePair<int, int> count in this.counts)
                {
                    var source = count.Key;
                    sendBuffer[countStash[source]] = source;
                    sendBuffer[countStash[source] + 1] = count.Value;
                    countStash[source] += 2;
                }

                edgesOut += this.buffer.Count;

                for (int i = 0; i < this.buffer.Count; ++i)
                {
                    var source = buffer[i].First;
                    var target = buffer[i].Second;
                    sendBuffer[countStash[source]] = target;
                    ++countStash[source];
                }

                var output = this.Output.GetBufferForTime(time);
                unsafe
                {
                    int* batchBuffer = &batchStruct.i00;
                    int thisMessageIndex = 0;
                    int cursor = 0;
                    while (cursor < bufferLength)
                    {
                        var source = sendBuffer[cursor];
                        var edgesLeft = sendBuffer[cursor + 1];
                        cursor += 2;

                        while (edgesLeft > 0)
                        {
                            if (thisMessageIndex > 253)
                            {
                                if (thisMessageIndex < 256)
                                {
                                    batchBuffer[thisMessageIndex] = -1;
                                }
                                output.Send(batchStruct);
                                thisMessageIndex = 0;
                            }

                            var maxDegree = 254 - thisMessageIndex;

                            var thisDegree = Math.Min(edgesLeft, maxDegree);

                            batchBuffer[thisMessageIndex] = source;
                            batchBuffer[thisMessageIndex + 1] = thisDegree;
                            thisMessageIndex += 2;
                            for (int i = 0; i < thisDegree; ++i)
                            {
                                batchBuffer[thisMessageIndex + i] = sendBuffer[cursor + i];
                            }
                            edgesLeft -= thisDegree;
                            cursor += thisDegree;
                            thisMessageIndex += thisDegree;
                        }
                    }

                    if (thisMessageIndex < 256)
                    {
                        batchBuffer[thisMessageIndex] = -1;
                    }
                    output.Send(batchStruct);
                }

                this.buffer.Clear();
                this.counts.Clear();
                this.countStash.Clear();
            }

            public override void OnReceive(Message<BatchStruct, Epoch> message)
            {
                if (this.stopwatch == null)
                {
                    this.stopwatch = System.Diagnostics.Stopwatch.StartNew();
                    this.NotifyAt(message.time);

                    var firstSource = message.payload[0].i00;
                    var firstTarget = message.payload[0].i02;

                    if (this.graphPartsIn > 0)
                    {
                        this.sourcePart = firstSource % this.graphPartsIn;
                        this.targetPart = firstTarget % this.graphPartsIn;
                        Console.WriteLine("Worker " + part + " doing part " + sourcePart + "," + targetPart + " of " + this.graphPartsIn);
                    }
                    else if (this.graphPartsIn < 0)
                    {
                        int gpIn = -this.graphPartsIn;
                        this.sourcePart = firstSource % gpIn;
                        this.targetPart = firstTarget % gpIn;
                        Console.WriteLine("Worker " + part + " doing interim part " + sourcePart + "," + targetPart + " of " + gpIn);
                    }
                    else
                    {
                        Console.WriteLine("Worker " + part + " starting " + firstSource + "," + firstTarget);
                    }

                    this.roots = new Dictionary<int, int>();
                    this.ranks = new Dictionary<int, byte>();
                    this.counts = new Dictionary<int, int>();
                    this.buffer = new List<Pair<int, int>>();
                    this.countStash = new Dictionary<int, int>();
                    this.sendBuffer = new int[600000];
                }

                for (int p = 0; p < message.length; ++p)
                {
                    unsafe
                    {
                        fixed (int* payload = &message.payload[p].i00)
                        {
                            int cursor = 0;
                            while (cursor < 256 && payload[cursor] >= 0)
                            {
                                var ssource = payload[cursor];
                                var source = ssource;

                                if (!roots.ContainsKey(source))
                                {
                                    roots[source] = source;
                                    ranks[source] = 0;
                                }

                                // find(vertex) 
                                while (source != roots[source])
                                    source = roots[source];

                                var degree = payload[cursor + 1];

                                edgesIn += degree;
                                ++sourcesIn;

                                cursor += 2;

                                for (int i = 0; i < degree; ++i)
                                {
                                    var ttarget = payload[cursor + i];
                                    var target = ttarget;

                                    if (!roots.ContainsKey(target))
                                    {
                                        roots[target] = target;
                                        ranks[target] = 0;
                                    }

                                    // find(vertex) 
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
                                                buffer.Add(source.PairWith(ttarget));
                                                if (counts.ContainsKey(source))
                                                {
                                                    ++counts[source];
                                                }
                                                else
                                                {
                                                    counts[source] = 1;
                                                }
                                            }
                                            else
                                            {
                                                ranks[target]++;
                                                roots[source] = target;
                                                source = target;
                                                buffer.Add(target.PairWith(ssource));
                                                if (counts.ContainsKey(target))
                                                {
                                                    ++counts[target];
                                                }
                                                else
                                                {
                                                    counts[target] = 1;
                                                }
                                            }
                                        }
                                        else
                                        {
                                            // attatch lower rank to higher
                                            if (ranks[source] < ranks[target])
                                            {
                                                roots[source] = target;
                                                source = target;
                                                buffer.Add(target.PairWith(ssource));
                                                if (counts.ContainsKey(target))
                                                {
                                                    ++counts[target];
                                                }
                                                else
                                                {
                                                    counts[target] = 1;
                                                }
                                            }
                                            else
                                            {
                                                roots[target] = source;
                                                buffer.Add(source.PairWith(ttarget));
                                                if (counts.ContainsKey(source))
                                                {
                                                    ++counts[source];
                                                }
                                                else
                                                {
                                                    counts[source] = 1;
                                                }
                                            }
                                        }
                                    }
                                }

                                cursor += degree;

                                if (this.buffer.Count > 100 * 1024)
                                {
                                    this.SendBuffer(message.time);
                                }
                            }
                        }
                    }
                }

                this.lastOnRecv = this.stopwatch.Elapsed;
            }

            public override void OnNotify(Epoch time)
            {
                Console.WriteLine("Worker[{1}]: Entering OnNotify {0} last OnRecv {2}",
                    this.stopwatch.Elapsed, this.VertexId, this.lastOnRecv);

                this.ranks = null;
                this.roots = null;

                if (this.buffer.Count > 0)
                {
                    this.SendBuffer(time);
                }

                Console.WriteLine("Worker[{1}]: UnionedFound {0} {2},{3} -> {4},{5}",
                    this.stopwatch.Elapsed, this.VertexId, sourcesIn, edgesIn, sourcesOut, edgesOut);
            }

            public UnionFindVertexHashTable(int index, Stage<Epoch> stage, uint nodes, int graphPartsIn, int graphPartsOut)
                : base(index, stage)
            {
                this.part = index;
                this.parts = stage.Placement.Count;

                this.nodes = nodes;
                this.graphPartsIn = graphPartsIn;
                this.graphPartsOut = graphPartsOut;
            }
        }

        public static Stream<Edge, Epoch> UnionFind(this Stream<BatchStruct, Epoch> edges, uint nodes)
        {
            return edges.NewUnaryStage((i, s) => new UnionFindVertexPacked(i, s, nodes), null, null, "UnionFind");
        }

        internal unsafe class UnionFindVertexPacked : UnaryVertex<BatchStruct, Edge, Epoch>
        {
            private readonly int part;
            private readonly int parts;
            private readonly uint nodes;
#if UseLargePages
            private int* rootsMaybeShared;
            private byte* ranksMaybeShared;
#else
            private int[] rootsMaybeShared;
            private byte[] ranksMaybeShared;
#endif

            private System.Diagnostics.Stopwatch stopwatch;
            private int nodesIn;
            private int edgesIn;

            public override void OnReceive(Message<BatchStruct, Epoch> message)
            {
                if (this.stopwatch == null)
                {
                    this.stopwatch = System.Diagnostics.Stopwatch.StartNew();
                    this.NotifyAt(message.time);

#if UseLargePages
                    this.rootsMaybeShared = LargePages.AllocateInts(nodes);
                    this.ranksMaybeShared = LargePages.AllocateBytes(nodes);
#else
                    this.rootsMaybeShared = new int[nodes];
                    this.ranksMaybeShared = new byte[nodes];
#endif
                    for (int i = 0; i < this.nodes; i++)
                        this.rootsMaybeShared[i] = i;
                }

                int payloadLength = 256;

                unsafe
                {
#if UseLargePages
                    int* roots = this.rootsMaybeShared;
                    {
                        byte* ranks = this.ranksMaybeShared;
                        {
#else
                    fixed (int* roots = &this.rootsMaybeShared[0])
                    {
                        fixed (byte* ranks = &this.ranksMaybeShared[0])
                        {
#endif
                            for (int cs = 0; cs < message.length; ++cs)
                            {
                                fixed (int* payload = &message.payload[cs].i00)
                                {
                                    var cursor = 0;
                                    while (cursor < payloadLength && payload[cursor] >= 0)
                                    {
                                        var degree = payload[cursor + 1];

                                        uint source = (uint)payload[cursor];
                                        var munged = (source & 0xfff00000) | ((source & 0x000ff000) >> 12) | ((source & 0x00000fff) << 8);
                                        //var munged = source;
                                        payload[cursor] = roots[munged];
                                        cursor += 2;

                                        for (int i = 0; i < degree; i++)
                                        {
                                            source = (uint)payload[cursor + i];
                                            munged = (source & 0xfff00000) | ((source & 0x000ff000) >> 12) | ((source & 0x00000fff) << 8);
                                            //munged = source;
                                            payload[cursor + i] = roots[munged];
                                        }

                                        cursor += degree;

                                        ++nodesIn;
                                        edgesIn += degree;
                                    }

                                    var output = this.Output.GetBufferForTime(message.time);

                                    cursor = 0;
                                    while (cursor < payloadLength && payload[cursor] >= 0)
                                    {
                                        var source = payload[cursor++];
                                        while (source != roots[source])
                                            source = roots[source];

                                        var degree = payload[cursor++];
                                        for (int i = 0; i < degree; i++)
                                        {
                                            var target = payload[cursor++];
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
                                                        roots[target] = source;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            public override void OnNotify(Epoch time)
            {
                this.ranksMaybeShared = null;

                var roots = this.rootsMaybeShared;

                var edges = 0;

                var output = this.Output.GetBufferForTime(time);
                for (int i = 0; i < this.nodes; i++)
                {
                    var target = roots[i];
                    if (target != i)
                    {
                        while (target != roots[target])
                            target = roots[target];

                        edges++;
                    }
                }

                Console.WriteLine("UnionedFound {1} edges (read {2},{3}) in {0}", this.stopwatch.Elapsed, edges, nodesIn, edgesIn);

                this.rootsMaybeShared = null;
            }

            public UnionFindVertexPacked(int index, Stage<Epoch> stage, uint nodes)
                : base(index, stage)
            {
                this.part = index;
                this.parts = stage.Placement.Count;

                this.nodes = nodes;
            }
        }

        public static Stream<Edge, Epoch> UnionFindHashTable(this Stream<BatchStruct, Epoch> edges, uint nodes)
        {
            return edges.NewUnaryStage((i, s) => new UnionFindVertexPackedHashTable(i, s, nodes), null, null, "UnionFind");
        }

        internal unsafe class UnionFindVertexPackedHashTable : UnaryVertex<BatchStruct, Edge, Epoch>
        {
            private readonly int part;
            private readonly int parts;
            private readonly uint nodes;

            private Dictionary<int,int> roots;
            private Dictionary<int, byte> ranks;

            private System.Diagnostics.Stopwatch stopwatch;
            private int nodesIn;
            private int edgesIn;

            public override void OnReceive(Message<BatchStruct, Epoch> message)
            {
                if (this.stopwatch == null)
                {
                    this.stopwatch = System.Diagnostics.Stopwatch.StartNew();
                    this.NotifyAt(message.time);

                    this.roots = new Dictionary<int,int>();
                    this.ranks = new Dictionary<int,byte>();
                }

                int payloadLength = 256;

                unsafe
                {
                    for (int cs = 0; cs < message.length; ++cs)
                    {
                        fixed (int* payload = &message.payload[cs].i00)
                        {
                            var cursor = 0;
                            while (cursor < payloadLength && payload[cursor] >= 0)
                            {
                                var degree = payload[cursor + 1];

                                var source = payload[cursor];
                                if (!roots.ContainsKey(source))
                                {
                                    roots[source] = source;
                                    ranks[source] = 0;
                                }

                                payload[cursor] = roots[source];
                                cursor += 2;

                                for (int i = 0; i < degree; i++)
                                {
                                    var target = payload[cursor + i];
                                    if (!roots.ContainsKey(target))
                                    {
                                        roots[target] = target;
                                        ranks[target] = 0;
                                    }
                                    payload[cursor + i] = roots[target];
                                }

                                cursor += degree;

                                ++nodesIn;
                                edgesIn += degree;
                            }

                            var output = this.Output.GetBufferForTime(message.time);

                            cursor = 0;
                            while (cursor < payloadLength && payload[cursor] >= 0)
                            {
                                var source = payload[cursor++];
                                while (source != roots[source])
                                    source = roots[source];

                                var degree = payload[cursor++];
                                for (int i = 0; i < degree; i++)
                                {
                                    var target = payload[cursor++];
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
                                                roots[target] = source;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            public override void OnNotify(Epoch time)
            {
                this.ranks = null;

                var edges = 0;

                var output = this.Output.GetBufferForTime(time);
                foreach (KeyValuePair<int,int> root in this.roots)
                {
                    var source = root.Key;
                    var target = root.Value;
                    if (target != source)
                    {
                        while (target != roots[target])
                            target = roots[target];

                        edges++;
                    }
                }

                Console.WriteLine("UnionedFound {1} edges (read {2},{3}) in {0}", this.stopwatch.Elapsed, edges, nodesIn, edgesIn);

                this.roots = null;
            }

            public UnionFindVertexPackedHashTable(int index, Stage<Epoch> stage, uint nodes)
                : base(index, stage)
            {
                this.part = index;
                this.parts = stage.Placement.Count;

                this.nodes = nodes;
            }
        }
        #endregion

        #region pagerank
        public static Stream<NodeWithValue<float>, Epoch> PageRank(this Stream<Edge, Epoch> edges, int iterations, string description)
        {
            // shuffles edges once, rather than twice.
            edges = edges.PartitionBy(x => x.source);

            // capture degrees before trimming leaves.
            var degrees = edges.Select(x => x.source)
                               .CountNodes()
                               .Select(x => x.node.WithValue((Int32)x.value));

            // removes edges to pages with zero out-degree.
            var trim = false;
            if (trim)
                edges = edges.Select(x => x.target.WithValue(x.source))
                             .FilterBy(degrees.Select(x => x.node))
                             .Select(x => new Edge(x.value, x.node));

            // initial distribution of ranks.
            var start = degrees.Select(x => x.node.WithValue(0.15f))
                               .PartitionBy(x => x.node.index);

            // define an iterative pagerank computation, add initial values, aggregate up the results and print them to the screen.
            var ranks = start.IterateAndAccumulate((lc, deltas) => deltas.PageRankStep(lc.EnterLoop(degrees),
                                                                                       lc.EnterLoop(edges)),
                                                    x => x.node.index,
                                                    iterations,
                                                    "PageRank")
                             .Concat(start)                             // add initial ranks in for correctness.
                             .NodeAggregate((x, y) => x + y)            // accumulate up the ranks.
                             .Where(x => x.value > 0.0f);               // report only positive ranks.

            return ranks;
        }

        // performs one step of pagerank, scaling ranks by (1.0 - reset) / degree, tranmitting to neighbors, aggregating along the way.
        public static Stream<NodeWithValue<float>, T> PageRankStep<T>(this Stream<NodeWithValue<float>, T> ranks,
                                                                           Stream<NodeWithValue<Int32>, T> degrees,
                                                                           Stream<Edge, T> edges)
            where T : Time<T>
        {
            // join ranks with degrees, scaled down. then "graphreduce", accumulating ranks over graph edges.
            return ranks.NodeJoin(degrees, (rank, degree) => degree > 0 ? rank * (0.85f / degree) : 0.0f)
                        .GraphReduce(edges, (x, y) => x + y, false)
                        .Where(x => x.value > 0.0f);
        }
        #endregion
    }
}
