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

namespace COST
{
    public static class HilbertCurve
    {
        // lifted from wikipedia's hilbert curve page
        //convert (x,y) to d
        public static Int64 xy2d(int logn, int x, int y)
        {
            Int64 d = 0;
            for (var logs = logn - 1; logs >= 0; logs--)
            {
                int rx = (x >> logs) & 1;
                int ry = (y >> logs) & 1;

                // map (0,0), (0,1), (1,0), (1,1) to 00, 01, 10, 11.
                d += (((Int64)1) << (2 * logs)) * ((3 * rx) ^ ry);
                rot(logs, ref x, ref y, rx, ry);
            }
            return d;
        }

        public static Int64 xy2dAlt(int logn, uint x, uint y, ref bool rotated, ref bool flipped)
        {
            Int64 d = 0;

            for (var logs = logn - 1; logs >= 0; logs--)
            {
                uint rx = (x >> logs) & 1;
                uint ry = (y >> logs) & 1;

                if (flipped)
                {
                    var t = rx;
                    rx = ry;
                    ry = t;
                }

                if (rotated)
                {
                    rx = 1 - rx;
                    ry = 1 - ry;
                }

                // map (0,0), (0,1), (1,0), (1,1) to 00, 01, 10, 11.
                d += (((Int64)1) << (2 * logs)) * ((3 * rx) ^ ry);

                if (ry == 0)
                {
                    if (rx == 1)
                        rotated = !rotated;

                    flipped = !flipped;
                }

                // rot(logs, ref x, ref y, rx, ry);
            }
            return d;
        }

        public static UInt64 xy2dByte(uint x, uint y)
        {
            UInt64 d = 0;

            var flipped = false;
            var rotated = false;

            for (var logs = 3; logs >= 0; logs--)
            {
                // read out relevant bytes
                var rx = (byte)(x >> (8 * logs)) & 0xFF;
                var ry = (byte)(y >> (8 * logs)) & 0xFF;

                // flip and rotate bytes, if needed
                if (flipped)
                {
                    var t = rx;
                    rx = ry;
                    ry = t;
                }

                if (rotated)
                {
                    rx = 0xFF - rx;
                    ry = 0xFF - ry;
                }

                // update flipped and rotated
                flipped = flipped ^ Flipped[rx][ry];
                rotated = rotated ^ Rotated[rx][ry];

                // look up the transform of these bytes
                var result = (UInt64)Transformed[rx][ry];

                d += result << (16 * logs);
            }

            return d;
        }

        //convert d to (x,y)
        public static void d2xy(int logn, Int64 d, out int x, out int y)
        {
            int logs;
            var t = d;

            x = y = 0;
            for (logs = 0; logs < logn; logs++)
            {
                var rx = (int)(t >> 1) & 1;
                var ry = (int)(t ^ rx) & 1;

                rot(logs, ref x, ref y, rx, ry);

                x += rx << logs;
                y += ry << logs;

                t /= 4;
            }
        }

        public static void d2xyByte(UInt64 d, out uint x, out uint y)
        {
            var flipped = false;
            var rotated = false;

            x = y = 0;
            for (int logs = 3; logs >= 0; logs--)
            {
                var trans = (d >> (logs * 16)) & 0xFFFF;

                uint rx = Inverted[2 * trans + 0];
                uint ry = Inverted[2 * trans + 1];

                var flippedUpdate = Flipped[rx][ry];
                var rotatedUpdate = Rotated[rx][ry];

                if (flipped)
                {
                    var t = rx;
                    rx = ry;
                    ry = t;
                }

                if (rotated)
                {
                    rx = 0xFF - rx;
                    ry = 0xFF - ry;
                }

                flipped = flipped ^ flippedUpdate;
                rotated = rotated ^ rotatedUpdate;

                x = (x << 8) + rx;
                y = (y << 8) + ry;
            }
        }


        //rotate/flip a quadrant appropriately
        public static void rot(int logs, ref int x, ref int y, int rx, int ry)
        {
            if (ry == 0)
            {
                if (rx == 1)
                {
                    x = ((1 << logs) - 1 - x);
                    y = ((1 << logs) - 1 - y);
                }

                //Swap x and y
                int t = x;
                x = y;
                y = t;
            }
        }

        static bool[][] Rotated;
        static bool[][] Flipped;
        static UInt16[][] Transformed;

        static byte[] Inverted;

        public static void Test(int tests)
        {
            var random = new Random(0);

            var testxs = new uint[tests];
            var testys = new uint[tests];
            for (int i = 0; i < tests; i++)
            {
                testxs[i] = (uint)random.Next(Int32.MaxValue);
                testys[i] = (uint)random.Next(Int32.MaxValue);
            }

            var results1 = new long[tests];
            var results2 = new ulong[tests];

            for (int i = 0; i < results1.Length; i++)
                results1[i] = i;

            for (uint i = 0; i < results2.Length; i++)
                results2[i] = i;

            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            stopwatch.Restart();
            for (int i = 0; i < tests; i++)
                results1[i] = HilbertCurve.xy2d(32, (int)testxs[i], (int)testys[i]);

            Console.WriteLine("Slow encode:\t{0:0.00}ns/record", stopwatch.ElapsedMilliseconds / (tests / 1000000.0));

            stopwatch.Restart();
            for (int i = 0; i < tests; i++)
            {
                int x = 0, y = 0;
                HilbertCurve.d2xy(32, results1[i], out x, out y);
            }
            Console.WriteLine("Slow decode:\t{0:0.00}ns/record", stopwatch.ElapsedMilliseconds / (tests / 1000000.0));

            stopwatch.Restart();
            for (int i = 0; i < tests; i++)
                results2[i] = HilbertCurve.xy2dByte(testxs[i], testys[i]);

            Console.WriteLine("Fast encode:\t{0:0.00}ns/record", stopwatch.ElapsedMilliseconds / (tests / 1000000.0));

            stopwatch.Restart();
            for (int i = 0; i < tests; i++)
            {
                uint x = 0, y = 0;
                HilbertCurve.d2xyByte((ulong)results2[i], out x, out y);
            }
            Console.WriteLine("Fast decode:\t{0:0.00}ns/record", stopwatch.ElapsedMilliseconds / (tests / 1000000.0));

            for (int i = 0; i < tests; i++)
            {
                if (results1[i] != (long)results2[i])
                    Console.WriteLine("Error! ({0}, {1}) -> [ {2}, {3} ]", testxs[i], testys[i], results1[i], results2[i]);

                uint x = 0;
                uint y = 0;

                HilbertCurve.d2xyByte((ulong)results1[i], out x, out y);

                if (x != testxs[i] || y != testys[i])
                {
                    Console.WriteLine("Error!!! {4} -> ({0}, {2}) != ({1}, {3})", x, testxs[i], y, testys[i], results1[i]);
                    Console.ReadLine();
                }
            }
        }


        static HilbertCurve()
        {
            Rotated = new bool[256][];
            Flipped = new bool[256][];
            Transformed = new ushort[256][];

            Inverted = new byte[256 * 256 * 2];

            for (uint i = 0; i < 256; i++)
            {
                Rotated[i] = new bool[256];
                Flipped[i] = new bool[256];
                Transformed[i] = new ushort[256];

                for (uint j = 0; j < 256; j++)
                {
                    var transform = (UInt16)HilbertCurve.xy2dAlt(8, i, j, ref Rotated[i][j], ref Flipped[i][j]);

                    Transformed[i][j] = transform;
                    Inverted[2 * transform + 0] = (byte)i;
                    Inverted[2 * transform + 1] = (byte)j;
                }
            }

        }
    }

    public class MultiHilbertScanner
    {
        private readonly uint[] high32Buffer;
        private readonly ushort[] next16Buffer;
        private readonly byte[] next08Buffer;

        private readonly int[] last08ToRead;
        private readonly byte[] last08Buffer;

        private readonly string prefix;

        public MultiHilbertScanner(string prefix)
        {
            this.prefix = prefix;

            this.high32Buffer = UnbufferedIO.ReadFile<uint>(prefix + "-high32");
            this.next16Buffer = UnbufferedIO.ReadFile<ushort>(prefix + "-next16");
            this.next08Buffer = UnbufferedIO.ReadFile<byte>(prefix + "-next08");

            this.last08ToRead = new int[high32Buffer.Length / 2];

            var next16Offset = 0;
            var next08Offset = 0;

            for (int i = 0; i < high32Buffer.Length; i += 2)
            {
                var high32value = high32Buffer[i];
                var high32count = (int)high32Buffer[i + 1];

                var next16count = 0;
                for (int j = 0; j < high32count; j++)
                    next16count += (int)(next16Buffer[next16Offset + 2 * j + 1] == 0 ? ((uint)ushort.MaxValue) + 1 : next16Buffer[next16Offset + 2 * j + 1]);

                next16Offset += 2 * high32count;

                var next08count = 0;
                for (int k = 0; k < next16count; k++)
                    next08count += (int)(next08Buffer[next08Offset + 2 * k + 1] == 0 ? ((uint)byte.MaxValue) + 1 : next08Buffer[next08Offset + 2 * k + 1]);

                next08Offset += 2 * next16count;

                last08ToRead[i / 2] = next08count;
            }

            this.last08Buffer = new byte[last08ToRead.Max()];
        }

        public void Scan(Action<uint, uint, int, int, byte[]> perEdgeAction)
        {
            var next16Offset = 0;
            var next08Offset = 0;

            using (var last08Reader = new UnbufferedIO.SequentialReader(prefix + "-last08"))
            {
                for (int i = 0; i < high32Buffer.Length; i += 2)
                {
                    var high32value = high32Buffer[i];
                    var high32count = (int)high32Buffer[i + 1];

                    var readLast08 = last08Reader.Read<byte>(last08Buffer, 0, last08ToRead[i / 2]);
                    if (readLast08 != last08ToRead[i / 2])
                        Console.WriteLine("Read error, last08; requested {0}, read {1}", last08ToRead[i / 2], readLast08);

                    var last08Offset = 0;

                    // do work for all these dudes
                    var xHigh32 = (high32value & 0xFFFF0000) << 0;
                    var yHigh32 = (high32value & 0x0000FFFF) << 16;

                    for (int j = 0; j < high32count; j++)
                    {
                        var xNext16 = xHigh32 + (uint)((next16Buffer[next16Offset + 2 * j] & 0xFF00) << 0);
                        var yNext16 = yHigh32 + (uint)((next16Buffer[next16Offset + 2 * j] & 0x00FF) << 8);

                        var next16count = (int)next16Buffer[next16Offset + 2 * j + 1];
                        if (next16count == 0)
                            next16count = ((int)ushort.MaxValue) + 1;

                        for (int k = 0; k < next16count; k++)
                        {
                            var xNext08 = xNext16 + (uint)((next08Buffer[next08Offset + 2 * k] & 0xF0) << 0);
                            var yNext08 = yNext16 + (uint)((next08Buffer[next08Offset + 2 * k] & 0x0F) << 4);

                            var next08count = (int)next08Buffer[next08Offset + 2 * k + 1];
                            if (next08count == 0)
                                next08count = ((int)byte.MaxValue) + 1;

                            perEdgeAction(xNext08, yNext08, next08count, last08Offset, last08Buffer);

                            last08Offset += next08count;
                        }

                        next08Offset += 2 * next16count;
                    }

                    next16Offset += 2 * high32count;
                }
            }
        }
    }

    public static class HilbertConversion
    {
        public static void ConvertToHilbert(this string prefix)
        {
            var graph = new FileGraphScanner(prefix);

            var High32 = new Dictionary<UInt32, UInt32>();

            graph.ForEach((vertex, degree, offset, neighbors) =>
            {
                //if (vertex < 1000000)
                {
                    for (int i = 0; i < degree; i++)
                    {
                        var neighbor = neighbors[offset + i];
                        var high32 = (uint)(HilbertCurve.xy2dByte((uint)vertex, (uint)neighbor) >> 32);

                        if (!High32.ContainsKey(high32))
                            High32.Add(high32, 0);

                        High32[high32] = High32[high32] + 1;
                    }
                }
            });

            Console.WriteLine("Assesed prefix sizes");

            var pairs = High32.OrderBy(x => x.Key).ToArray();

            var edgeCount = 0L;

            var lowIndex = (uint) 0;

            var high32Stream = new System.IO.BinaryWriter(System.IO.File.OpenWrite(prefix + "-high32"));
            var next16Stream = new System.IO.BinaryWriter(System.IO.File.OpenWrite(prefix + "-next16"));
            var next08Stream = new System.IO.BinaryWriter(System.IO.File.OpenWrite(prefix + "-next08"));
            var last08Stream = new System.IO.BinaryWriter(System.IO.File.OpenWrite(prefix + "-last08"));

            for (uint i = 0; i < pairs.Length; i++)
            {
                // if we would have too many edges, do work
                if (edgeCount + pairs[i].Value > 1 << 29)
                {
                    var edges = new ulong[edgeCount];
                    var cursor = 0;

                    graph.ForEach((vertex, degree, offset, neighbors) =>
                    {
                        //if (vertex < 1000000)
                        {
                            for (int j = 0; j < degree; j++)
                            {
                                var transform = HilbertCurve.xy2dByte((uint)vertex, (uint)neighbors[offset + j]);
                                if ((transform >> 32) >= pairs[lowIndex].Key && (transform >> 32) < pairs[i].Key)
                                    edges[cursor++] = transform;
                            }
                        }
                    });

                    if (cursor != edges.Length)
                        Console.WriteLine("Somehow read the wrong number of edges {0} vs {1}", cursor, edges.Length);

                    Array.Sort(edges);

                    Console.WriteLine("About to process {0} high32 blocks", i - lowIndex);

                    // traverse edges in order and write out when interesting things happen.
                    WriteTransformed(edges, high32Stream, next16Stream, next08Stream, last08Stream);

                    edgeCount = 0;
                    lowIndex = i;
                }

                edgeCount += pairs[i].Value;
            }

            var edges2 = new ulong[edgeCount];
            var cursor2 = 0;

            graph.ForEach((vertex, degree, offset, neighbors) =>
            {
                //if (vertex < 1000000)
                {
                    for (int j = 0; j < degree; j++)
                    {
                        var transform = HilbertCurve.xy2dByte((uint)vertex, (uint)neighbors[offset + j]);

                        if ((transform >> 32) >= pairs[lowIndex].Key)
                            edges2[cursor2++] = transform;
                    }
                }
            });

            Array.Sort(edges2);

            if (cursor2 != edges2.Length)
                Console.WriteLine("Somehow read the wrong number of edges {0} vs {1}", cursor2, edges2.Length);


            // traverse edges in order and write out when interesting things happen.
            WriteTransformed(edges2, high32Stream, next16Stream, next08Stream, last08Stream);

            high32Stream.Close();
            next16Stream.Close();
            next08Stream.Close();
            last08Stream.Close();
        }

        private static void WriteTransformed(ulong[] edges, 
                                             System.IO.BinaryWriter high32Stream, 
                                             System.IO.BinaryWriter next16Stream, 
                                             System.IO.BinaryWriter next08Stream, 
                                             System.IO.BinaryWriter last08Stream)
        {
            Console.WriteLine("Writing {0} edges", edges.Length);

            if (edges.Length == 0)
                return;

            uint x = 0;
            uint y = 0;

            // recover x and y, to interleave their bits
            HilbertCurve.d2xyByte(edges[0], out x, out y);

            // interleave each bit pattern we want to collect together
            var high32Prior = (uint)   ((x & 0xFFFF0000) + ((y & 0xFFFF0000) >> 16));
            var next16Prior = (ushort) ((x & 0x0000FF00) + ((y & 0x0000FF00) >> 8));
            var next08Prior = (byte)   ((x & 0x000000F0) + ((y & 0x000000F0) >> 4));

            // count distinct subsequent bit patterns
            var high32Count = (uint)1;
            var next16Count = (uint)1;
            var next08Count = (uint)1;

            last08Stream.Write((byte)(((x & 0x0F) << 4) + (y & 0x0F)));

            for (int i = 1; i < edges.Length; i++)
            {
                // recover x and y, to interleave their bits
                HilbertCurve.d2xyByte(edges[i], out x, out y);

                var high32 = (uint)   ((x & 0xFFFF0000) + ((y & 0xFFFF0000) >> 16));
                var next16 = (ushort) ((x & 0x0000FF00) + ((y & 0x0000FF00) >> 8));
                var next08 = (byte)   ((x & 0x000000F0) + ((y & 0x000000F0) >> 4));

                // changes in bits call for writing out
                var high32Change = high32 != high32Prior;
                var next16Change = high32Change || (next16 != next16Prior);
                var next08Change = next16Change || (next08 != next08Prior);

                // write high 32, reset high32count
                if (high32Change)
                {
                    high32Stream.Write(high32Prior);
                    high32Stream.Write(high32Count);

                    high32Prior = high32;
                    high32Count = 0;
                }

                // write next 16, reset nex16count, bump high32count
                if (next16Change)
                {
                    next16Stream.Write(next16Prior);
                    next16Stream.Write((ushort)next16Count);
                    high32Count++;

                    next16Prior = next16;
                    next16Count = 0;
                }
                
                // write next 08, reset next08count, bump next16count
                if (next08Change)
                {
                    next08Stream.Write(next08Prior);
                    next08Stream.Write((byte) next08Count);
                    next16Count++;

                    next08Prior = next08;
                    next08Count = 0;
                }


                // always write the last byte, bump next08count
                last08Stream.Write((byte)(((x & 0x0F) << 4) + (y & 0x0F)));
                next08Count++;
            }

            // flush final metadata records
            next08Stream.Write(next08Prior);
            next08Stream.Write(next08Count);

            next16Stream.Write(next16Prior);
            next16Stream.Write(next16Count);
            
            high32Stream.Write(high32Prior);
            high32Stream.Write(high32Count);
        }

        public static void Scan(this string prefix, Action<uint, uint, int, int, byte[]> perEdgeAction)
        {
            var high32MetaData = UnbufferedIO.ReadFile<uint>(prefix + "-high32");

            var next16Reader = new UnbufferedIO.SequentialReader(prefix + "-next16");
            var next08Reader = new UnbufferedIO.SequentialReader(prefix + "-next08");
            var last08Reader = new UnbufferedIO.SequentialReader(prefix + "-last08");

            var next16Buffer = new ushort[] { };
            var next08Buffer = new byte[] { };
            var last08Buffer = new byte[] { };

            for (int i = 0; i < high32MetaData.Length; i += 2)
            {
                var high32value = high32MetaData[i];
                var high32count = (int)high32MetaData[i + 1];

                if (next16Buffer.Length < 2 * high32count)
                    next16Buffer = new ushort[2 * high32count];

                var readNext16 = next16Reader.Read<ushort>(next16Buffer, 0, 2 * high32count);
                if (readNext16 != 2 * high32count)
                    Console.WriteLine("Read error, next16; requested {0}, read {1}", 2 * high32count, readNext16);


                var next16count = 0;
                for (int j = 0; j < high32count; j++)
                    next16count += (int)(next16Buffer[2 * j + 1] == 0 ? ((uint)ushort.MaxValue) + 1 : next16Buffer[2 * j + 1]);

                if (next08Buffer.Length < 2 * next16count)
                    next08Buffer = new byte[2 * next16count];

                var readNext08 = next08Reader.Read<byte>(next08Buffer, 0, 2 * next16count);
                if (readNext08 != 2 * next16count)
                    Console.WriteLine("Read error, next08; requested {0}, read {1}", 2 * next16count, readNext08);


                var next08count = 0;
                for (int k = 0; k < next16count; k++)
                    next08count += (int)(next08Buffer[2 * k + 1] == 0 ? ((uint)byte.MaxValue) + 1 : next08Buffer[2 * k + 1]);

                if (last08Buffer.Length < next08count)
                    last08Buffer = new byte[next08count];

                var readLast08 = last08Reader.Read<byte>(last08Buffer, 0, next08count);
                if (readLast08 != next08count)
                    Console.WriteLine("Read error, last08; requested {0}, read {1}", next08count, readLast08);

                var next16Offset = 0;
                var next08Offset = 0;
                var last08Offset = 0;

                // do work for all these dudes
                var xHigh32 = (high32value & 0xFFFF0000) << 0;
                var yHigh32 = (high32value & 0x0000FFFF) << 16;

                for (int j = 0; j < high32count; j++)
                {
                    var xNext16 = xHigh32 + (uint) ((next16Buffer[next16Offset + 2 * j] & 0xFF00) << 0);
                    var yNext16 = yHigh32 + (uint) ((next16Buffer[next16Offset + 2 * j] & 0x00FF) << 8);

                    next16count = next16Buffer[next16Offset + 2 * j + 1];
                    if (next16count == 0)
                        next16count = ((int)ushort.MaxValue) + 1;

                    for (int k = 0; k < next16count; k++)
                    {
                        var xNext08 = xNext16 + (uint) ((next08Buffer[next08Offset + 2 * k] & 0xF0) << 0);
                        var yNext08 = yNext16 + (uint) ((next08Buffer[next08Offset + 2 * k] & 0x0F) << 4);

                        next08count = next08Buffer[next08Offset + 2 * k + 1];
                        if (next08count == 0)
                            next08count = ((int)byte.MaxValue) + 1;

                        perEdgeAction(xNext08, yNext08, next08count, last08Offset, last08Buffer);

                        last08Offset += next08count;
                    }

                    next08Offset += 2 * next16count;
                }
            }

            next16Reader.Dispose();
            next08Reader.Dispose();
            last08Reader.Dispose();
        }

        public static void ScanAlt(this string prefix, Action<uint, uint, int, int, byte[]> perEdgeAction)
        {
            var high32Buffer = UnbufferedIO.ReadFile<uint>(prefix + "-high32");
            var next16Buffer = UnbufferedIO.ReadFile<ushort>(prefix + "-next16");
            var next08Buffer = UnbufferedIO.ReadFile<byte>(prefix + "-next08");
            
            var last08ToRead = new int[high32Buffer.Length / 2];

            var next16Offset = 0;
            var next08Offset = 0;

            for (int i = 0; i < high32Buffer.Length; i += 2)
            {
                var high32value = high32Buffer[i];
                var high32count = (int)high32Buffer[i + 1];

                var next16count = 0;
                for (int j = 0; j < high32count; j++)
                    next16count += (int)(next16Buffer[next16Offset + 2 * j + 1] == 0 ? ((uint)ushort.MaxValue) + 1 : next16Buffer[next16Offset + 2 * j + 1]);

                next16Offset += 2 * high32count;

                var next08count = 0;
                for (int k = 0; k < next16count; k++)
                    next08count += (int)(next08Buffer[next08Offset + 2 * k + 1] == 0 ? ((uint)byte.MaxValue) + 1 : next08Buffer[next08Offset + 2 * k + 1]);

                next08Offset += 2 * next16count;

                last08ToRead[i / 2] = next08count;
            }

            next16Offset = 0;
            next08Offset = 0;

            var last08Reader = new UnbufferedIO.SequentialReader(prefix + "-last08");
            var last08Buffer = new byte[last08ToRead.Max()];

            for (int i = 0; i < high32Buffer.Length; i += 2)
            {
                var high32value = high32Buffer[i];
                var high32count = (int)high32Buffer[i + 1];

                var readLast08 = last08Reader.Read<byte>(last08Buffer, 0, last08ToRead[i / 2]);
                if (readLast08 != last08ToRead[i / 2])
                    Console.WriteLine("Read error, last08; requested {0}, read {1}", last08ToRead[i / 2], readLast08);

                var last08Offset = 0;

                // do work for all these dudes
                var xHigh32 = (high32value & 0xFFFF0000) << 0;
                var yHigh32 = (high32value & 0x0000FFFF) << 16;

                for (int j = 0; j < high32count; j++)
                {
                    var xNext16 = xHigh32 + (uint)((next16Buffer[next16Offset + 2 * j] & 0xFF00) << 0);
                    var yNext16 = yHigh32 + (uint)((next16Buffer[next16Offset + 2 * j] & 0x00FF) << 8);

                    var next16count = (int) next16Buffer[next16Offset + 2 * j + 1];
                    if (next16count == 0)
                        next16count = ((int)ushort.MaxValue) + 1;

                    for (int k = 0; k < next16count; k++)
                    {
                        var xNext08 = xNext16 + (uint)((next08Buffer[next08Offset + 2 * k] & 0xF0) << 0);
                        var yNext08 = yNext16 + (uint)((next08Buffer[next08Offset + 2 * k] & 0x0F) << 4);

                        var next08count = (int) next08Buffer[next08Offset + 2 * k + 1];
                        if (next08count == 0)
                            next08count = ((int)byte.MaxValue) + 1;

                        perEdgeAction(xNext08, yNext08, next08count, last08Offset, last08Buffer);

                        last08Offset += next08count;
                    }

                    next08Offset += 2 * next16count;
                }

                next16Offset += 2 * high32count;
            }

            last08Reader.Dispose();
        }


        public static void TestTransformed(string prefix)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            var high32MetaData = UnbufferedIO.ReadFile<uint>(prefix + "-high32");

            var next16Reader = new UnbufferedIO.SequentialReader(prefix + "-next16");
            var next08Reader = new UnbufferedIO.SequentialReader(prefix + "-next08");
            var last08Reader = new UnbufferedIO.SequentialReader(prefix + "-last08");

            var next16Buffer = new ushort[] { };
            var next08Buffer = new byte[] { };
            var last08Buffer = new byte[] { };

            var iDegrees = new uint[106000000];
            var oDegrees = new uint[106000000];

            var edgeCount = 0L;

            for (int i = 0; i < high32MetaData.Length; i += 2)
            {
                var high32value = high32MetaData[i];
                var high32count = (int) high32MetaData[i + 1];

                if (next16Buffer.Length < 2 * high32count)
                    next16Buffer = new ushort[2 * high32count];

                var readNext16 = next16Reader.Read<ushort>(next16Buffer, 0, 2 * high32count);
                if (readNext16 != 2 * high32count)
                    Console.WriteLine("Read error, next16; requested {0}, read {1}", 2 * high32count, readNext16);


                var next16count = 0;
                for (int j = 0; j < high32count; j++)
                    next16count += (int) (next16Buffer[2 * j + 1] == 0 ? ((uint) ushort.MaxValue) + 1 : next16Buffer[2 * j + 1]);

                if (next08Buffer.Length < 2 * next16count)
                    next08Buffer = new byte[2 * next16count];

                var readNext08 = next08Reader.Read<byte>(next08Buffer, 0, 2 * next16count);
                if (readNext08 != 2 * next16count)
                    Console.WriteLine("Read error, next08; requested {0}, read {1}", 2 * next16count, readNext08);


                var next08count = 0;
                for (int k = 0; k < next16count; k++)
                    next08count += (int) (next08Buffer[2 * k + 1] == 0 ? ((uint) byte.MaxValue) + 1 : next08Buffer[2 * k + 1]);

                if (last08Buffer.Length < next08count)
                    last08Buffer = new byte[next08count];

                var readLast08 = last08Reader.Read<byte>(last08Buffer, 0, next08count);
                if (readLast08 != next08count)
                    Console.WriteLine("Read error, last08; requested {0}, read {1}", next08count, readLast08);


                var next16Offset = 0;
                var next08Offset = 0;
                var last08Offset = 0;

                // do work for all these dudes
                var xHigh32 = (high32value & 0xFFFF0000) << 0;
                var yHigh32 = (high32value & 0x0000FFFF) << 16;

                for (int j = 0; j < high32count; j++)
                {
                    var xNext16 = xHigh32 + ((next16Buffer[next16Offset + 2 * j] & 0xFF00) << 0);
                    var yNext16 = yHigh32 + ((next16Buffer[next16Offset + 2 * j] & 0x00FF) << 8);

                    next16count = next16Buffer[next16Offset + 2 * j + 1];
                    if (next16count == 0)
                        next16count = ((int)ushort.MaxValue) + 1;

                    for (int k = 0; k < next16count; k++)
                    {
                        var xNext08 = xNext16 + ((next08Buffer[next08Offset + 2 * k] & 0xF0) << 0);
                        var yNext08 = yNext16 + ((next08Buffer[next08Offset + 2 * k] & 0x0F) << 4);

                        next08count = next08Buffer[next08Offset + 2 * k + 1];
                        if (next08count == 0)
                            next08count = ((int)byte.MaxValue) + 1;

                        for (int l = 0; l < next08count; l++)
                        {
                            var x = xNext08 + ((last08Buffer[last08Offset + l] & 0xF0) >> 4);
                            var y = yNext08 + ((last08Buffer[last08Offset + l] & 0x0F) >> 0);

                            iDegrees[y]++;
                            oDegrees[x]++;

                            edgeCount++;
                        }

                        last08Offset += next08count;
                    }

                    next08Offset += 2 * next16count;
                }
            }

            Console.WriteLine("{1}\tProcessed\t{0} edges", edgeCount, stopwatch.Elapsed);

            next16Reader.Dispose();
            next08Reader.Dispose();
            last08Reader.Dispose();
            
            
            var graph = new FileGraphScanner(prefix);

            graph.ForEach((vertex, degree, offset, neighbors) =>
            {
                for (int i = 0; i < degree; i++)
                {
                    iDegrees[neighbors[offset + i]]--;
                    oDegrees[vertex]--;
                }
            });

            var iErrors = iDegrees.Count(x => x > 0);
            var oErrors = oDegrees.Count(x => x > 0);

            Console.WriteLine("iErrors: {0}\toErrors: {1}", iErrors, oErrors);

            Console.WriteLine("{0}\tCounted {1} edges", stopwatch.Elapsed, edgeCount);
        }

        public static void TestTransformedAlt(string prefix)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            var high32MetaData = UnbufferedIO.ReadFile<uint>(prefix + "-high32");

            var high32Counts = 0L;
            for (int i = 0; i < high32MetaData.Length; i += 2)
                high32Counts += high32MetaData[i + 1];

            Console.WriteLine("Read {0} high32, expecting {1} next16", high32MetaData.Length / 2, high32Counts);

            var next16Total = 0L;
            var next16Count = 0L;
            var next16Buffer = new ushort[1 << 20];
            using (var next16Reader = new UnbufferedIO.SequentialReader(prefix + "-next16"))
            {
                for (var read = next16Reader.Read<ushort>(next16Buffer, 0, 1 << 20); read > 0; read = next16Reader.Read<ushort>(next16Buffer, 0, 1 << 20))
                {
                    next16Total += read / 2;
                    for (int i = 0; i < read; i += 2)
                        next16Count += next16Buffer[i + 1];
                }
            }

            Console.WriteLine("Read {0} next16, expecting {1} next08", next16Total, next16Count);

            var next08Total = 0L;
            var next08Count = 0L;
            var next08Buffer = new byte[1 << 20];
            using (var next08Reader = new UnbufferedIO.SequentialReader(prefix + "-next08"))
            {
                for (var read = next08Reader.Read<byte>(next08Buffer, 0, 1 << 20); read > 0; read = next08Reader.Read<byte>(next08Buffer, 0, 1 << 20))
                {
                    next08Total += read / 2;
                    for (int i = 0; i < read; i += 2)
                        next08Count += next08Buffer[i + 1] == 0 ? 256 : (int) next08Buffer[i + 1];
                }
            }

            Console.WriteLine("Read {0} next08, expecting {1} last08", next08Total, next08Count);


            Console.WriteLine(stopwatch.Elapsed);
        }

    }

}
