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

using System.IO;
using System.Threading;

namespace COST
{

    public static class UnbufferedIO
    {
        public class SequentialWriter<T> : IDisposable
        {
            readonly string filename;           // name of target file
            FileStream target;                  // file stream used for writing

            readonly byte[] buffer;             // buffer for async writes
            readonly IAsyncResult[] results;    // stores async write results

            readonly int bufferSize = 1 << 20;  // number of bytes per write
            int currentIO = 0;                  // number of block writes done

            readonly T[] typedBuffer;       // staging area for block writes
            int offset = 0;                     // position in the current buffer

            public void Write(T data)
            {
                typedBuffer[offset++] = data;

                if (offset == typedBuffer.Length)
                {
                    this.Write(typedBuffer);
                    offset = 0;
                }
            }

            public void Write(T[] data)
            {
                if (results[(currentIO % results.Length)] != null)
                    target.EndWrite(results[(currentIO % results.Length)]);

                System.Buffer.BlockCopy(data, 0, buffer, bufferSize * (currentIO % results.Length), bufferSize);

                results[(currentIO % results.Length)] = target.BeginWrite(buffer, bufferSize * (currentIO % results.Length), bufferSize, null, null);

                currentIO++;
            }

            public void Close()
            {
                for (int i = 0; i < results.Length; i++)
                    if (results[i] != null)
                        target.EndWrite(results[i]);

                target.Flush();
                target.Close();
                target = null;

                System.Buffer.BlockCopy(typedBuffer, 0, buffer, 0, offset * 4);
                using (var file = new FileStream(filename, FileMode.Append, FileAccess.Write, FileShare.None))
                    file.Write(buffer, 0, offset * 4);
            }

            public void Dispose() { if (target != null) Close(); }

            public SequentialWriter(string filename, long expectedSize = 0)
            {
                this.filename = filename;

                // append not yet supported
                if (File.Exists(filename))
                    File.Delete(filename);

                target = new FileStream(filename, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None, 8, FileOptions.Asynchronous | FileOptions.SequentialScan | FileOptions.WriteThrough); //  | (FileOptions)0x20000000

                results = new IAsyncResult[4];

                buffer = new byte[results.Length * bufferSize];
                typedBuffer = new T[bufferSize / System.Runtime.InteropServices.Marshal.SizeOf(typeof(T))];
            }
        }

        public class SequentialReader<T> : IDisposable
        {
            public readonly long fileLength;    // file length in bytes
            public readonly string filename;    // name of source file

            readonly byte[] buffer;             // buffers for async reads
            readonly int requestLength;         // number of bytes per read
            readonly IAsyncResult[] results;    // results of async reads

            readonly int typeSize;

            FileStream source;                  // source file stream
            int currentIO;                      // numbef of block reads

            readonly T[][] typedBuffers;
            AutoResetEvent[] waitEvents;

            public void EndRead(int index, IAsyncResult result)
            {
                if (source != null)
                {
                    var read = source.EndRead(result);
                    System.Buffer.BlockCopy(buffer, requestLength * index, typedBuffers[index], 0, read);
                    waitEvents[index].Set();
                }
            }

            public int Read(ref T[] dest)
            {
                if (dest == null || dest.Length != requestLength / this.typeSize)
                    dest = new T[requestLength / this.typeSize];

                // if we have a full block to read in ...
                if (currentIO < fileLength / requestLength)
                {
                    var index = currentIO % results.Length;

                    waitEvents[index].WaitOne();

                    var temp = typedBuffers[index];
                    typedBuffers[index] = dest;
                    dest = temp;

                    if (((long)requestLength * (currentIO + results.Length + 1)) <= fileLength)
                        source.BeginRead(buffer, requestLength * index, requestLength, x => this.EndRead(index, x), null);

                    currentIO++;

                    return dest.Length;
                }
                else if (currentIO <= fileLength / requestLength)
                {
                    source.Close();

                    source = new FileStream(filename, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
                    source.Seek(-(fileLength % requestLength), SeekOrigin.End);

                    var read = source.Read(buffer, 0, (int)(fileLength % requestLength));

                    dest = new T[read / System.Runtime.InteropServices.Marshal.SizeOf(typeof(T))];

                    System.Buffer.BlockCopy(buffer, 0, dest, 0, read);

                    source.Close();
                    source = null;

                    currentIO++;

                    return read / System.Runtime.InteropServices.Marshal.SizeOf(typeof(T));
                }
                else
                    return 0;
            }

            public void Dispose()
            {
                for (int i = 0; i < results.Length; i++)
                    if (results[i] != null && !results[i].IsCompleted)
                        waitEvents[i].WaitOne();

                if (source != null) { source.Close(); source = null; }
            }

            public SequentialReader(string filename, int depth = 4)
            {
                this.filename = filename;

                source = new FileStream(filename, FileMode.OpenOrCreate, FileAccess.Read, FileShare.None, 8, FileOptions.Asynchronous | FileOptions.SequentialScan | FileOptions.WriteThrough | (FileOptions)0x20000000);

                results = new IAsyncResult[depth];
                requestLength = 1 << 20;

                buffer = new byte[depth * requestLength];

                this.typeSize = System.Runtime.InteropServices.Marshal.SizeOf(typeof(T));

                typedBuffers = new T[depth][];
                for (int i = 0; i < typedBuffers.Length; i++)
                    typedBuffers[i] = new T[requestLength / this.typeSize];

                waitEvents = new AutoResetEvent[depth];
                for (int i = 0; i < waitEvents.Length; i++)
                    waitEvents[i] = new AutoResetEvent(false);

                fileLength = source.Length;

                for (int i = 0; i < results.Length; i++)
                {
                    var index = i;
                    if (i < fileLength / requestLength)
                        source.BeginRead(buffer, requestLength * i, requestLength, x => this.EndRead(index, x), null);
                }
            }

            public static IEnumerable<T> Enumerate(string filename)
            {
                var readBuffer = new T[] { };
                using (var reader = new SequentialReader<T>(filename))
                {
                    while (reader.Read(ref readBuffer) > 0)
                        for (int i = 0; i < readBuffer.Length; i++)
                            yield return readBuffer[i];
                }
            }

            public static IEnumerable<T[]> EnumerateArrays(string filename)
            {
                var readBuffer = new T[] { };
                using (var reader = new SequentialReader<T>(filename))
                {
                    while (reader.Read(ref readBuffer) > 0)
                        yield return readBuffer;
                }
            }
        }

        public class SequentialReader : IDisposable
        {
            public readonly long Length;                // file length in bytes
            public readonly string filename;            // name of source file

            private readonly byte[] buffer;             // buffers for async reads
            private readonly int requestLength;         // number of bytes per read
            private readonly IAsyncResult[] results;    // results of async reads

            private readonly FileStream source;         // source file stream

            private long position;

            public int Read<T>(T[] dest, int offset, int length)
            {
                var copied = 0;

                while (copied < length && this.position != this.Length)
                {
                    var typeSize = System.Runtime.InteropServices.Marshal.SizeOf(typeof(T));

                    // only snag as many records as are valid, and needed.
                    var toCopy = length - copied;

                    // if we would read over the segment boundary, limit read.
                    if (toCopy > (this.requestLength - (this.position % this.requestLength)) / typeSize)
                        toCopy = (int)(this.requestLength - (this.position % this.requestLength)) / typeSize;

                    // if we would read past the end of file, don't.
                    if (toCopy > (this.Length - this.position) / typeSize)
                        toCopy = (int)(this.Length - this.position) / typeSize;

                    Buffer.BlockCopy(this.buffer, (int)(this.position % this.buffer.Length), dest, typeSize * (offset + copied), typeSize * toCopy);

                    copied += toCopy;
                    this.position += typeSize * toCopy;

                    // we may have emptied a hunk of data, and should start the next
                    if ((this.position % this.requestLength) == 0)
                    {
                        var offsetToRead = this.position + this.buffer.Length - this.requestLength;

                        // initiate new read request, only if there is a full valid page!
                        if (offsetToRead / this.requestLength < this.Length / this.requestLength)
                        {
                            var nextReadPosition = (int)((this.position + this.buffer.Length - this.requestLength) % this.buffer.Length);
                            results[nextReadPosition / this.requestLength] = source.BeginRead(this.buffer, nextReadPosition, this.requestLength, null, null);
                        }

                        // validate the forthcoming data
                        if (this.Length - this.position < this.requestLength)
                        {
                            this.source.Close();

                            using (var finalSource = new FileStream(filename, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read))
                            {
                                finalSource.Seek(-(Length % requestLength), SeekOrigin.End);
                                finalSource.Read(buffer, (int)((this.position) % this.buffer.Length), (int)(Length % requestLength));
                            }
                        }
                        else if (!results[(position % this.buffer.Length) / this.requestLength].IsCompleted)
                            source.EndRead(results[(position % this.buffer.Length) / this.requestLength]);
                    }
                }

                return copied;
            }

            public void Dispose()
            {
                for (int i = 0; i < this.results.Length; i++)
                    if (this.results[i] != null && !this.results[i].IsCompleted)
                        this.results[i].AsyncWaitHandle.WaitOne();

                if (source != null) { source.Close(); }
            }

            public SequentialReader(string filename) : this(filename, 4) { }

            public SequentialReader(string filename, int depth)
            {
                this.filename = filename;

                var options = FileOptions.Asynchronous | FileOptions.SequentialScan | FileOptions.WriteThrough | (FileOptions)0x20000000;

                this.source = new FileStream(filename, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite, 8, options);

                this.results = new IAsyncResult[depth];
                this.requestLength = 1 << 24;

                this.buffer = new byte[depth * this.requestLength];

                this.Length = source.Length;

                for (int i = 0; i < this.results.Length; i++)
                {
                    var readPosition = i * this.requestLength;

                    if (readPosition / this.requestLength < this.Length / this.requestLength)
                        this.results[i] = this.source.BeginRead(this.buffer, readPosition, this.requestLength, null, null);
                }

                if (this.results[0] == null)
                {
                    this.source.Close();

                    using (var finalSource = new FileStream(filename, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read))
                    {
                        finalSource.Seek(-(Length % requestLength), SeekOrigin.End);
                        finalSource.Read(buffer, 0, (int)(Length % requestLength));
                    }
                }
                else if (!this.results[0].IsCompleted)
                    this.source.EndRead(this.results[0]);
            }
        }

        public static T[] ReadFile<T>(string filename)
        {
            using (var reader = new UnbufferedIO.SequentialReader(filename))
            {
                var result = new T[reader.Length / System.Runtime.InteropServices.Marshal.SizeOf(typeof(T))];

                // if the thing to read is bigger than 2GB, Buffer.BlockCopy will choke (yay 32 bits)
                if (((Int64)result.Length) * System.Runtime.InteropServices.Marshal.SizeOf(typeof(T)) > Int32.MaxValue)
                {
                    var intermediate = new T[1 << 20];
                    var cursor = 0;

                    while (reader.Read(intermediate, 0, intermediate.Length) > 0)
                    { 
                        Array.Copy(intermediate, 0, result, cursor, Math.Min(intermediate.Length, result.Length - cursor));
                        cursor += intermediate.Length;
                    }
                }
                else
                    reader.Read(result, 0, result.Length);

                return result;
            }

        }
    }
}
