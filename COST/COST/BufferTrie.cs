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
    public class BufferTrie<TValue>
    {
        public struct Pair
        {
            internal readonly Int32 Index;
            internal readonly TValue Value;

            public Pair(Int32 index, TValue value) { this.Index = index; this.Value = value; }
        }

        private readonly Pair[] Buffer;
        private readonly Int32[] ChildOffsets;

        private readonly Int32 BufferLength = (1 << 12);
        private readonly Int32 Height = 2;
        private readonly Int32 Bits = 4;

        private readonly Int32 MaxBits;
        private readonly Action<Pair[], int, int> Action;

        /// <summary>
        /// Constructs a new BufferTrie based on a maximum number of valid index bits and an action to apply to each buffer of data.
        /// </summary>
        /// <param name="maxBits">Maximum number of index bits to consider in input data.</param>
        /// <param name="action">Action to apply to each inserted buffer. Action may be delayed to improve cache locality.</param>
        public BufferTrie(int maxBits, Action<Pair[], int, int> action)
        {
            this.MaxBits = maxBits;

            this.Action = action;

            var count = 0;
            for (int i = 0; i <= this.Height; i++)
                count += (1 << (i * this.Bits));

            this.Buffer = new Pair[this.BufferLength * count];

            this.ChildOffsets = new int[count];
            for (int i = 0; i < this.ChildOffsets.Length; i++)
                this.ChildOffsets[i] = this.BufferLength * i;
        }

        /// <summary>
        /// Inserts a range of data into the trie, eventually to have the action called on it.
        /// </summary>
        /// <param name="sourceArray">array containing data to insert</param>
        /// <param name="sourceOffset">offset in array to start from</param>
        /// <param name="sourceCount">number of pairs to read from array</param>
        public void Insert(Pair[] sourceArray, int sourceOffset, int sourceCount)
        {
            this.InsertAt(sourceArray, sourceOffset, sourceCount, 0, 0);
        }

        private void InsertAt(Pair[] sourceArray, int sourceOffset, int sourceCount, int targetPosition, int targetHeight)
        {
            if (targetHeight < this.Height)
            {
                var inserted = 0;
                while (inserted < sourceCount)
                {
                    var maxBatchSize = this.BufferLength / 8;

                    var available = Math.Min(sourceCount - inserted, maxBatchSize);

                    var offset = (targetPosition << this.Bits) + 1;
                    var shift = (this.MaxBits - this.Bits * (targetHeight + 1));
                    var mask = ((1 << this.Bits) - 1);

                    // move some number of records from the source array segment to partitioned buffers.
                    for (int i = sourceOffset + inserted; i < sourceOffset + inserted + available; i++)
                    {
                        var childIndex = offset + ((sourceArray[i].Index >> shift) & mask);
                        this.Buffer[this.ChildOffsets[childIndex]++] = sourceArray[i];
                    }

                    inserted += available;

                    // test buffers to see if they lack space.
                    for (int i = 0; i < (1 << this.Bits); i++)
                    {
                        var childIndex = (targetPosition << this.Bits) + i + 1;
                        var childCount = this.ChildOffsets[childIndex] - (this.BufferLength * childIndex);

                        if (childCount + maxBatchSize >= this.BufferLength)
                        {
                            this.InsertAt(this.Buffer, this.BufferLength * childIndex, childCount, childIndex, targetHeight + 1);
                            this.ChildOffsets[childIndex] -= childCount;
                        }
                    }
                }
            }
            else
                this.Action(sourceArray, sourceOffset, sourceCount);
        }

        /// <summary>
        /// Calls the supplied action on elements lingering in internal buffers.
        /// </summary>
        public void Flush()
        {
            // walk the array backwards for FIFO action order.
            for (int i = this.ChildOffsets.Length - 1; i >= 0; i--)
            {
                if (this.ChildOffsets[i] > this.BufferLength * i)
                {
                    this.Action(this.Buffer, this.BufferLength * i, this.ChildOffsets[i] - (this.BufferLength * i));
                    this.ChildOffsets[i] = this.BufferLength * i;
                }
            }
        }
    }
}
