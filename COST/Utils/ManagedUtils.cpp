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

#include "large_page_utils.h"

using namespace System;
using namespace System::Runtime::InteropServices;

namespace ManagedUtils {

	public ref class ManagedUtils
	{
	public:
		IntPtr InitLargePages(unsigned long array_size_bytes)
		{
			int *x;
			_InitLargePages((int *&)x, array_size_bytes, false, false);
			return (IntPtr)x;
		}

		void CleanLargePages(IntPtr x)
		{
			_CleanLargePages((int*)(void*)x);
		}

		IntPtr InitSmallPages(unsigned long array_size_bytes)
		{
			int *x;
			_InitSmallPages((int *&)x, array_size_bytes, false, false);
			return (IntPtr)x;
		}

		void CleanSmallPages(IntPtr x)
		{
			_CleanSmallPages((int*)(void*)x);
		}

		ManagedUtils()	{ }
	};
}
