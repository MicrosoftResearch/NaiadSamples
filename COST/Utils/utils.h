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

#pragma once

#ifndef _UTILS_H_
#define _UTILS_H_

#include <windows.h>
#include <winioctl.h>
#include <winbase.h>
#include <assert.h>
#include <strsafe.h>
#include <stdio.h>

#define ASSERTS
#ifndef ASSERTS
	#define ASSERT(cond)
#else /* ASSERTS */
	#define ASSERT(cond)                                 \
		if (!(cond)) {                                   \
			fprintf(stderr, "\nAssertion failed\n");     \
		}                                                \
	assert(cond);
#endif /* ASSERTS */

void				ErrorExit(LPTSTR lpszFunction);
void				Error(LPTSTR lpszFunction);
unsigned long long	get_time_64ms(void);
unsigned long long	get_time_64us(LARGE_INTEGER begin_ticks, LARGE_INTEGER end_ticks);
unsigned long long	get_ticks(LARGE_INTEGER begin_ticks, LARGE_INTEGER end_ticks);
int					get_rand(unsigned int min, unsigned int max);
void				seed_rand_gen();
void				seed_rand_gen(int seed);
LPWSTR				MultiCharToUniChar(char* mbString);

//int GetAPICId();

#endif 
