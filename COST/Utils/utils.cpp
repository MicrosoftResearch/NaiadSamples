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

#include <intrin.h>
#include "utils.h"

LARGE_INTEGER ticks_per_second;
LARGE_INTEGER ticks_per_us;

/* 
 * Copied this function from MSDN.
 *
 * Retrieve the system error message for the 
 * last-error code and exit the process.
 */
void ErrorExit(LPTSTR lpszFunction) 
{
	LPVOID lpMsgBuf;

	DWORD dw = GetLastError();
	FormatMessage(
		FORMAT_MESSAGE_ALLOCATE_BUFFER | 
		FORMAT_MESSAGE_FROM_SYSTEM |
		FORMAT_MESSAGE_IGNORE_INSERTS,
		NULL,
		dw,
		MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
		(LPTSTR) &lpMsgBuf,
		0, NULL );

	// Display the error message and exit the process
	wprintf(L"%s: %s", lpszFunction, (LPTSTR)lpMsgBuf);

	ASSERT(0);
	LocalFree(lpMsgBuf);
	ExitProcess(dw);
}

void Error(LPTSTR lpszFunction)
{
	LPVOID lpMsgBuf;

	DWORD dw = GetLastError();
	FormatMessage(
		FORMAT_MESSAGE_ALLOCATE_BUFFER | 
		FORMAT_MESSAGE_FROM_SYSTEM |
		FORMAT_MESSAGE_IGNORE_INSERTS,
		NULL,
		dw,
		MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
		(LPTSTR) &lpMsgBuf,
		0, NULL );

	// Display the error message and exit the process
	wprintf(L"%s: %s", lpszFunction, (LPTSTR)lpMsgBuf);
}

/* 
 * Return time, In ms (10^-3 second) 
 */
unsigned __int64 get_time_64ms(void)
{
	FILETIME time;
	ULARGE_INTEGER now_value;

	GetSystemTimeAsFileTime(&time);
	now_value.LowPart = time.dwLowDateTime;
	now_value.HighPart = time.dwHighDateTime;
	return now_value.QuadPart /10000; 
}

/* 
 * Return time, In us (10^-6 second).
 * The begin_ticks and end_ticks are measured by calling
 * QueryPerformanceCounter() function.
 */
unsigned long long get_time_64us(LARGE_INTEGER begin_ticks, LARGE_INTEGER end_ticks)
{
	static BOOL clock_freq_init = FALSE;
	LARGE_INTEGER elapsed_time;

	if (!clock_freq_init) {
		
		// get the high resolution counter's accuracy
		if (!(clock_freq_init = QueryPerformanceFrequency(&ticks_per_second))) {
			ErrorExit(TEXT("QueryPerformanceFrequency"));
		}

		ticks_per_us.QuadPart = ticks_per_second.QuadPart / 1000000;
		//printf("%I64Ld ticks/sec %I64Ld ticks/usec\n", ticks_per_second, ticks_per_us);
	}

	elapsed_time.QuadPart = end_ticks.QuadPart - begin_ticks.QuadPart;
	return (elapsed_time.QuadPart * 1000000) / ticks_per_second.QuadPart;
}

/* 
 * Return elapsed ticks.
 * The begin_ticks and end_ticks are measured by calling
 * QueryPerformanceCounter() function.
 */
unsigned long long get_ticks(LARGE_INTEGER begin_ticks, LARGE_INTEGER end_ticks)
{
	unsigned long long elapsed_ticks = end_ticks.QuadPart - begin_ticks.QuadPart;
	return elapsed_ticks;
}

/*
 * Returns a random number between [min, max],
 * where min < max < RAND_MAX.
 */
int get_rand(unsigned int min, unsigned int max)
{
	int j;
	j = min + (int) ((max+1) * (rand() / (RAND_MAX + 1.0)));

	return j;
}

/*
 * Seeds the random number generator with a random seed.
 */
void seed_rand_gen()
{
	BOOL result;
	LARGE_INTEGER ticks;

	if (!(result = QueryPerformanceCounter(&ticks))) {
		ErrorExit(TEXT("QueryPerformanceCounter"));
	}

	//printf("Seeding the rand gen w/ %lld\n", ticks.LowPart);
	srand(ticks.LowPart);
}

/*
 * Seeds the random number generator with a particular seed.
 */
void seed_rand_gen(int seed)
{
	srand(seed);
}

/*
 * Converts LPSTR to LPWSTR.
 */
LPWSTR MultiCharToUniChar(char* mbString)
{
	size_t len = strlen(mbString) + 1;
	wchar_t *ucString = new wchar_t[len];
    size_t nConverted;
	mbstowcs_s(&nConverted, ucString, len, mbString, len);
	return (LPWSTR)ucString;
}

#if 0
int GetAPICId()
{
	int CPUInfo[4];
	__cpuid(CPUInfo, 1);
    int nAPICId = (CPUInfo[1] >> 24) & 0xff;
	return nAPICId;
}
#endif
