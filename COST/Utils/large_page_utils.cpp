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

LPCTSTR lpszPrivilege = _T("SeLockMemoryPrivilege");
HANDLE hToken;

BOOL SetPrivilege(
	HANDLE hToken,           // access token handle
	LPCTSTR lpszPrivilege,   // name of privilege to enable/disable
	BOOL bEnablePrivilege    // to enable (or disable privilege)
	)
{
	// Token privilege structure
	TOKEN_PRIVILEGES tp;

	// Used by local system to identify the privilege
	LUID luid;
 
	if(!LookupPrivilegeValue(
			NULL,                // lookup privilege on local system
			lpszPrivilege,    // privilege to lookup
			&luid))               // receives LUID of privilege
	{
		fprintf(stdout,"LookupPrivilegeValue() error: %u\n", GetLastError());
		return FALSE;
	}
 
	tp.PrivilegeCount = 1;
	tp.Privileges[0].Luid = luid;
 
	// Don't forget to disable the privileges after you enabled them,
	// or have already completed your task. Don't mess up your system :o)
	if(bEnablePrivilege)
	{
		   tp.Privileges[0].Attributes = SE_PRIVILEGE_ENABLED;
	}
	else
	{
		   tp.Privileges[0].Attributes = 0;
	}
 
	// Enable the privilege (or disable all privileges).
	if(!AdjustTokenPrivileges(
		   hToken,
		   FALSE, // If TRUE, function disables all privileges, if FALSE the function modifies privilege based on the tp
		   &tp,
		   sizeof(TOKEN_PRIVILEGES),
		   (PTOKEN_PRIVILEGES) NULL,
		   (PDWORD) NULL))
	{
		  fprintf(stdout,"AdjustTokenPrivileges() error: %u\n", GetLastError());
		  return FALSE;
	}
	return TRUE;
}

void _InitLargePages(int *&x, SIZE_T array_size_bytes, BOOL no_cache, BOOL write_combine)
{
	// Open a handle to the access token for the calling process. That is this running program
	if(!OpenProcessToken(GetCurrentProcess(), TOKEN_ADJUST_PRIVILEGES, &hToken))
	{
		fprintf(stdout,"OpenProcessToken() error %u\n", GetLastError());
		exit(0);
	}

	// Call the user defined SetPrivilege() function to enable and set the needed privilege
	if (!SetPrivilege(hToken, lpszPrivilege, TRUE))
	{
		fprintf(stdout,"SetPrivilege() error %u\n", GetLastError());
		exit(0);
	}

	SIZE_T largePageSize = GetLargePageMinimum();
	if (largePageSize == 0)
	{
		fprintf(stdout,"GetLargePageMinimum() error %u\n", GetLastError());
		scanf_s("%*c");
		exit(0);
	}

	SIZE_T allocSize;
	if(array_size_bytes < largePageSize)
	{
		allocSize = largePageSize;
	}
	else
	{
		SIZE_T factor = array_size_bytes / largePageSize;
		factor += (array_size_bytes % largePageSize > 0) ? 1 : 0;
		allocSize = factor * largePageSize;	
	}

	if (write_combine)
	{
		x = static_cast <int *> (:: VirtualAlloc (NULL, allocSize, MEM_LARGE_PAGES|MEM_COMMIT|MEM_RESERVE, PAGE_READWRITE|PAGE_WRITECOMBINE));
	}
	else if (no_cache)
	{
		x = static_cast <int *> (:: VirtualAlloc (NULL, allocSize, MEM_LARGE_PAGES|MEM_COMMIT|MEM_RESERVE, PAGE_READWRITE|PAGE_NOCACHE));
	}
	else
	{
		x = static_cast <int *> (:: VirtualAlloc (NULL, allocSize, MEM_LARGE_PAGES|MEM_COMMIT|MEM_RESERVE, PAGE_READWRITE));
	}

	if(x == NULL)
	{
		fprintf(stdout,"VirtualAlloc() error %u\n", GetLastError());
		if (!SetPrivilege(hToken, lpszPrivilege, FALSE))
		{
			fprintf(stdout,"SetPrivilege() error %u\n", GetLastError());
		}
		exit(0);
	}
}

void _InitSmallPages(int *&x, SIZE_T array_size_bytes, BOOL no_cache, BOOL write_combine)
{
	SIZE_T smallPageSize = 4 * 1024;
	SIZE_T allocSize;
	if(array_size_bytes < smallPageSize)
	{
		allocSize = smallPageSize;
	}
	else
	{
		SIZE_T factor = array_size_bytes / smallPageSize;
		factor += (array_size_bytes % smallPageSize > 0) ? 1 : 0;
		allocSize = factor * smallPageSize;	
	}

	if (write_combine)
	{
		x = static_cast <int *> (:: VirtualAlloc (NULL, allocSize, MEM_COMMIT|MEM_RESERVE, PAGE_READWRITE|PAGE_WRITECOMBINE));
	}
	else if (no_cache)
	{
		x = static_cast <int *> (:: VirtualAlloc (NULL, allocSize, MEM_COMMIT|MEM_RESERVE, PAGE_READWRITE|PAGE_NOCACHE));
	}
	else
	{
		x = static_cast <int *> (:: VirtualAlloc (NULL, allocSize, MEM_COMMIT|MEM_RESERVE, PAGE_READWRITE));
	}

	if(x == NULL)
	{
		fprintf(stdout,"VirtualAlloc() error %u\n", GetLastError());
		exit(0);
	}
}

void _CleanLargePages(int *x)
{
		if (!SetPrivilege(hToken, lpszPrivilege, FALSE))	{
			fprintf(stdout,"SetPrivilege() error %u\n", GetLastError());
		}
		if (!VirtualFree(x, 0, MEM_RELEASE))	{
			fprintf(stdout,"VirtualFree() error %u\n", GetLastError());
		}		
}

void _CleanSmallPages(int *x)
{
		if (!VirtualFree(x, 0, MEM_RELEASE))	{
			fprintf(stdout,"VirtualFree() error %u\n", GetLastError());
		}		
}
