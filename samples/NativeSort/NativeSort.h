// NativeSort.h

#pragma once

#include <stdlib.h>     
#include <string.h>
#include <math.h>
#include <atlstr.h>

// ==================================================
#pragma unmanaged

int __cdecl compare64(void *context, const void *a, const void *b);

extern "C" __declspec(dllexport)
__forceinline void __stdcall alignsort64(unsigned __int64 *buf, int align, int num)
{
    qsort_s(buf, num, align * 8, compare64, &align);
}

int sortFile(unsigned __int64 *buf, int bufSize, int align, LPTSTR inFile, LPTSTR outFile);

// ==================================================
#pragma managed

using namespace System;

namespace NativeSort {

	public ref class Sort
	{
    public:
        static void AlignSort64(IntPtr buf, int align, int num)
        {
            alignsort64((unsigned __int64 *)buf.ToPointer(), align, num);
        }

        static int SortFile(array<UInt64>^ tempSpace, int align, String^ inFile, String^ outFile)
        {
            pin_ptr<UInt64> p = &tempSpace[0];
            int bufSize = tempSpace->Length;
            CString in(inFile);
            CString out(outFile);
            return sortFile(p, bufSize, align, in.GetBuffer(), out.GetBuffer());
        }

        Sort() {}
    };
}

