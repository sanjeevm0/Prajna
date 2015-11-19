// NativeSort.h

#pragma once

// ==================================================
#pragma unmanaged

#include <stdlib.h>     
#include <string.h>
#include <math.h>

int __cdecl compare64(void *context, const void *a, const void *b);

extern "C" __declspec(dllexport)
__forceinline void __stdcall alignsort64(unsigned __int64 *buf, int align, int num)
{
    qsort_s(buf, num, align * 8, compare64, &align);
}

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

        Sort() {}
    };
}

