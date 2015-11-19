#include <stdlib.h>     
#include <string.h>
#include <math.h>

// whole file has no CLR support, so no need to say unmanaged
//#pragma unmanaged

int __cdecl compare64(void *context, const void *a, const void *b)
{
    int i;
    int dim = *(int*)context;
    unsigned __int64 *a64 = (unsigned __int64*)a;
    unsigned __int64 *b64 = (unsigned __int64*)b;

#if 0
    if (a64[0] > b64[0]) return 1;
    if (a64[0] < b64[0]) return -1;
    for (i = 1; i < dim; i++)
    {
        if (a64[i] > b64[i]) return 1;
        if (a64[i] < b64[i]) return -1;
    }
#else
    if (_byteswap_uint64(a64[0]) > _byteswap_uint64(b64[0])) return 1;
    if (_byteswap_uint64(a64[0]) < _byteswap_uint64(b64[0])) return -1;
    for (i = 1; i < dim; i++)
    {
        if (_byteswap_uint64(a64[i]) > _byteswap_uint64(b64[i])) return 1;
        if (_byteswap_uint64(a64[i]) < _byteswap_uint64(b64[i])) return -1;
    }

#endif
    return 0;
}

//extern "C" __declspec(dllexport)
//void __stdcall alignsort64(unsigned __int64 *buf, int align, int num)
//{
//    qsort_s(buf, num, align * 8, compare64, &align);
//}

