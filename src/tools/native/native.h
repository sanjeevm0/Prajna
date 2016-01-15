/*-------------------------------------------------------------------------- -
	Copyright 2013 Microsoft

	Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http ://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Author: Sanjeev Mehrotra
-------------------------------------------------------------------------- - */
#pragma once

#pragma unmanaged

#include "stdio.h"
#include <windows.h>
typedef void CallbackFn(int ioResult, void *pState, void *pBuffer, int bytesTransferred);
typedef BOOL OperFn(HANDLE, LPCVOID, DWORD, LPDWORD, LPOVERLAPPED);

class IOCallback {
private:
    OVERLAPPED m_olap;
    CallbackFn *m_pfn;
    void *m_pstate;
    PTP_IO m_ptp;
    HANDLE m_hFile;
    void *m_pBuffer;
    volatile int m_inUse;

    void __stdcall Callback(
        PTP_CALLBACK_INSTANCE Instance,
        void *state,
        void *olp,
        ULONG ioResult,
        ULONG *pBytesTransffered,
        PTP_IO ptp)
    {
        if (NO_ERROR == ioResult)
        {
            m_inUse = 0;
            (*m_pfn)(ioResult, m_pstate, m_pBuffer, *pBytesTransffered);
        }
        else
        {
            (*m_pfn)(ioResult, m_pstate, m_pBuffer, 0);
        }
    }

public:
    IOCallback(HANDLE hFile) :
        m_pfn(nullptr), m_pstate(nullptr), m_ptp(nullptr), m_hFile(hFile), 
        m_pBuffer(nullptr), m_inUse(0)
    {
    }

    ~IOCallback()
    {
        if (m_ptp)
        {
            CloseThreadpoolIo(m_ptp);
            m_ptp = nullptr;
        }
    }

    // can use bool in case OperFn has differing signatures
    template <class T, OperFn fn>
    BOOL OperFile(T *pBuffer, DWORD nNumberOfElems, CallbackFn *pfn, void *state)
    {
        if (InterlockedCompareExchange(&m_inUse, 1, 0) = 0)
        {
            if (nullptr == m_ptp)
                m_ptpRead = CreateThreadpoolIo(hFile, &this->Callback, nullptr, NULL);
            if (nullptr == m_ptp)
                return false;
            DWORD num;
            m_pfn = pfn;
            m_state = state;
            m_pBuffer = pBuffer;
            StartThreadpoolIo(m_ptp);
            return fn(m_hFile, (void*)pBuffer, nNumberOfElems*sizeof<T>, &num, m_olap);
        }
        else
        {
            return false;
        }
    }

    template <class T>
    __forceinline ReadFile(T *pBuffer, DWORD nNum, Callback *pfn, void *state)
    {
        return OperFile<T, ReadFile>(pBuffer, nNum, pfn, state);
    }

    template <class T>
    __forceinline WriteFile(T *pBuffer, DWORD nNum, Callback *pfn, void *state)
    {
        return OperFile<T, WriteFile>(pBuffer, nNum, pfn, state);
    }
};

// ====================================================

#pragma managed

using namespace System;
using namespace System::IO;
using namespace System::Runtime::InteropServices;

namespace Tools {
	namespace Native {
        delegate void CallbackByte(int ioResult, IntPtr pState, array<byte> ^pBuffer, int offset, int num);
        
        public ref class AsyncStreamIO
		{
        private:
            IOCallback *m_cb;
            GCHandle ^handleState;
            GCHandle ^handleBuffer;

            void CbByte(int ioResult, void *pState, void *pBuffer, int bytesTransferred)
            {

            }

        public:
            AsyncStreamIO(FileStream ^strm)
            {
                m_cb = new IOCallback((void*)strm->SafeFileHandle->DangerousGetHandle());
            }

            Boolean ReadFileByte(array<byte> ^pBuffer, int offset, int nNum, CallbackByte ^cb, Object ^state)
            {
                handleState = GCHandle::Alloc(state, GCHandleType::Pinned);
                handleBuffer = GCHandle::Alloc(pBuffer, GCHandleType::Pinned);
                m_cb->ReadFile(IntPtr::Add(handleBuffer->AddrOfPinnedObject(), offset), nNum, Marshal::GetFunctionPointerForDelegate(new Delegate(CbByte)), handleState);
            }
		};
	}
}



