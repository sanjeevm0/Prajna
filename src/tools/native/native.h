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
#include <assert.h>

typedef void (__stdcall CallbackFn)(int ioResult, void *pState, void *pBuffer, int bytesTransferred);
typedef BOOL (__stdcall *OperFn)(HANDLE, LPVOID, DWORD, LPDWORD, LPOVERLAPPED);

typedef enum _Oper
{
    ReadFileOper = 0,
    WriteFileOper = 1
} Oper;

class IOCallback {
private:
    OVERLAPPED m_olap;
    CallbackFn *m_pfn;
    void *m_pstate;
    PTP_IO m_ptp;
    HANDLE m_hFile;
    void *m_pBuffer;
    volatile unsigned int m_inUse;

    static void CALLBACK Callback(
        PTP_CALLBACK_INSTANCE Instance,
        PVOID state,
        PVOID olp,
        ULONG ioResult,
        ULONG_PTR bytesTransferred,
        PTP_IO ptp)
    {
        IOCallback *x = (IOCallback*)state;
        if (NO_ERROR == ioResult)
        {
            x->m_inUse = 0;
            (*x->m_pfn)(ioResult, x->m_pstate, x->m_pBuffer, (int)bytesTransferred);
        }
        else
        {
            (*x->m_pfn)(ioResult, x->m_pstate, x->m_pBuffer, 0);
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

    // use enum as WriteFile and ReadFile have slightly differing signatures (LPVOID vs. LPCVOID)
    //template <class T, OperFn fn>
    template <class T, Oper oper>
    BOOL OperFile(T *pBuffer, DWORD nNumberOfElems, CallbackFn *pfn, void *state)
    {
        if (0 == InterlockedCompareExchange(&m_inUse, 1, 0))
        {
            if (nullptr == m_ptp)
                m_ptp = CreateThreadpoolIo(m_hFile, IOCallback::Callback, this, NULL);
            if (nullptr == m_ptp)
                return false;
            DWORD num;
            m_pfn = pfn;
            m_pstate = state;
            m_pBuffer = pBuffer;
            StartThreadpoolIo(m_ptp);
            //return fn(m_hFile, (void*)pBuffer, nNumberOfElems*sizeof(T), &num, &m_olap);
            switch (oper)
            {
            case ReadFileOper:
                return ReadFile(m_hFile, pBuffer, nNumberOfElems*sizeof(T), &num, &m_olap);
            case WriteFileOper:
                return WriteFile(m_hFile, pBuffer, nNumberOfElems*sizeof(T), &num, &m_olap);
            default:
                assert(false);
                return FALSE;
            }
        }
        else
        {
            return false;
        }
    }

    template <class T>
    __forceinline BOOL ReadFileAsync(T *pBuffer, DWORD nNum, CallbackFn *pfn, void *state)
    {
        return OperFile<T, ReadFileOper>(pBuffer, nNum, pfn, state);
    }

    template <class T>
    __forceinline int ReadFileSync(T *pBuffer, DWORD nNum)
    {
        DWORD num;
        if (ReadFile(m_hFile, (void*)pBuffer, nNum*sizeof(T), &num, nullptr))
        {
            return num;
        }
        else
        {
            return -1;
        }
    }

    template <class T>
    __forceinline BOOL WriteFileAsync(T *pBuffer, DWORD nNum, CallbackFn *pfn, void *state)
    {
        return OperFile<T, WriteFileOper>(pBuffer, nNum, pfn, state);
    }

    template <class T>
    __forceinline int WriteFileSync(T *pBuffer, DWORD nNum)
    {
        DWORD num;
        if (WriteFile(m_hFile, (void*)pBuffer, nNum*sizeof(T), &num, nullptr))
        {
            return num;
        }
        else
        {
            return -1;
        }
    }
};

// ====================================================

#pragma managed

using namespace System;
using namespace System::IO;
using namespace System::Reflection;
using namespace System::Threading;
using namespace System::Collections::Generic;
using namespace System::Runtime::InteropServices;

namespace Tools {
	namespace Native {
        ref class Lock {
            Object^ m_pObject;
        public:
            Lock(Object ^ pObject) : m_pObject(pObject) {
                Monitor::Enter(m_pObject);
            }
            ~Lock() {
                Monitor::Exit(m_pObject);
            }
        };

        ref class NativeHelper
        {
        private:
            generic <class T>
                static int SizeOf()
                {
                    //return sizeof(T::typeid);
                    return sizeof(T);
                }
        };

        delegate void NativeIOCallback(int ioResult, void *pState, void *pBuffer, int bytesTransferred);

        generic <class T>
            public delegate void IOCallbackDel(int ioResult, Object ^pState, array<T> ^pBuffer, int offset, int bytesTransferred);

        generic <class T>
            public ref class IOCallbackClass
            {
            private:
                IOCallbackDel<T> ^m_cb;
                int m_managedTypeSize;
                GCHandle ^m_handleBuffer;
                GCHandle ^m_handleCallback;
                GCHandle ^m_self;
                Object ^m_pState;
                array<T> ^m_buffer;
                int m_offset;
                CallbackFn *m_pfn;
                IntPtr m_selfPtr;

                static void __clrcall Callback(int ioResult, void *pState, void *pBuffer, int bytesTransferred)
                {
                    GCHandle ^pStateHandle = GCHandle::FromIntPtr(static_cast<IntPtr>(pState));
                    IOCallbackClass ^x = safe_cast<IOCallbackClass^>(pStateHandle->Target);
                    x->m_handleBuffer->Free();
                    x->m_cb->Invoke(ioResult, x->m_pState, x->m_buffer, x->m_offset, bytesTransferred);
                }

            public:
                IOCallbackClass()
                {
                    //Type^ t = NativeHelper::typeid;
                    //Object ^o = t->GetMethod("SizeOf", BindingFlags::Static | BindingFlags::NonPublic)
                    //    ->GetGenericMethodDefinition()
                    //    ->MakeGenericMethod(T::typeid)
                    //    ->Invoke(nullptr, nullptr);
                    //m_managedTypeSize = *(safe_cast<int^>(o));
                    //m_managedTypeSize = safe_cast<int>(o); // this will also work as unboxing is implicit in safe_cast
                    m_managedTypeSize = sizeof(T);

                    NativeIOCallback ^ncb = gcnew NativeIOCallback(IOCallbackClass::Callback); // create delegate from function
                    m_handleCallback = GCHandle::Alloc(ncb); // GCHandle to prevent garbage collection
                    IntPtr ip = Marshal::GetFunctionPointerForDelegate(ncb); // function pointer for the delgate
                    m_pfn = static_cast<CallbackFn*>(ip.ToPointer());

                    // a gchandle to self
                    m_self = GCHandle::Alloc(this);
                    m_selfPtr = GCHandle::ToIntPtr(*m_self);
                }

                ~IOCallbackClass()
                {
                    Lock lock(this);
                    m_handleCallback->Free();
                    m_self->Free();
                }

                IntPtr Set(Object ^state, array<T> ^buffer, int offset, IOCallbackDel<T> ^cb)
                {
                    m_handleBuffer = GCHandle::Alloc(buffer, GCHandleType::Pinned); // must pin so unmanaged code can use it
                    m_pState = state;
                    m_buffer = buffer;
                    m_offset = offset;
                    m_cb = cb;
                    return IntPtr::Add(m_handleBuffer->AddrOfPinnedObject(), m_offset*m_managedTypeSize);
                }

                void OnError()
                {
                    m_handleBuffer->Free();
                }

                __forceinline CallbackFn* CbFn() { return m_pfn; }
                __forceinline int TypeSize() { return m_managedTypeSize; }
                __forceinline IntPtr SelfPtr() { return m_selfPtr;  }
            };

        public ref class AsyncStreamIO
		{
        private:
            IOCallback *m_cb;
            Dictionary<Type^, Object^> ^m_cbFns;
            Object ^m_ioLock;

        protected:
            void virtual Free(bool bDisposing)
            {
                Lock lock(this);
                if (nullptr != m_cb)
                {
                    delete m_cb;
                    m_cb = nullptr;
                }
                //ICollection<Object^>^ cb = (ICollection<Object^>^)m_cbFns;
            }

        public:
            AsyncStreamIO(FileStream ^strm) : m_cb(nullptr)
            {
                m_cb = new IOCallback((void*)strm->SafeFileHandle->DangerousGetHandle());
                m_cbFns = gcnew Dictionary<Type^, Object^>();
                m_ioLock = gcnew Object();
            }

            // destructor (e.g. dispose)
            ~AsyncStreamIO()
            {
                Free(true);
            }

            !AsyncStreamIO()
            {
                Free(false);
            }

            generic <class T>
                IOCallbackClass<T>^ GetCbFn(array<T> ^pBuffer)
                {
                    Type ^t = T::typeid;
                    if (! m_cbFns->ContainsKey(t))
                        m_cbFns[t] = (Object^)(gcnew IOCallbackClass<T>());
                    return (IOCallbackClass<T>^)m_cbFns[t];
                }

            generic <class T>
                int ReadFile(array<T> ^pBuffer, int offset, int nNum, IOCallbackDel<T> ^cb, Object ^state)
                {
                    Lock lock(m_ioLock);
                    IOCallbackClass<T>^ cbFn = GetCbFn(pBuffer);
                    IntPtr pBuf = cbFn->Set(state, pBuffer, offset, cb);
                    int ret = m_cb->ReadFileAsync<byte>((byte*)pBuf.ToPointer(), nNum*cbFn->TypeSize(), cbFn->CbFn(), cbFn->SelfPtr().ToPointer());
                    if (!ret)
                    {
                        cbFn->OnError();
                        return -1;
                    }
                    else
                        return ret;
                }

            generic <class T>
                int WriteFile(array<T> ^pBuffer, int offset, int nNum, IOCallbackDel<T> ^cb, Object ^state)
                {
                    Lock lock(m_ioLock);
                    IOCallbackClass<T>^ cbFn = GetCbFn(pBuffer);
                    IntPtr pBuf = cbFn->Set(state, pBuffer, offset, cb);
                    int ret = m_cb->WriteFileAsync<byte>((byte*)pBuf.ToPointer(), nNum*cbFn->TypeSize(), cbFn->CbFn(), cbFn->SelfPtr().ToPointer());
                    if (!ret)
                    {
                        cbFn->OnError();
                        return -1;
                    }
                    else
                        return ret;
                }

            generic <class T>
                int ReadFileSync(array<T> ^pBuffer, int offset, int nNum)
                {
                    Lock lock(m_ioLock);
                    pin_ptr<T> pBuf = &pBuffer[0];
                    return m_cb->ReadFileSync<byte>((byte*)pBuf, nNum*sizeof(T));
                }

            generic <class T>
                int WriteFileSync(array<T> ^pBuffer, int offset, int nNum)
                {
                    Lock lock(m_ioLock);
                    IOCallbackClass<T>^ cbFn = GetCbFn(pBuffer);
                    pin_ptr<T> pBuf = &pBuffer[0];
                    return m_cb->WriteFileSync<byte>((byte*)pBuf, nNum*sizeof(T));
                }
        };
	}
}

