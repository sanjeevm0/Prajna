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
    CRITICAL_SECTION *m_cs;

    static void CALLBACK Callback(
        PTP_CALLBACK_INSTANCE Instance,
        PVOID state,
        PVOID olp,
        ULONG ioResult,
        ULONG_PTR bytesTransferred,
        PTP_IO ptp)
    {
        IOCallback *x = (IOCallback*)state;
        if (NO_ERROR != ioResult)
            bytesTransferred = 0LL;
        x->m_inUse = 0;
        (*x->m_pfn)(ioResult, x->m_pstate, x->m_pBuffer, (int)bytesTransferred);
    }

public:
    IOCallback(HANDLE hFile) :
        m_pfn(nullptr), m_pstate(nullptr), m_ptp(nullptr), m_hFile(hFile), 
        m_pBuffer(nullptr), m_inUse(0)
    {
        if (nullptr == m_ptp)
            m_ptp = CreateThreadpoolIo(m_hFile, IOCallback::Callback, this, NULL);
        memset(&m_olap, 0, sizeof(m_olap));
        m_cs = new CRITICAL_SECTION;
        InitializeCriticalSection(m_cs);
    }

    ~IOCallback()
    {
        if (m_ptp)
        {
            WaitForThreadpoolIoCallbacks(m_ptp, TRUE);
            CloseThreadpoolIo(m_ptp);
            m_ptp = nullptr;
        }
        Close(); // in case we didn't call close
        DeleteCriticalSection(m_cs);
        delete m_cs;
        m_cs = NULL;
        //printf("CritSec deleted\n");
    }

    // use enum as WriteFile and ReadFile have slightly differing signatures (LPVOID vs. LPCVOID)
    //template <class T, OperFn fn>
    template <class T, Oper oper>
    int OperFile(T *pBuffer, DWORD nNumberOfElems, CallbackFn *pfn, void *state, __int64 pos)
    {
        if (0 == InterlockedCompareExchange(&m_inUse, 1, 0))
        {
            if (nullptr == m_ptp)
                m_ptp = CreateThreadpoolIo(m_hFile, IOCallback::Callback, this, NULL);
            if (nullptr == m_ptp)
            {
                int e = GetLastError();
                printf("Fail to initialize threadpool I/O error: %x", e);
                return -1;
            }
            DWORD num;
            m_pfn = pfn;
            m_pstate = state;
            m_pBuffer = pBuffer;
            if (pos < 0)
                pos = SetFilePointer(m_hFile, 0, 0, FILE_CURRENT); // won't work for files > 32-bit in length
            m_olap.Offset = (DWORD)(pos & 0x00000000ffffffff);
            m_olap.OffsetHigh = (DWORD)(pos >> 32);
            StartThreadpoolIo(m_ptp);
            //return fn(m_hFile, (void*)pBuffer, nNumberOfElems*sizeof(T), &num, &m_olap);
            int ret = 0;
            switch (oper)
            {
            case ReadFileOper:
                ret = ReadFile(m_hFile, pBuffer, nNumberOfElems*sizeof(T), &num, &m_olap);
                break;
            case WriteFileOper:
                ret = WriteFile(m_hFile, pBuffer, nNumberOfElems*sizeof(T), &num, &m_olap);
                break;
            default:
                assert(false);
                return -1;
            }
            if (0 == ret)
            {
                int e = GetLastError();
                if (ERROR_IO_PENDING == e)
                    return 0;
                else
                    return -1; // some other error
            }
            else
                return ret;
        }
        else
        {
            return -1;
        }
    }

    template <class T>
    __forceinline int ReadFileAsync(T *pBuffer, DWORD nNum, CallbackFn *pfn, void *state, __int64 pos=-1LL)
    {
        return OperFile<T, ReadFileOper>(pBuffer, nNum, pfn, state, pos);
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
    __forceinline int WriteFileAsync(T *pBuffer, DWORD nNum, CallbackFn *pfn, void *state, __int64 pos=-1LL)
    {
        return OperFile<T, WriteFileOper>(pBuffer, nNum, pfn, state, pos);
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

    __forceinline BOOL SeekFile(__int64 offset, DWORD moveMethod, __int64 *newPos)
    {
        LARGE_INTEGER liOffset;
        LARGE_INTEGER newLiOffset;
        liOffset.QuadPart = offset;
        BOOL ret = SetFilePointerEx(m_hFile, liOffset, &newLiOffset, moveMethod);
        *newPos = newLiOffset.QuadPart;
        return ret;
    }

    __forceinline BOOL FlushFile()
    {
        return FlushFileBuffers(m_hFile);
    }

    __forceinline BOOL FileSize(__int64 *fsize)
    {
        if (!fsize)
            return FALSE;
        else
        {
            LARGE_INTEGER size;
            BOOL ret = GetFileSizeEx(m_hFile, &size);
            *fsize = size.QuadPart;
            return ret;
        }
    }

    __forceinline BOOL Close()
    {
        BOOL ret = TRUE;
        //printf("InClose - Close handle %llx\n", (__int64)m_hFile);
        EnterCriticalSection(m_cs);
        //printf("InClose - Enter\n");
        if (m_hFile != NULL)
        {
            //printf("Closing handle %llx\n", (__int64)m_hFile);
            ret = CloseHandle(m_hFile);
            //printf("Handle %llx close\n", (__int64)m_hFile);
            m_hFile = NULL;
        }
        LeaveCriticalSection(m_cs);
        //printf("InClose - Leave\n");
        return ret;
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
using namespace Microsoft::Win32::SafeHandles;

namespace Prajna {
namespace Tools {
namespace Test {
    // In case class does not have destructor, and it does not derive from base class which is IDisposable, then it is not IDisposable
    // In case class has destructor or it derives from base class which is IDisposable, then it is IDisposable
    // 1. In case it has dtor and does not derive from base class which is IDisposable, then
    //    it implements IDisposable::Dispose automatically, and implements "virtual Dispose(bool disposing) method"
    // 2. In case it has dtor and derives from base class which is IDisposable, then
    //    it does not implement IDisposable::Dispose, it only implements "override Dispose(bool disposing) method" which automatically calls base.Dispose
    public ref class TestDispose : public IDisposable
    {
    private:
        int a;
        byte *buf;
    public:
        TestDispose()
        {
            a = 4;
            buf = new byte[100];
        }
        !TestDispose()
        {
            delete[] buf;
        }
        ~TestDispose()
        {
            delete[] buf;
        }
    };

    // Any CLI class wtih destructor, dtor (i.e. ~ method) automatically becomes IDisposable
    // Following code is then automatically generated
    // [HandleProcessCorruptedStateExceptions]
    //protected override void Dispose([MarshalAs(UnmanagedType.U1)] bool flag1)
    //{
    //    if (flag1)
    //    {
    //        try
    //            this.~CppDispose();
    //        finally
    //            base.Dispose(true);
    //            GC::SuppressFinalize(this);
    //    }
    //    else
    //    {
    //        try
    //            this.!CppDispose();
    //        finally
    //            base.Dispose(false);
    //    }
    //}
    //
    //protected override void Finalize()
    //{
    //    this.Dispose(false);
    //}
    //
    // In addition, if and only if there is no base class in the hierarchy which is IDisposable, the class implements IDisposable and following is added
    //virtual void Dispose(array<byte> ^buf, int offset, int cnt) = IDisposable::Dispose
    //{
    //    this.Dispose(true);
    //}
    // Therefore for CLI
    // 1. Never directly implement IDisposable
    // 2. Instead do following - the created dispose method will automatically have suppressfinalize call
    //    a) Create destructor to free managed resources
    //    ~Class()
    //    {
    //        ... free managed resources ... (stuff that goes inside "if bDisposing" block)
    //        ... call finalizer if it exists ...
    //    }
    //    !Class
    //    {
    //        ... free unmanaged resources ...
    //    }

    // In C#, the destructor is actually the finalizer and simply inserts following code:
    //protected override void Finalize()
    //{
    //    try
    //    {
    //        // Cleanup statements...
    //    }
    //    finally
    //    {
    //        base.Finalize();
    //    }
    //}
}
}
}

namespace Prajna {
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
        generic <class T> static int SizeOf()
        {
            //return sizeof(T::typeid);
            return sizeof(T);
        }
    };

    delegate void NativeIOCallback(int ioResult, void *pState, void *pBuffer, int bytesTransferred);

    generic <class T> public delegate void IOCallbackDel(int ioResult, Object ^pState, array<T> ^pBuffer, int offset, int bytesTransferred);

    private interface class IIO
    {
    public:
        virtual void UpdatePos(__int64 amt);
    };

    generic <class T> private ref class IOCallbackClass
    {
    private:
        IIO ^m_parent;
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
            x->m_parent->UpdatePos(bytesTransferred);
            x->m_cb->Invoke(ioResult, x->m_pState, x->m_buffer, x->m_offset, bytesTransferred);
        }

    public:
        IOCallbackClass(IIO ^parent)
        {
            this->m_parent = parent;

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

    public ref class AsyncStreamIO : public IIO, public Stream
    {
    private:
        IOCallback *m_cb;
        Dictionary<Type^, Object^> ^m_cbFns;
        Object ^m_ioLock;
        // Stream stuff
        bool m_canRead;
        bool m_canWrite;
        bool m_canSeek;
        Int64 m_length;
        Int64 m_position;

        generic <class T>
            IOCallbackClass<T>^ GetCbFn(array<T> ^pBuffer)
            {
                Type ^t = T::typeid;
                if (!m_cbFns->ContainsKey(t))
                    m_cbFns[t] = (Object^)(gcnew IOCallbackClass<T>(this));
                return (IOCallbackClass<T>^)m_cbFns[t];
            }

    protected:
        void virtual Free()
        {
            Lock lock(this);
            if (nullptr != m_cb)
            {
                m_cb->Close();
                delete m_cb;
                m_cb = nullptr;
            }
            //ICollection<Object^>^ cb = (ICollection<Object^>^)m_cbFns;
        }

    public:
        AsyncStreamIO(FileStream ^strm) : m_cb(nullptr), m_cbFns(nullptr), m_ioLock(nullptr),
            m_canRead(false), m_canWrite(false), m_canSeek(false), m_length(0LL), m_position(0LL)
        {
            m_cb = new IOCallback((void*)strm->SafeFileHandle->DangerousGetHandle());
            m_cbFns = gcnew Dictionary<Type^, Object^>();
            m_ioLock = gcnew Object();
        }

        AsyncStreamIO(HANDLE h) : m_cb(nullptr)
        {
            m_cb = new IOCallback(h);
            m_cbFns = gcnew Dictionary<Type^, Object^>();
            m_ioLock = gcnew Object();
        }

        // destructor (e.g. Dispose with bDisposing = true), automatically adds suppressfinalize call
        ~AsyncStreamIO()
        {
            m_cbFns->Clear(); // managed resource
            this->!AsyncStreamIO(); // finalizer - suppress finalize is already done
            //GC::SuppressFinalize(this);
            //Free();
        }

        !AsyncStreamIO()
        {
            Free();
        }

        // Since Stream class implements IDisposable, it automatically calls close first prior to the virtual Dispose method, i.e. in Stream class
        // IDisposable::Dispose()
        // {
        //     Dispose(); // calls publicly available Dispose() - for all classes in namespace System (then calls Close)
        // }
        // 
        // void Dispose()
        // {
        //     Close();
        // }
        //
        // virtual void Close()
        // {
        //     Dispose(true)
        // }
        //
        // virtual void Dispose(bool bDisposing) {} -> gets overwritten by us in dtor
        virtual void __clrcall Close() new = Stream::Close
        {
            // close the stream
            Lock lock(this);
            if (m_cb != nullptr)
                m_cb->Close();
            // this will actually dispose stuff, so close stuff before
            Stream::Close();
        }

        property bool CanRead
        {
            virtual bool __clrcall get() new = Stream::CanRead::get { return m_canRead; }
        }
        property bool CanWrite
        {
            virtual bool __clrcall get() override { return m_canWrite; }
        }
        property bool CanSeek
        {
            virtual bool __clrcall get() override { return m_canSeek; }
        }
        virtual property __int64 Length
        {
            __int64 __clrcall get() override { return m_length; }
        }
        virtual property __int64 Position
        {
            __int64 __clrcall get() override { return m_position; }
            void __clrcall set(__int64 pos) override
            {
                this->Seek(pos, SeekOrigin::Begin);
            }
        }
        virtual void __clrcall Flush() override
        {
            if (FALSE == m_cb->FlushFile())
                throw gcnew IOException("Unable to flush file");
        }
        virtual __int64 __clrcall Seek(__int64 offset, SeekOrigin origin) override
        {
            __int64 newPos;
            int ret = m_cb->SeekFile(offset, (int)origin, &newPos);
            if (FALSE == ret)
                throw gcnew IOException("Unable to seek");
            return newPos;
        }
        virtual void __clrcall SetLength(__int64 length) override
        {
            assert(false);
            // not supported
        }
        virtual int __clrcall Read(array<byte> ^buf, int offset, int cnt) new = Stream::Read
        {
            return ReadFileSync(buf, offset, cnt);
        }
        virtual void __clrcall Write(array<byte> ^buf, int offset, int cnt) new = Stream::Write
        {
            int ret = WriteFileSync(buf, offset, cnt);
            if (ret != cnt)
                throw gcnew IOException("Unable to write to stream");
        }

        virtual void UpdatePos(Int64 amt) = IIO::UpdatePos
        {
            m_position += amt;
            m_length = max(m_length, m_position);
        }

        // not useful as both m_ptp and FileStream cannot both simultaneously exist!!
        //static FileStream^ OpenFileAsyncWrite(String^ name)
        //{
        //    array<wchar_t> ^nameArr = name->ToCharArray();
        //    pin_ptr<wchar_t> namePtr = &nameArr[0];
        //    LPCWSTR pName = (LPCWSTR)namePtr;
        //    HANDLE h = CreateFile(pName, GENERIC_WRITE, FILE_SHARE_READ, nullptr, CREATE_ALWAYS, FILE_FLAG_OVERLAPPED | FILE_ATTRIBUTE_NORMAL | FILE_FLAG_WRITE_THROUGH, nullptr);
        //    SafeFileHandle ^sh = gcnew SafeFileHandle(IntPtr(h), true); // cannot do with "true" as finalizer attempts to close the handle without checking
        //    FileStream ^fs = gcnew FileStream(sh, FileAccess::Write, 4096, true);
        //    return fs;
        //}

        static AsyncStreamIO^ OpenFileWrite(String^ name, Stream^ %fsOut, FileOptions fOpt)
        {
            array<Char> ^nameArr = name->ToCharArray();
            pin_ptr<Char> namePtr = &nameArr[0];
            Char *pName = (Char*)namePtr;
            IntPtr h = (IntPtr)CreateFile(pName, GENERIC_WRITE, FILE_SHARE_READ, nullptr, CREATE_ALWAYS, (int)fOpt, nullptr);
            AsyncStreamIO ^io = gcnew AsyncStreamIO((HANDLE)h);
            io->m_position = 0LL;
            io->m_length = 0LL;
            io->m_canRead = false;
            io->m_canWrite = true;
            io->m_canSeek = true;
            fsOut = io;
            return io;
        }

        static AsyncStreamIO^ OpenFileRead(String^ name, Stream^ %fsOut, FileOptions fOpt)
        {
            array<Char> ^nameArr = name->ToCharArray();
            pin_ptr<Char> namePtr = &nameArr[0];
            Char *pName = (Char*)namePtr;
            IntPtr h = (IntPtr)CreateFile(pName, GENERIC_READ, FILE_SHARE_READ, nullptr, OPEN_EXISTING, (int)fOpt, nullptr);
            AsyncStreamIO ^io = gcnew AsyncStreamIO((HANDLE)h);
            io->m_position = 0LL;
            pin_ptr<__int64> pLen = &io->m_length;
            int ret = io->m_cb->FileSize((__int64*)pLen);
            if (!ret)
                throw gcnew IOException("Unable to get file length");
            io->m_canRead = true;
            io->m_canWrite = false;
            io->m_canSeek = true;
            fsOut = io;
            return io;
        }

        generic <class T> int ReadFile(array<T> ^pBuffer, int offset, int nNum, IOCallbackDel<T> ^cb, Object ^state)
        {
            Lock lock(m_ioLock);
            IOCallbackClass<T>^ cbFn = GetCbFn(pBuffer);
            IntPtr pBuf = cbFn->Set(state, pBuffer, offset, cb);
            int ret = m_cb->ReadFileAsync<byte>((byte*)pBuf.ToPointer(), nNum*cbFn->TypeSize(), cbFn->CbFn(), cbFn->SelfPtr().ToPointer(), m_position);
            if (-1 == ret || ret > 0) // > 0 not really an error, but finished sync, so no callback
                cbFn->OnError();
            return ret;
        }

        generic <class T> int WriteFile(array<T> ^pBuffer, int offset, int nNum, IOCallbackDel<T> ^cb, Object ^state)
        {
            Lock lock(m_ioLock);
            IOCallbackClass<T>^ cbFn = GetCbFn(pBuffer);
            IntPtr pBuf = cbFn->Set(state, pBuffer, offset, cb);
            int ret = m_cb->WriteFileAsync<byte>((byte*)pBuf.ToPointer(), nNum*cbFn->TypeSize(), cbFn->CbFn(), cbFn->SelfPtr().ToPointer(), m_position);
            if (-1 == ret || ret > 0) // > 0 not really an error, but finished sync, so no callback
                cbFn->OnError();
            return ret;
        }

        generic <class T> int ReadFileSync(array<T> ^pBuffer, int offset, int nNum)
        {
            Lock lock(m_ioLock);
            pin_ptr<T> pBuf = &pBuffer[0];
            int amtRead = m_cb->ReadFileSync<byte>((byte*)pBuf, nNum*sizeof(T));
            if (amtRead >= 0)
                UpdatePos(amtRead);
            return amtRead;
        }

        generic <class T> int WriteFileSync(array<T> ^pBuffer, int offset, int nNum)
        {
            Lock lock(m_ioLock);
            pin_ptr<T> pBuf = &pBuffer[0];
            int amtWrite = m_cb->WriteFileSync<byte>((byte*)pBuf, nNum*sizeof(T));
            if (amtWrite >= 0)
                UpdatePos(amtWrite);
            return amtWrite;
        }
    };
}
}
}


