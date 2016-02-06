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

class TPIO {
private:
    HANDLE m_hFile;
    OVERLAPPED m_olap;
    PTP_IO m_ptp;
    void *m_pstate;
    void *m_pBuffer;
    volatile unsigned int m_inUse;
    CallbackFn *m_pfn;

    static void CALLBACK Callback(
        PTP_CALLBACK_INSTANCE Instance,
        PVOID state,
        PVOID olp,
        ULONG ioResult,
        ULONG_PTR bytesTransferred,
        PTP_IO ptp)
    {
        TPIO *x = (TPIO*)state;
        if (NO_ERROR != ioResult)
            bytesTransferred = 0LL;
        x->m_inUse = 0;
        (*x->m_pfn)(ioResult, x->m_pstate, x->m_pBuffer, (int)bytesTransferred);
    }

public:
    TPIO(HANDLE hFile) : m_hFile(hFile), m_pfn(nullptr), m_pstate(nullptr), m_ptp(nullptr),
        m_pBuffer(nullptr), m_inUse(0)
    {
        m_ptp = CreateThreadpoolIo(hFile, TPIO::Callback, this, NULL);
        if (nullptr == m_ptp)
        {
            int e = GetLastError();
            printf("Last error: 0x%x", e);
            throw "Fail to initialize threadpool I/O error";
        }
        memset(&m_olap, 0, sizeof(m_olap));
    }

    ~TPIO()
    {
        if (m_ptp)
        {
            WaitForThreadpoolIoCallbacks(m_ptp, TRUE);
            CloseThreadpoolIo(m_ptp);
            m_ptp = nullptr;
        }
    }

    // use enum as WriteFile and ReadFile have slightly differing signatures (LPVOID vs. LPCVOID)
    //template <class T, OperFn fn>
    template <class T, Oper oper>
    int OperFile(T *pBuffer, DWORD nNumberOfElems, CallbackFn *pfn, void *state, __int64 pos)
    {
        if (0 == InterlockedCompareExchange(&m_inUse, 1, 0))
        {
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
                ret = ReadFile(m_hFile, pBuffer, nNumberOfElems*sizeof(T), NULL, &m_olap);
                break;
            case WriteFileOper:
                ret = WriteFile(m_hFile, pBuffer, nNumberOfElems*sizeof(T), NULL, &m_olap);
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
};

class IOCallback {
private:
    HANDLE m_hFile;
    CRITICAL_SECTION *m_cs;

public:
    IOCallback(HANDLE hFile) : m_hFile(hFile)
    {
        m_cs = new CRITICAL_SECTION;
        InitializeCriticalSection(m_cs);
    }

    ~IOCallback()
    {
        Close(); // in case we didn't call close
        DeleteCriticalSection(m_cs);
        delete m_cs;
        m_cs = NULL;
    }

    template <class T>
    __forceinline int ReadFileAsync(TPIO *tpio, T *pBuffer, DWORD nNum, CallbackFn *pfn, void *state, __int64 pos=-1LL)
    {
        return tpio->OperFile<T, ReadFileOper>(pBuffer, nNum, pfn, state, pos);
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
    __forceinline int WriteFileAsync(TPIO *tpio, T *pBuffer, DWORD nNum, CallbackFn *pfn, void *state, __int64 pos=-1LL)
    {
        return tpio->OperFile<T, WriteFileOper>(pBuffer, nNum, pfn, state, pos);
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

    __forceinline BOOL SetFileEnd()
    {
        return SetEndOfFile(m_hFile);
    }
};

// ====================================================

#pragma managed

using namespace System;
using namespace System::IO;
using namespace System::Reflection;
using namespace System::Threading;
using namespace System::Collections::Generic;
using namespace System::Collections::Concurrent;
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

    // with null object, lock is removed
    ref class Lock {
        Object^ m_pObject;
    public:
        Lock(Object ^ pObject) : m_pObject(pObject) {
            if (nullptr != m_pObject)
                Monitor::Enter(m_pObject);
        }
        ~Lock() {
            if (nullptr != m_pObject)
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
        virtual void UpdateIO(IntPtr tpio, __int64 amt);
    };

    generic <class T> private ref class IOState
    {
    public:
        GCHandle m_self;
        IntPtr m_selfPtr;

        Object ^m_parent;
        GCHandle ^m_handleBuffer;
        IOCallbackDel<T> ^m_cb;
        Object ^m_pState;
        array<T> ^m_buffer;
        IntPtr m_tpio;
        int m_offset;

        IOState() 
        {
            m_self = GCHandle::Alloc(this);
            m_selfPtr = GCHandle::ToIntPtr(m_self);
        }

        void Free()
        {
            m_self.Free();
            if (nullptr != m_handleBuffer)
                m_handleBuffer->Free();
        }

        __forceinline void* SelfPtr()
        {
            return m_selfPtr.ToPointer();
        }
    };

    generic <class T> private ref class IOCallbackClass
    {
    private:
        IIO ^m_parent;
        int m_managedTypeSize;
        GCHandle ^m_handleCallback;
        CallbackFn *m_pfn;
        //GCHandle ^m_self;
        //IntPtr m_selfPtr;

        static void __clrcall Callback(int ioResult, void *pState, void *pBuffer, int bytesTransferred)
        {
            GCHandle ^pStateHandle = GCHandle::FromIntPtr(static_cast<IntPtr>(pState));
            IOState<T> ^ioState = safe_cast<IOState<T>^>(pStateHandle->Target);
            IOCallbackClass ^x = (IOCallbackClass^)ioState->m_parent;
            x->m_parent->UpdateIO(ioState->m_tpio, bytesTransferred);
            ioState->m_cb->Invoke(ioResult, ioState->m_pState, ioState->m_buffer, ioState->m_offset, bytesTransferred);
            ioState->Free();
        }

    public:
        IOCallbackClass(IIO ^parent)
        {
            this->m_parent = parent;

            //Type^ t = NativeHelper::typeid;q
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
            //m_self = GCHandle::Alloc(this);
            //m_selfPtr = GCHandle::ToIntPtr(*m_self);
        }

        ~IOCallbackClass()
        {
            Lock lock(this);
            m_handleCallback->Free();
            //m_self->Free();
        }

        IntPtr Set(IntPtr ptr, Object ^state, array<T> ^buffer, int offset, IOCallbackDel<T> ^cb, IntPtr tpio, IOState<T> ^ioState)
        {
            ioState->m_parent = this;
            ioState->m_pState = state;
            ioState->m_buffer = buffer;
            ioState->m_offset = offset;
            ioState->m_cb = cb;
            ioState->m_tpio = tpio;

            if (IntPtr::Zero == ptr)
            {
                ioState->m_handleBuffer = GCHandle::Alloc(buffer, GCHandleType::Pinned); // must pin so unmanaged code can use it            
                ptr = IntPtr::Add(ioState->m_handleBuffer->AddrOfPinnedObject(), ioState->m_offset*m_managedTypeSize);
            }
            else
            {
                ioState->m_handleBuffer = nullptr;
            }
            return ptr;
        }

        void OnNoCallback(IOState<T> ^ioState)
        {
            m_parent->UpdateIO(ioState->m_tpio, 0L);
            ioState->Free();
        }

        __forceinline CallbackFn* CbFn() { return m_pfn; }
        __forceinline int TypeSize() { return m_managedTypeSize; }
        //__forceinline IntPtr SelfPtr() { return m_selfPtr;  }
    };

    // expand a function from Func<T,TResult> to Func<Object^,T,TResult>
    generic <class N, class T, class TResult> private ref class Func2to3
    {
    private:
        N m_val;
        Func<N, T, TResult> ^m_fn;

        TResult PerformFn(T x)
        {
            return m_fn(m_val, x);
        }

    public:
        Func2to3(N val, Func<N, T, TResult> ^fn) : m_val(val), m_fn(fn) {}

        static Func<T,TResult>^ Init(N val, Func<N, T, TResult>^ fn)
        {
            Func2to3 ^x = gcnew Func2to3(val, fn);
            return gcnew Func<T, TResult>(x, &Func2to3::PerformFn);
        }
    };

    public ref class AsyncStreamIO : public IIO, public Stream
    {
    private:
        IOCallback *m_cb;
        TPIO **m_tpio;
        int m_numTPIO;
        BlockingCollection<IntPtr> ^m_tpioColl;
        ConcurrentDictionary<Type^, Object^> ^m_cbFns;
        Object ^m_ioLock;
        // Stream stuff
        bool m_canRead;
        bool m_canWrite;
        bool m_canSeek;
        bool m_bBufferless;
        Int64 m_length;
        Int64 m_position;
        Func<Type^, Object^> ^m_cbCreate;

        generic <class T> static Object^ GetNewCbX(Object ^x, Type ^t)
        {
            return gcnew IOCallbackClass<T>((IIO^)x);
        }

        Object^ GetNewCb(Type ^t)
        {
            Assembly ^a = Assembly::GetExecutingAssembly();
            //array<Type^> ^tps = a->GetTypes();
            Type ^cbClass = a->GetType("Prajna.Tools.Native.IOCallbackClass`1");
            //FullName	"Prajna.Tools.Native.IOCallbackClass`1"	System::String^
            array<Type^> ^arr = gcnew array<Type^>(1);
            arr[0] = t;
            Type ^gType = cbClass->MakeGenericType(arr);
            array<Object^> ^ao = gcnew array<Object^>(1);
            ao[0] = this;
            return Activator::CreateInstance(gType, ao);
            //return gcnew IOCallbackClass<T>((IIO^)this);
        }

        generic <class T> IOCallbackClass<T>^ GetCbFn()
        {
            //Func2to3<Object^,Type^,Object^> f(this, gcnew Func<Object^, Type^, Object^>(AsyncStreamIO::GetNewCbX<T>));
            //Func<Type^, Object^> ^getNew = gcnew Func2to3::Init(this, gcnew Func<Type^, Object^>(AsyncStreamIO::GetNewCbX));
            // unmanaged C++ lambda: [this](Type ^t) -> IOCallbackClass<T> { return gcnew IOCallbackClass<T>(this); }
            return (IOCallbackClass<T>^)m_cbFns->GetOrAdd(T::typeid, m_cbCreate);
        }

    protected:
        void virtual Free()
        {
            Lock lock(this);
            int i;

            if (nullptr != m_cb)
            {
                m_cb->Close();
                delete m_cb;
                m_cb = nullptr;
                for (i = 0; i < m_numTPIO; i++)
                    delete m_tpio[i];
                delete[] m_tpio;
            }
            //ICollection<Object^>^ cb = (ICollection<Object^>^)m_cbFns;
        }

    public:
        //AsyncStreamIO(FileStream ^strm) : m_cb(nullptr), m_cbFns(nullptr), m_ioLock(nullptr),
        //    m_canRead(false), m_canWrite(false), m_canSeek(false), m_length(0LL), m_position(0LL)
        //{
        //    m_cb = new IOCallback((void*)strm->SafeFileHandle->DangerousGetHandle());
        //    m_cbFns = gcnew Dictionary<Type^, Object^>();
        //    m_ioLock = gcnew Object();
        //}

        AsyncStreamIO(HANDLE h, int maxIO) : m_cb(nullptr)
        {
            int i;

            m_cbCreate = gcnew Func<Type^, Object^>(this, &AsyncStreamIO::GetNewCb);

            m_cbFns = gcnew ConcurrentDictionary<Type^, Object^>();
            m_ioLock = gcnew Object();
            m_tpioColl = gcnew BlockingCollection<IntPtr>();

            m_cb = new IOCallback(h);
            m_numTPIO = maxIO;
            m_tpio = new TPIO*[m_numTPIO];
            for (i = 0; i < maxIO; i++)
            {
                m_tpio[i] = new TPIO(h);
                m_tpioColl->Add((IntPtr)m_tpio[i]);
            }
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

        bool BufferLess() { return m_bBufferless;  }

        virtual void UpdatePos(IntPtr tpio, Int64 amt) = IIO::UpdateIO
        {
            m_position += amt;
            m_length = max(m_length, m_position);
            if (IntPtr::Zero != tpio)
                m_tpioColl->Add(tpio);
        }

        void SetLock(Object^ lockObj)
        {
            m_ioLock = lockObj;
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

        static AsyncStreamIO^ OpenFile(String^ name, FileAccess access, FileOptions fOpt, bool bBufferLess)
        {
            array<Char> ^nameArr = name->ToCharArray();
            pin_ptr<Char> namePtr = &nameArr[0];
            Char *pName = (Char*)namePtr;
            int dwFlags = (int)fOpt;
            if (bBufferLess) dwFlags |= FILE_FLAG_NO_BUFFERING;
            Int32 accessMode = 0;
            Int32 creation = 0;
            //if ((int)access & (int)FileAccess::Read)
            //    accessMode |= GENERIC_READ;
            //if ((int)access & (int)FileAccess::Write)
            //    accessMode |= GENERIC_WRITE;
            switch (access)
            {
            case FileAccess::Read:
                accessMode = GENERIC_READ;
                creation = OPEN_EXISTING;
                break;
            case FileAccess::Write:
                accessMode = GENERIC_WRITE;
                creation = CREATE_ALWAYS;
                break;
            case FileAccess::ReadWrite:
                accessMode = GENERIC_READ | GENERIC_WRITE;
                creation = OPEN_ALWAYS;
                break;
            }   
            IntPtr h = (IntPtr)CreateFile(pName, accessMode, FILE_SHARE_READ, nullptr, creation, dwFlags, nullptr);
            AsyncStreamIO ^io = gcnew AsyncStreamIO((HANDLE)h, 1);
            io->m_bBufferless = bBufferLess;
            io->m_position = 0LL;
            pin_ptr<__int64> pLen = &io->m_length;
            int ret = io->m_cb->FileSize((__int64*)pLen);
            if (!ret)
                throw gcnew IOException("Unable to get file length");
            io->m_canRead = ((accessMode & GENERIC_READ) != 0);
            io->m_canWrite = ((accessMode & GENERIC_WRITE) != 0);
            io->m_canSeek = true;
            return io;
        }

        generic <class T> int ReadFilePos(IntPtr ptr, array<T> ^pBuffer, int offset, int nNum, IOCallbackDel<T> ^cb, Object ^state, Int64 position)
        {
            //Lock lock(m_ioLock);
            IOCallbackClass<T>^ cbFn = GetCbFn<T>();
            IOState<T>^ ioState = gcnew IOState<T>();
            IntPtr tpio = m_tpioColl->Take(); // may block until one is available
            IntPtr pBuf = cbFn->Set(ptr, state, pBuffer, offset, cb, tpio, ioState);
            int ret = m_cb->ReadFileAsync<byte>((TPIO*)tpio.ToPointer(), (byte*)pBuf.ToPointer(), nNum*cbFn->TypeSize(), cbFn->CbFn(), ioState->SelfPtr(), position*cbFn->TypeSize());
            if (-1 == ret || ret > 0)
            {   // > 0 not really an error, but finished sync, so no callback
                if (-1 == ret)
                {
                    Int32 error = GetLastError();
                    throw gcnew Exception("Error reading file:" + error.ToString());
                }
                cbFn->OnNoCallback(ioState);
            }
            return ret;
        }

        generic <class T> __forceinline int ReadFilePos(array<T> ^pBuffer, int offset, int nNum, IOCallbackDel<T> ^cb, Object ^state, Int64 position)
        {
            return ReadFilePos(IntPtr::Zero, pBuffer, offset, nNum, cb, state, position);
        }

        generic <class T> __forceinline int ReadFile(array<T> ^pBuffer, int offset, int nNum, IOCallbackDel<T> ^cb, Object ^state)
        {
            return ReadFilePos(pBuffer, offset, nNum, cb, state, m_position);
        }

        generic <class T> int WriteFilePos(IntPtr ptr, array<T> ^pBuffer, int offset, int nNum, IOCallbackDel<T> ^cb, Object ^state, Int64 position)
        {
            //Lock lock(m_ioLock);
            IOCallbackClass<T>^ cbFn = GetCbFn<T>();
            IOState<T> ^ioState = gcnew IOState<T>();
            IntPtr tpio = m_tpioColl->Take();
            IntPtr pBuf = cbFn->Set(ptr, state, pBuffer, offset, cb, tpio, ioState);
            printf("Write first elem: %d\n", *(int*)pBuf.ToPointer());
            int ret = m_cb->WriteFileAsync<byte>((TPIO*)tpio.ToPointer(), (byte*)pBuf.ToPointer(), nNum*cbFn->TypeSize(), cbFn->CbFn(), ioState->SelfPtr(), position*cbFn->TypeSize());
            if (-1 == ret || ret > 0)
            {   // > 0 not really an error, but finished sync, so no callback
                if (-1 == ret)
                {
                    Int32 error = GetLastError();
                    throw gcnew Exception("Error writing file:" + error.ToString());
                }
                cbFn->OnNoCallback(ioState);
            }
            return ret;
        }

        generic <class T> __forceinline int WriteFilePos(array<T> ^pBuffer, int offset, int nNum, IOCallbackDel<T> ^cb, Object ^state, Int64 position)
        {
            return WriteFilePos(IntPtr::Zero, pBuffer, offset, nNum, cb, state, position);
        }

        generic <class T> __forceinline int WriteFile(array<T> ^pBuffer, int offset, int nNum, IOCallbackDel<T> ^cb, Object ^state)
        {
            return WriteFilePos(pBuffer, offset, nNum, cb, state, m_position);
        }

        generic <class T> int ReadFileSync(array<T> ^pBuffer, int offset, int nNum)
        {
            Lock lock(m_ioLock);
            pin_ptr<T> pBuf = &pBuffer[0];
            int amtRead = m_cb->ReadFileSync<byte>((byte*)pBuf, nNum*sizeof(T));
            if (amtRead >= 0)
                UpdatePos(IntPtr::Zero, amtRead);
            return amtRead;
        }

        generic <class T> int WriteFileSync(array<T> ^pBuffer, int offset, int nNum)
        {
            Lock lock(m_ioLock);
            pin_ptr<T> pBuf = &pBuffer[0];
            int amtWrite = m_cb->WriteFileSync<byte>((byte*)pBuf, nNum*sizeof(T));
            if (amtWrite >= 0)
                UpdatePos(IntPtr::Zero, amtWrite);
            return amtWrite;
        }

        generic <class T> void AdjustFilePosition(int amt)
        {
            IOCallbackClass<T>^ cbFn = GetCbFn<T>();
            Lock lock(m_ioLock);
            Int64 newPos;
            m_position = m_position + amt;
            if (FALSE == m_cb->SeekFile(amt*cbFn->TypeSize(), (Int32)SeekOrigin::Current, &newPos))
                throw gcnew IOException("Unable to seek");
            if (FALSE == m_cb->SetFileEnd())
                throw gcnew IOException("Unable to set end of file");
        }
    };
}
}
}


