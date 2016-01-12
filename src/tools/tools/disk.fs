(*---------------------------------------------------------------------------
	Copyright 2013 Microsoft

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.                                                      

    File: 
        Disk.fs

 ---------------------------------------------------------------------------*)
namespace Prajna.Tools

open System
open System.IO
open System.Threading
open Prajna.Tools.Queue
open Prajna.Tools.Network
open System.Collections.Generic
open System.Collections.Concurrent
open System.Runtime.CompilerServices

type internal StrmReq<'TIn,'T> =
    struct
        val mutable input : 'TIn
        val mutable index : 'T
        val mutable strm : Stream
        val mutable cb : Option<Stream*byte[]*int*int*int->unit>
        val mutable oper : 'TIn*'T*Stream*byte[]*int*int*Option<Stream*byte[]*int*int*int->unit>->unit
        val mutable buf : byte[]
        val mutable offset : int
        val mutable cnt : int
        internal new (_input, _index, _strm, _cb, _oper, _buf, _offset, _cnt) = {
            input = _input
            index = _index
            strm = _strm
            cb = _cb
            oper = _oper
            buf = _buf
            offset = _offset
            cnt = _cnt
        }
    end

type BufIOEvent =
    //struct
        val mutable buf : byte[]
        val mutable offset : int
        val mutable cnt : int
        val mutable amt : int ref
        val mutable event : ManualResetEvent
        new(_buf, _offset, _cnt) = {
            buf = _buf
            offset = _offset
            cnt = _cnt
            amt = ref 0
            event = new ManualResetEvent(false)
        }

        interface IDisposable with
            override x.Dispose() =
                x.event.Dispose()
    //end

type StrmIOReq<'TIn,'T> internal () =
    let q = ConcurrentDictionary<'T, ConcurrentQueue<StrmReq<'TIn,'T>>*Semaphore*int ref*int>()
    let strmsRead = ConcurrentDictionary<'TIn, Stream>()
    let strmsWrite = ConcurrentDictionary<'TIn, Stream>()
    let mutable getIndexWrite : 'TIn->'T = (fun _ -> Unchecked.defaultof<'T>)
    let mutable getIndexRead : 'TIn->'T = (fun _ -> Unchecked.defaultof<'T>)
    let disposed = ref 0
    let nullInput = Unchecked.defaultof<'TIn>

    interface IDisposable with
        override x.Dispose() =
            if (Interlocked.CompareExchange(disposed, 1, 0) = 0) then
                for e in q do
                    let (a,s,b,c) = e.Value
                    s.Dispose()
                for s in strmsRead do
                    s.Value.Close()
                for s in strmsWrite do
                    s.Value.Close()

    member x.Init(input : ('T*int)[], mapperWrite : 'TIn->'T, mapperRead : 'TIn->'T) =
        getIndexWrite <- mapperWrite
        getIndexRead <- mapperRead
        for i = 0 to input.Length-1 do
            let (index, max) = input.[i]
            q.[index] <- (ConcurrentQueue<StrmReq<'TIn,'T>>(), new Semaphore(max, max), ref 0, max)            

    member x.OpenStrms(inputRead : 'TIn[], inputWrite : 'TIn[]) =
        for i in inputRead do
            strmsRead.[i] <- x.OpenStreamForRead(i)
        for i in inputWrite do
            strmsWrite.[i] <- x.OpenStreamForWrite(i)

    member private x.OperLoop(index : 'T) =
        let (q, s, outstanding, max) = q.[index]
        let mutable bDone = false
        while (q.Count > 0 && not bDone) do
            if (Interlocked.Increment(outstanding) <= max) then
                let (ret, elem) = q.TryDequeue()
                if (ret) then
                    s.WaitOne() |> ignore
                    elem.oper(elem.input, index, elem.strm, elem.buf, elem.offset, elem.cnt, elem.cb)
                else
                    Interlocked.Decrement(outstanding) |> ignore
            else
                Interlocked.Decrement(outstanding) |> ignore
                bDone <- true

    member private x.OperLoopExec(o : obj) =
        let index = o :?> 'T
        x.OperLoop(index)

    member private x.SetLen (amtReadRef : int ref) (strm : Stream, buf : byte[], offset : int, cnt : int, amtRead : int) =
        amtReadRef := amtRead

    member private x.FireEventCb (event : EventWaitHandle, amtReadRef : int ref) (strm : Stream, buf : byte[], offset : int, cnt : int, amtRead : int) =
        amtReadRef := amtRead
        if (Utils.IsNotNull event) then
            event.Set() |> ignore

    member inline private x.FinishOper(index, outstanding : int ref, s : Semaphore, furtherCb, strm : Stream, buf, offset, cnt, amt, bClose) =
        Interlocked.Decrement(outstanding) |> ignore
        s.Release() |> ignore
        match furtherCb with
            | None -> ()
            | Some(cb) -> cb(strm, buf, offset, cnt, amt)
        if (bClose) then
            strm.Close()
        ThreadPool.QueueUserWorkItem(WaitCallback(x.OperLoopExec), index) |> ignore 

    member val OpenStreamForRead : 'TIn->Stream = (fun _ -> null) with get, set
    member val OpenStreamForWrite : 'TIn->Stream = (fun _ -> null) with get, set

    // Read operations
    member private x.ReadCb(ar : IAsyncResult) =
        let (index, strm, buf, offset, cnt, outstanding, s, furtherCb, bClose) = ar.AsyncState :?> ('T*Stream*byte[]*int*int*int ref*Semaphore*Option<Stream*byte[]*int*int*int->unit>*bool)
        let amtRead = strm.EndRead(ar)
        x.FinishOper(index, outstanding, s, furtherCb, strm, buf, offset, cnt, amtRead, bClose)

    member private x.ReadInternal(input : 'TIn, index : 'T, strm : Stream, buf : byte[], offset : int, cnt : int, outstanding : int ref, s : Semaphore, furtherCb : Option<Stream*byte[]*int*int*int->unit>) =
        try
            let (strm, bClose) =
                if (Utils.IsNull strm) then
                    if (strmsRead.ContainsKey(input)) then
                        (strmsRead.[input], false)
                    else
                        (x.OpenStreamForRead(input), true)
                else
                    (strm, false)  
            strm.BeginRead(buf, offset, cnt, AsyncCallback(x.ReadCb), (index, strm, buf, offset, cnt, outstanding, s, furtherCb, bClose)) |> ignore
        with e ->
            s.Release() |> ignore
            Interlocked.Decrement(outstanding) |> ignore
            reraise()

    member x.TryRead(input : 'TIn, index : 'T, strm : Stream, buf : byte[], offset : int, cnt : int, furtherCb) =
        let (q, s, outstanding, max) = q.[index]
        if (s.WaitOne(0)) then
            Interlocked.Increment(outstanding) |> ignore
            x.ReadInternal(input, index, strm, buf, offset, cnt, outstanding, s, furtherCb)
            true
        else
            false

    member x.TryReadIn(input : 'TIn, strm : Stream, buf : byte[], offset : int, cnt : int, furtherCb) =
        x.TryRead(input, getIndexRead(input), strm, buf, offset, cnt, furtherCb)

    member private x.DoRead(input : 'TIn, index : 'T, strm : Stream, buf : byte[], offset : int, cnt : int, furtherCb) =
        let (q, s, outstanding, max) = q.[index]
        x.ReadInternal(input, index, strm, buf, offset, cnt, outstanding, s, furtherCb)

    member x.AddReadReq(input  : 'TIn, strm : Stream, buf : byte[], offset : int, cnt : int, furtherCb) =
        let index = getIndexRead(input)
        let (q, s, outstanding, max) = q.[index]
        let bDone = x.TryRead(input, index, strm, buf, offset, cnt, furtherCb)
        if not bDone then
            q.Enqueue(new StrmReq<'TIn,'T>(input, index, strm, furtherCb, x.DoRead, buf, offset, cnt))

    // !!only can use if strm is not null
    member x.AddDirectReadReq(index : 'T, strm : Stream, buf : byte[], offset : int, cnt : int, furtherCb) =
        let (q, s, outstanding, max) = q.[index]
        let bDone = x.TryRead(nullInput, index, strm, buf, offset, cnt, furtherCb)
        if not bDone then
            q.Enqueue(new StrmReq<'TIn,'T>(nullInput, index, strm, furtherCb, x.DoRead, buf, offset, cnt))

    member x.AddReadReq(input  : 'TIn, strm : Stream, buf : byte[], offset : int, cnt : int, event, amtReadRef : int ref) =
        let index = getIndexRead(input)
        let (q, s, outstanding, max) = q.[index]
        let furtherCb = Some(x.FireEventCb (event, amtReadRef))
        event.Reset() |> ignore
        let bDone = x.TryRead(input, index, strm, buf, offset, cnt, furtherCb)
        if not bDone then
            q.Enqueue(new StrmReq<'TIn,'T>(input, index, strm, furtherCb, x.DoRead, buf, offset, cnt))

    member x.AddReadReq(input : 'TIn, strm : Stream, bufIO : BufIOEvent) =
        x.AddReadReq(input, strm, bufIO.buf, bufIO.offset, bufIO.cnt, bufIO.event, bufIO.amt)

    member x.SyncRead(input : 'TIn, strm : Stream, buf : byte[], offset : int, cnt : int) : int =
        let index = getIndexRead(input)
        let (q, s, outstanding, max) = q.[index]
        let amtReadRef = ref 0
        let furtherCb = Some(x.SetLen amtReadRef)
        s.WaitOne() |> ignore
        Interlocked.Increment(outstanding) |> ignore
        x.ReadInternal(input, index, strm, buf, offset, cnt, outstanding, s, furtherCb)
        !amtReadRef

    // Write operations
    member private x.WriteCb(ar : IAsyncResult) =
        let (index, strm, buf, offset, cnt, outstanding, s, furtherCb, bClose) = ar.AsyncState :?> ('T*Stream*byte[]*int*int*int ref*Semaphore*Option<Stream*byte[]*int*int*int->unit>*bool)
        strm.EndWrite(ar)
        x.FinishOper(index, outstanding, s, furtherCb, strm, buf, offset, cnt, 0, bClose)

    member private x.WriteInternal(input : 'TIn, index : 'T, strm : Stream, buf : byte[], offset : int, cnt : int, outstanding : int ref, s : Semaphore, furtherCb : Option<Stream*byte[]*int*int*int->unit>) =
        try
            let (strm, bClose) =
                if (Utils.IsNull strm) then
                    if (strmsWrite.ContainsKey(input)) then
                        (strmsWrite.[input], false)
                    else
                        (x.OpenStreamForWrite(input), true)
                else
                    (strm, false)
            strm.BeginWrite(buf, offset, cnt, AsyncCallback(x.WriteCb), (index, strm, buf, offset, cnt, outstanding, s, furtherCb, bClose)) |> ignore
        with e ->
            s.Release() |> ignore
            Interlocked.Decrement(outstanding) |> ignore
            reraise()

    member x.TryWrite(input : 'TIn, index : 'T, strm : Stream, buf : byte[], offset : int, cnt : int, furtherCb) =
        let (q, s, outstanding, max) = q.[index]
        if (s.WaitOne(0)) then
            Interlocked.Increment(outstanding) |> ignore
            x.WriteInternal(input, index, strm, buf, offset, cnt, outstanding, s, furtherCb)
            true
        else
            false

    member x.TryWriteIn(input : 'TIn, strm : Stream, buf : byte[], offset : int, cnt : int, furtherCb) =
        x.TryWrite(input, getIndexWrite(input), strm, buf, offset, cnt, furtherCb)

    member private x.DoWrite(input : 'TIn, index : 'T, strm : Stream, buf : byte[], offset : int, cnt : int, furtherCb) =
        let (q, s, outstanding, max) = q.[index]
        x.WriteInternal(input, index, strm, buf, offset, cnt, outstanding, s, furtherCb)

    member x.AddWriteReq(input  : 'TIn, strm : Stream, buf : byte[], offset : int, cnt : int, furtherCb) =
        let index = getIndexWrite(input)
        let (q, s, outstanding, max) = q.[index]
        let bDone = x.TryWrite(input, index, strm, buf, offset, cnt, furtherCb)
        if not bDone then
            q.Enqueue(new StrmReq<'TIn,'T>(input, index, strm, furtherCb, x.DoWrite, buf, offset, cnt))

    member x.AddDirectWriteReq(index : 'T, strm : Stream, buf : byte[], offset : int, cnt : int, furtherCb) =
        let (q, s, outstanding, max) = q.[index]
        let bDone = x.TryWrite(nullInput, index, strm, buf, offset, cnt, furtherCb)
        if not bDone then
            q.Enqueue(new StrmReq<'TIn,'T>(nullInput, index, strm, furtherCb, x.DoWrite, buf, offset, cnt))

    member x.AddWriteReq(input  : 'TIn, strm : Stream, buf : byte[], offset : int, cnt : int, event) =
        let index = getIndexWrite(input)
        let (q, s, outstanding, max) = q.[index]
        let furtherCb = Some(x.FireEventCb (event, ref 0))
        event.Reset() |> ignore
        let bDone = x.TryWrite(input, index, strm, buf, offset, cnt, furtherCb)
        if not bDone then
            q.Enqueue(new StrmReq<'TIn,'T>(input, index, strm, furtherCb, x.DoWrite, buf, offset, cnt))

    member x.AddWriteReq(input : 'TIn, strm : Stream, bufIO : BufIOEvent) =
        x.AddWriteReq(input, strm, bufIO.buf, bufIO.offset, bufIO.cnt, bufIO.event)

    member x.SyncWrite(input : 'TIn, strm : Stream, buf : byte[], offset : int, cnt : int) =
        let index = getIndexWrite(input)
        let (q, s, outstanding, max) = q.[index]
        s.WaitOne() |> ignore
        Interlocked.Increment(outstanding) |> ignore
        x.WriteInternal(input, index, strm, buf, offset, cnt, outstanding, s, None)

[<Extension>]
type IOReq() =
    static member DiskMap(disk : string) =
        disk.[0]

    static member NewDiskIO(writeLoc : (char*int)[]) =
        let x = new StrmIOReq<string, char>()
        x.Init(writeLoc, IOReq.DiskMap, IOReq.DiskMap)
        x.OpenStreamForWrite <- (fun name -> File.Open(name, FileMode.Append, FileAccess.Write) :> Stream)
        x.OpenStreamForRead <- (fun name -> File.Open(name, FileMode.Open, FileAccess.Read) :> Stream)
        x

    [<Extension>]
    static member AddReadReq(ioReq : StrmIOReq<'TIn,'T>, input : 'TIn, buf : byte[], offset : int, cnt : int, event, amt : int ref) =
        ioReq.AddReadReq(input, null, buf, offset, cnt, event, amt)

    [<Extension>]
    static member AddReadReq(ioReq : StrmIOReq<'TIn,'T>, input : 'TIn, buf : BufIOEvent) =
        ioReq.AddReadReq(input, null, buf)

    [<Extension>]
    static member AddReadReq(ioReq : StrmIOReq<'TIn,'T>, input : 'TIn, buf : byte[], offset : int, cnt : int, furtherCb) =
        ioReq.AddReadReq(input, null, buf, offset, cnt, Some(furtherCb))

    [<Extension>]
    static member SyncRead(ioReq : StrmIOReq<'TIn,'T>, input : 'TIn, buf : byte[], offset : int, cnt : int) =
        ioReq.SyncRead(input, null, buf, offset, cnt)

    [<Extension>]
    static member AddWriteReq(ioReq : StrmIOReq<'TIn,'T>, input : 'TIn, buf : byte[], offset : int, cnt : int, event : EventWaitHandle, amt : int ref) =
        ioReq.AddWriteReq(input, null, buf, offset, cnt, event)

    [<Extension>]
    static member AddWriteReq(ioReq : StrmIOReq<'TIn,'T>, input : 'TIn, buf : BufIOEvent) =
        ioReq.AddWriteReq(input, null, buf)

    [<Extension>]
    static member AddWriteReq(ioReq : StrmIOReq<'TIn,'T>, input : 'TIn, buf : byte[], offset : int, cnt : int, furtherCb) =
        ioReq.AddWriteReq(input, null, buf, offset, cnt, Some(furtherCb))

    [<Extension>]
    static member SyncWrite(ioReq : StrmIOReq<'TIn,'T>, input : 'TIn, buf : byte[], offset : int, cnt : int) =
        ioReq.SyncWrite(input, null, buf, offset, cnt)

type internal FileCache private () =
    new (buf, initOffset, len) as x =
        new FileCache()
        then
            x.Init(buf, initOffset, len)

    static member CreateAlign(size : int, align : int) =
        let x = new FileCache()
        x.Align <- new ArrAlign<byte>(size, align)
        x.Init(x.Align.Arr, x.Align.Offset, x.Align.Size)
        x

    member val Align : ArrAlign<byte> = null with get, set
    member val Buf : byte[] = null with get, set
    member val InitOffset : int = 0 with get, set
    member val Offset : int = 0 with get, set
    member val Finished : int ref = ref 0 with get
    member val Len : int = 0 with get, set
    member val LenRead : int = 0 with get, set
    member val CanWriteToMem : ManualResetEventSlim = new ManualResetEventSlim(true) with get
    member val CanRead : ManualResetEventSlim = new ManualResetEventSlim(false) with get
    member val Finish : ManualResetEvent = new ManualResetEvent(true) with get

    interface IDisposable with
        override x.Dispose() =
            if (Utils.IsNotNull x.Align) then
                (x.Align :> IDisposable).Dispose()
            x.CanWriteToMem.Dispose()
            x.CanRead.Dispose()
            x.Finish.Dispose()

    member x.Init(buf, initOffset, len) =
        x.Buf <- buf
        x.InitOffset <- initOffset
        x.Len <- len
        x.LenRead <- 0
        x.Offset <- 0

    member x.Reset() =
        x.Len <- 0
        x.LenRead <- 0
        x.Offset <- 0
        x.CanRead.Reset() |> ignore
        x.CanWriteToMem.Set() |> ignore
        x.Finish.Reset() |> ignore

    member x.SetToWrite() =
        x.Offset <- x.LenRead
        x.CanRead.Reset() |> ignore
        x.CanWriteToMem.Set() |> ignore
        x.Finish.Reset() |> ignore

type internal CacheReq private () =
    static member ReadReq(partial, name, event, cb) =
        let x = new CacheReq()
        x.bAllowPartial <- partial
        x.reqSize <- 0L
        x.name <- name
        x.strm <- null
        x.amtRem <- 0L
        x.eventKey <- event
        x.cb <- cb
        x

    static member GetReq(size, event, cb) =
        let x = new CacheReq()
        x.bAllowPartial <- false
        x.reqSize <- size
        x.name <- ""
        x.strm <- null
        x.amtRem <- 0L
        x.eventKey <- event
        x.cb <-  cb
        x

    member x.TransferName(name : string) =
        x.ReleaseStream()
        x.name <- name
        x.ReleasedStream <- 0

    member val private ReleasedStream = 0 with get, set
    member x.ReleaseStream() =
        if (x.ReleasedStream = 0) then
            lock (x) (fun () ->
                if (x.ReleasedStream = 0) then
                    if (Utils.IsNotNull x.strm) then
                        x.strm.Close()
                        x.strm <- null
            )

    interface IDisposable with
        override x.Dispose() =
            x.ReleaseStream()
            GC.SuppressFinalize(x)

    [<DefaultValue>] val mutable bAllowPartial : bool
    [<DefaultValue>] val mutable reqSize : int64
    [<DefaultValue>] val mutable name : string
    [<DefaultValue>] val mutable strm : Stream
    [<DefaultValue>] val mutable amtRem : int64
    [<DefaultValue>] val mutable eventKey : EventWaitHandle
    [<DefaultValue>] val mutable cb : Option<unit->unit>

    member val CacheIndex : List<int> = List<int>()
    member val Wait : List<WaitHandle> = List<WaitHandle>()

type internal SharedCache<'K when 'K:equality>(cacheArr : (int ref*FileCache)[], readBufferSize : int, writeBufferSize : int, diskIO : DiskIO<'K>) =
    let st = SingleThreadExec.ThreadPoolExecOnce()
    let eAvail = new ManualResetEvent(true)
    let numAvail = ref 0
    let segments : (int ref*FileCache)[] = cacheArr
    let readQ = ConcurrentQueue<CacheReq>()
    let outstandingList = ConcurrentDictionary<EventWaitHandle, CacheReq>()
    let mutable curRem = 0L
    let mutable reqId = -1L
    let mutable reqEvent : EventWaitHandle = null
    let readQCnt = ref 0

    let finishRead (cacheIndex : int, id : EventWaitHandle, bDone : bool) (strm : Stream, buf : byte[], offset : int, cnt : int, amtRead : int) =
        let req = outstandingList.[id]
        let (used, cache) = segments.[cacheIndex]
        cache.CanRead.Set() |> ignore
        cache.Finish.Set() |> ignore
        cache.LenRead <- amtRead
        if (bDone) then
            WaitHandle.WaitAll(req.Wait.ToArray()) |> ignore
            req.ReleaseStream()
            req.eventKey.Set() |> ignore
            match req.cb with
                | None -> ()
                | Some(cb) -> cb()
        else if (req.bAllowPartial) then
            req.eventKey.Set() |> ignore

    let finishGet (cacheIndex : int, id : EventWaitHandle, bDone : bool) =
        let req = outstandingList.[id]
        let (used, cache) = segments.[cacheIndex]
        cache.CanRead.Set() |> ignore
        cache.Finish.Set() |> ignore
        if (bDone) then
            req.eventKey.Set() |> ignore
            match req.cb with
                | None -> ()
                | Some(cb) -> cb()
        else if (req.bAllowPartial) then
            req.eventKey.Set() |> ignore

    interface IDisposable with
        override x.Dispose() =
            eAvail.Dispose()
            for s in segments do
                let (a, cache) = s
                (cache :> IDisposable).Dispose()
            let r = ref Unchecked.defaultof<CacheReq>
            while (readQ.TryDequeue(r)) do
                (!r :> IDisposable).Dispose()
            for e in outstandingList do
                (e.Value :> IDisposable).Dispose()
            GC.SuppressFinalize(x)

    member x.AddRead(name, event : EventWaitHandle, callback) =
        event.Reset() |> ignore
        readQ.Enqueue(CacheReq.ReadReq(false, name, event, callback))
        Interlocked.Increment(readQCnt) |> ignore
        st.ExecOnceTP(x.TryRead)

    member x.GetCache(size : int64, event : EventWaitHandle, callback) =
        if (size > 0L) then
            event.Reset() |> ignore
            readQ.Enqueue(CacheReq.GetReq(size, event, callback))
            Interlocked.Increment(readQCnt) |> ignore
            st.ExecOnceTP(x.TryRead)

    member x.WaitAndGet(event : EventWaitHandle) : List<int> =
        event.WaitOne() |> ignore
        outstandingList.[event].CacheIndex

    member x.GetItem(l : List<int>, index : int) =
        let (used, cache) = segments.[l.[index]]
        (cache.Buf, cache.Offset, cache.Len, cache.LenRead)

    member x.Clear(event : EventWaitHandle) =
        let (ret, req) = outstandingList.TryRemove(event)
        if (ret) then
            req.ReleaseStream()
            for lElem in req.CacheIndex do
                let (used, cache) = segments.[lElem]
                used := 0
                cache.Reset()
                Interlocked.Increment(numAvail) |> ignore
            (req :> IDisposable).Dispose()

    member x.SetToWrite(event : EventWaitHandle, name : string) =
        if (outstandingList.ContainsKey(event)) then
            outstandingList.[event].TransferName(name)
            for l in outstandingList.[event].CacheIndex do
                (snd segments.[l]).SetToWrite()

    member private x.FinishWrite(req : CacheReq, listIndex : int) (a) =
        let (used, cache) = segments.[req.CacheIndex.[listIndex]]
        used := 0
        cache.Reset()
        Interlocked.Increment(numAvail) |> ignore
        let newIndex = listIndex + 1
        if (req.CacheIndex.Count > newIndex) then
            let (used, cache) = segments.[req.CacheIndex.[newIndex]]
            diskIO.AddDirectWriteReq(req.name.[0], req.strm, cache.Buf, cache.InitOffset, cache.Offset, Some(x.FinishWrite (req, newIndex)))
        else
            req.ReleaseStream()
            (req :> IDisposable).Dispose()
            
    member x.WriteAndClear(event : EventWaitHandle) =
        let (ret, req) = outstandingList.TryRemove(event)
        if (ret && req.CacheIndex.Count > 0) then
            let fOpt = FileOptions.Asynchronous ||| FileOptions.WriteThrough
            let strmWrite = new FileStream(req.name, FileMode.Create, FileAccess.Write, FileShare.Read, writeBufferSize, fOpt)
            req.strm <- strmWrite
            let (used, cache) = segments.[req.CacheIndex.[0]]
            diskIO.AddDirectWriteReq(req.name.[0], strmWrite, cache.Buf, cache.InitOffset, cache.Offset, Some(x.FinishWrite (req, 0)))

    member x.TryRead() =
        let mutable bDone = false
        while (!readQCnt > 0 && not bDone) do
            if (Interlocked.Decrement(numAvail) >= 0) then
                let mutable bFound = false
                let mutable i = 0
                let mutable index = 0
                while (not bFound && i < cacheArr.Length) do
                    let (used, cache) = cacheArr.[i]
                    if (Interlocked.CompareExchange(used, 1, 0) = 0) then
                        bFound <- true
                        index <- i
                        cache.Reset()
                    i <- i + 1               
                if (bFound) then
                    if (0L = curRem) then
                        let (ret, res) = readQ.TryDequeue()
                        if (ret) then
                            if (0L = res.amtRem) then
                                reqId <- reqId + 1L
                                reqEvent <- res.eventKey
                                if (res.reqSize > 0L) then
                                    res.amtRem <- curRem
                                    outstandingList.[reqEvent] <- res
                                else
                                    let fOpt = FileOptions.Asynchronous ||| FileOptions.SequentialScan
                                    let strm = new FileStream(res.name, FileMode.Open, FileAccess.Read, FileShare.Read, readBufferSize, fOpt)
                                    res.amtRem <- curRem
                                    res.strm <- strm
                                    outstandingList.[reqEvent] <- res
                            curRem <- res.amtRem
                        else
                            bDone <- true
                    if (not bDone) then
                        let req = outstandingList.[reqEvent]
                        let (used, cache) = cacheArr.[index]
                        let toRead = Math.Min(curRem, int64 cache.Len)
                        curRem <- curRem - toRead
                        cache.CanRead.Reset()
                        req.CacheIndex.Add(index)
                        req.Wait.Add(cache.Finish)
                        if (req.reqSize > 0L) then
                            finishGet(index, reqEvent, 0L=curRem)
                        else
                            let lock =
                                if (req.CacheIndex.Count <= 1) then
                                    null
                                else
                                    (snd cacheArr.[req.CacheIndex.[req.CacheIndex.Count-2]]).Finish
                            let indexUse = index
                            Component<_>.WaitAndExecOnSystemTP(lock, -1) ((fun _ _ ->                                
                                diskIO.AddDirectReadReq(req.name.[0], req.strm, cache.Buf, cache.InitOffset, cache.Len, Some(finishRead(indexUse, reqEvent, 0L=curRem)))
                            ), null)
                else
                    bDone <- true
            else
                Interlocked.Increment(numAvail) |> ignore
                bDone <- true
        
and DiskIO<'K when 'K:equality>(writeLoc : (char*int)[]) as x =
    inherit StrmIOReq<'K, char>()

    let mutable writeBufferSize = 0
    let mutable readBufferSize = 0

    let writeCache = Dictionary<'K,string*int*Stream*FileCache[]>()
    let readCache = Dictionary<'K,string*int*Stream*FileCache[]>()
    let sharedCache : Dictionary<_,_> = Dictionary<char, SharedCache<'K>>()

    let mutable sharedReadWC : WaitCallback = null
    let st = SingleThreadExec.ThreadPoolExecOnce()

    let mapWrite (key : 'K) : char =
        let (name,_,_,_) = writeCache.[key]
        name.[0]

    let mapRead (key : 'K) : char =
        let (name,_,_,_) = readCache.[key]
        name.[0]

    do
        x.Init(writeLoc, mapWrite, mapRead)
        x.OpenStreamForWrite <- (fun key -> 
            let mutable (name,_,_,_) = writeCache.[key]
            File.Open(name, FileMode.Append, FileAccess.Write) :> Stream
        )
        x.OpenStreamForRead <- (fun key ->
            let (name,_,_,_) = readCache.[key]
            File.Open(name, FileMode.Open, FileAccess.Read) :> Stream
        )

    let createCacheElem (segmentSize : int, align : int) (index : int) =
        FileCache.CreateAlign(segmentSize, align)

    let finishWrite (e : ManualResetEventSlim) (o) =
        e.Set() |> ignore

    let finishRead (cache : FileCache) (o) =
        let (strm, buf, offset, cnt, amt) = o
        cache.Offset <- 0
        cache.LenRead <- amt
        cache.CanRead.Set() |> ignore

    let mutable bFirstCacheRead = true

    interface IDisposable with
        override x.Dispose() =
            for w in writeCache do
                let (_,_,s,arr) = w.Value
                if (Utils.IsNotNull s) then
                    s.Close()
                for a in arr do
                    (a :> IDisposable).Dispose()
            for r in readCache do
                let (_,_,s,arr) = r.Value
                if (Utils.IsNotNull s) then
                    s.Close()
                for a in arr do
                    (a :> IDisposable).Dispose()
            GC.SuppressFinalize(x)
        
    member x.InitSharedCache(segmentSize : int, numSegments : int, ?align : int) =
        let align = defaultArg align 1
        for w in writeLoc do
            let (key, max) = w
            let cacheArr = Array.init numSegments (fun _ -> ref 0, createCacheElem (segmentSize, align) (0))
            sharedCache.[key] <- new SharedCache<'K>(cacheArr, readBufferSize, writeBufferSize, x)

    member x.InitWriteCache(key : 'K, name : string, segmentSize : int, numSegments : int, bOpenFile : bool, ?align : int) =
        let align = defaultArg align 1
        lock (writeCache) (fun () ->
            let strm = 
                if (bOpenFile) then
                    let mutable fOpt = FileOptions.Asynchronous ||| FileOptions.SequentialScan
                    if writeBufferSize = 0 then
                        fOpt <- fOpt ||| FileOptions.WriteThrough
                    new FileStream(name, FileMode.Create, FileAccess.Write, FileShare.Read, writeBufferSize, fOpt)
                else
                    null
            writeCache.[key] <- (name, 0, strm :> Stream, Array.init numSegments (createCacheElem (segmentSize, align)))
        )

    member x.InitReadCache(key : 'K, name : string, segmentSize : int, numSegments : int, ?align : int) =
        let align = defaultArg align 1
        let mutable fOpt = FileOptions.Asynchronous ||| FileOptions.SequentialScan
        let strm = new FileStream(name, FileMode.Open, FileAccess.Read, FileShare.Read, readBufferSize, fOpt)
        // prefill cache
        let arr = Array.zeroCreate<FileCache>(numSegments)
        let mutable bDone = false
        let mutable i = 0
        while (not bDone) do
            arr.[i] <- createCacheElem(segmentSize, align)(i)
            arr.[i].LenRead <- strm.Read(arr.[i].Buf, arr.[i].InitOffset, arr.[i].Len)
            arr.[i].CanRead.Set() |> ignore
            i <- i + 1
            if (arr.[i].LenRead < arr.[i].Len || i = numSegments) then
                bDone <- true
        readCache.[key] <- (name, 0, strm :> Stream, arr)

    member x.Write(key : 'K, buf : byte[], offset : int, cnt : int) =
        let copyIndex : int ref = ref 0
        let copyStart : int ref = ref 0
        let (name, index, strm, cache) = writeCache.[key]
        lock (cache) (fun () ->
            let mutable index = index
            if (cache.[index].Offset + cnt > cache.[index].Len) then
                // wait for finish
                let sw = SpinWait()
                while (!cache.[index].Finished < cache.[index].Offset) do
                    sw.SpinOnce()
                cache.[index].CanWriteToMem.Reset() |> ignore
                x.AddWriteReq(key, strm, cache.[index].Buf, cache.[index].InitOffset, cache.[index].Offset, Some(finishWrite cache.[index].CanWriteToMem))
                index <- index + 1
                if (index = cache.Length) then
                    index <- 0
                cache.[index].Offset <- 0
                cache.[index].Finished := 0
                writeCache.[key] <- (name, index, strm, cache)
            cache.[index].CanWriteToMem.Wait() |> ignore
            copyIndex := index
            copyStart := cache.[index].Offset
            cache.[index].Offset <- cache.[index].Offset + cnt
        )
        Buffer.BlockCopy(buf, offset, cache.[!copyIndex].Buf, cache.[!copyIndex].InitOffset + !copyStart, cnt)
        Interlocked.Add(cache.[!copyIndex].Finished, cnt) |> ignore

    member x.Read(key : 'K, buf : byte[], offset : int, cnt : int) =
        let copyIndex : int ref = ref 0
        let copyStart : int ref = ref 0
        let mutable (name, index, strm, cache) = readCache.[key]
        let mutable rem = cnt
        let mutable curOffset = offset
        while (rem > 0) do
            cache.[index].CanRead.Wait() |> ignore
            let toCopy = Math.Min(rem, cache.[index].LenRead - cache.[index].Offset)
            if (toCopy > 0) then
                Buffer.BlockCopy(cache.[index].Buf, cache.[index].InitOffset + cache.[index].Offset, buf, curOffset, toCopy)
                cache.[index].Offset <- cache.[index].Offset + toCopy
                curOffset <- curOffset + toCopy
                rem <- rem - toCopy
            if (cache.[index].Offset = cache.[index].LenRead) then
                // read new for curIndex
                cache.[index].CanRead.Reset() |> ignore
                // make sure previous request is finished
                if (cache.Length > 1) then
                    cache.[(index-1)%cache.Length].CanRead.Wait() |> ignore
                cache.[index].CanRead.Reset() |> ignore
                x.AddReadReq(key, strm, cache.[index].Buf, cache.[index].InitOffset, cache.[index].Len, Some(finishRead cache.[index]))
                index <- index + 1
                if (index = cache.Length) then
                    index <- 0
                readCache.[key] <- (name, index, strm, cache)

    // a request to read next block into cache means previous block finished, issue a new read request for previous block
    member x.ReadCache(key : 'K) : byte[]*int*int =
        let mutable (name, index, strm, cache) = readCache.[key]
        // start a request to reread index-1 provided         
        if (not bFirstCacheRead) then
            let prevIndex = (index-1) % cache.Length
            cache.[prevIndex].CanRead.Reset() |> ignore
            if (cache.Length > 1) then
                cache.[(prevIndex-1) % cache.Length].CanRead.Wait() |> ignore
            cache.[index].CanRead.Reset() |> ignore
            x.AddReadReq(key, strm, cache.[prevIndex].Buf, cache.[prevIndex].InitOffset, cache.[prevIndex].Len, Some(finishRead cache.[prevIndex]))
        else
            bFirstCacheRead <- false
        cache.[index].CanRead.Wait() |> ignore
        let indexRead = index
        index <- index + 1
        if (index = cache.Length) then
            index <- 0
        (cache.[indexRead].Buf, cache.[indexRead].InitOffset, cache.[indexRead].LenRead)

    member x.BeginFileReadUsingSharedMemory(name : string, event : EventWaitHandle, ?callback : unit->unit) =
        let key = name.[0]
        sharedCache.[key].AddRead(name, event, callback)

    member x.SetToWrite(name : string, event : EventWaitHandle) =
        let key = name.[0]
        sharedCache.[key].SetToWrite(event, name)

    member x.GetMemory(key : char, size : int64, event : EventWaitHandle, ?callback : unit->unit) =
        sharedCache.[key].GetCache(size, event, callback)

    member x.BeginFileWrite(name : string, event : EventWaitHandle) =
        let key = name.[0]
        sharedCache.[key].WriteAndClear(event)

//type [<AllowNullLiteral>] RefCntBufDisk() =
//    let 
//
//    inherit RefCntBuf<byte>()
//
//    override x.Alloc(size : int) =
//        base.Alloc(size)
//
//// 'CK is cache key
//// 'RK is request key
//type DiskIO2<'CK,'RK>() =
//    let cache = new Dictionary<'CK,
