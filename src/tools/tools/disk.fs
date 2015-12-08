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
        x.Align <- new ByteArrAlign(size, align)
        x.Init(x.Align.Arr, x.Align.Offset, x.Align.Size)
        x

    member val Align : ByteArrAlign = null with get, set
    member val Buf : byte[] = null with get, set
    member val InitOffset : int = 0 with get, set
    member val Offset : int = 0 with get, set
    member val Finished : int ref = ref 0 with get
    member val Len : int = 0 with get, set
    member val CanWriteToMem : ManualResetEventSlim = new ManualResetEventSlim(true) with get

    interface IDisposable with
        override x.Dispose() =
            if (Utils.IsNotNull x.Align) then
                (x.Align :> IDisposable).Dispose()
            x.CanWriteToMem.Dispose()

    member x.Init(buf, initOffset, len) =
        x.Buf <- buf
        x.InitOffset <- initOffset
        x.Len <- len
        x.Offset <- 0
        
type DiskIO<'K when 'K:equality>(writeLoc : (char*int)[]) as x =
    inherit StrmIOReq<'K, char>()

    let mutable writeBufferSize = 0
    let mutable readBufferSize = 0
    let writeCache = Dictionary<'K,string*int*Stream*FileCache[]>()
    let readCache = Dictionary<'K,string*int*Stream*FileCache[]>()
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

    let createCacheElem (segmentSize : int) (index : int) =
        let arr = Array.zeroCreate<byte>(segmentSize)
        new FileCache(arr, 0, segmentSize)

    let finishWrite (e : ManualResetEventSlim) (o) =
        e.Set() |> ignore

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
        
    member x.InitWriteCache(key : 'K, name : string, segmentSize : int, numSegments : int, bOpenFile : bool) =
        lock (writeCache) (fun () ->
            let strm = 
                if (bOpenFile) then
                    let mutable fOpt = FileOptions.Asynchronous ||| FileOptions.SequentialScan
                    if writeBufferSize = 0 then
                        fOpt <- fOpt ||| FileOptions.WriteThrough
                    new FileStream(name, FileMode.Create, FileAccess.Write, FileShare.Read, writeBufferSize, fOpt)
                else
                    null
            writeCache.[key] <- (name, 0, strm :> Stream, Array.init numSegments (createCacheElem segmentSize))
        )

    member x.InitReadCache(key : 'K, name : string, segmentSize : int, numSegments : int, bOpenFile : bool) =
        lock (readCache) (fun () ->
            let strm = 
                if (bOpenFile) then
                    let mutable fOpt = FileOptions.Asynchronous ||| FileOptions.SequentialScan
                    let strm = new FileStream(name, FileMode.Create, FileAccess.Write, FileShare.Read, writeBufferSize, fOpt)
                    // prefill cache

                    strm
                else
                    null
            readCache.[key] <- (name, 0, strm :> Stream, Array.init numSegments (createCacheElem segmentSize))
        )

    member x.Write(key : 'K, buf : byte[], offset : int, cnt : int) =
        let copyIndex : int ref = ref 0
        let copyStart : int ref = ref 0
        lock (writeCache) (fun () ->
            let mutable (name, index, strm, cache) = writeCache.[key]
            if (cache.[index].Offset + cnt > cache.[index].Len) then
                // wait for finish
                let sw = SpinWait()
                while (!cache.[index].Finished < cache.[index].Offset) do
                    sw.SpinOnce()
                cache.[index].CanWriteToMem.Reset() |> ignore
                x.AddWriteReq(key, strm, cache.[index].Buf, cache.[index].InitOffset, cache.[index].Offset, Some(finishWrite cache.[index].CanWriteToMem))
                index <- index + 1
                cache.[index].Offset <- 0
                cache.[index].Finished := 0
            cache.[index].CanWriteToMem.Wait() |> ignore
            copyIndex := index
            copyStart := cache.[index].Offset
            cache.[index].Offset <- cache.[index].Offset + cnt
        )
        let (_,_,_,cache) = writeCache.[key]
        Buffer.BlockCopy(buf, offset, cache.[!copyIndex].Buf, cache.[!copyIndex].InitOffset + !copyStart, cnt)
        Interlocked.Add(cache.[!copyIndex].Finished, cnt) |> ignore

    member x.Read(key : 'K, buf : byte[])