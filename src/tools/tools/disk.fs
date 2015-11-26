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
    struct
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
    end

type StrmIOReq<'TIn,'T> internal () =
    let q = ConcurrentDictionary<'T, ConcurrentQueue<StrmReq<'TIn,'T>>*Semaphore*int ref*int>()
    let strmsRead = ConcurrentDictionary<'TIn, Stream>()
    let strmsWrite = ConcurrentDictionary<'TIn, Stream>()
    let mutable getIndex : 'TIn->'T = (fun _ -> Unchecked.defaultof<'T>)
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

    member x.Init(input : ('TIn*int)[], mapper : 'TIn->'T) =
        getIndex <- mapper
        for i = 0 to input.Length-1 do
            let (inStr, max) = input.[i]
            let index = getIndex(inStr)
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
        x.TryRead(input, getIndex(input), strm, buf, offset, cnt, furtherCb)

    member private x.DoRead(input : 'TIn, index : 'T, strm : Stream, buf : byte[], offset : int, cnt : int, furtherCb) =
        let (q, s, outstanding, max) = q.[index]
        x.ReadInternal(input, index, strm, buf, offset, cnt, outstanding, s, furtherCb)

    member x.AddReadReq(input  : 'TIn, strm : Stream, buf : byte[], offset : int, cnt : int, furtherCb) =
        let index = getIndex(input)
        let (q, s, outstanding, max) = q.[index]
        let bDone = x.TryRead(input, index, strm, buf, offset, cnt, furtherCb)
        if not bDone then
            q.Enqueue(new StrmReq<'TIn,'T>(input, index, strm, furtherCb, x.DoRead, buf, offset, cnt))

    member x.AddReadReq(input  : 'TIn, strm : Stream, buf : byte[], offset : int, cnt : int, event : EventWaitHandle, amtReadRef : int ref) =
        let index = getIndex(input)
        let (q, s, outstanding, max) = q.[index]
        let furtherCb = Some(x.FireEventCb (event, amtReadRef))
        event.Reset() |> ignore
        let bDone = x.TryRead(input, index, strm, buf, offset, cnt, furtherCb)
        if not bDone then
            q.Enqueue(new StrmReq<'TIn,'T>(input, index, strm, furtherCb, x.DoRead, buf, offset, cnt))

    member x.AddReadReq(input : 'TIn, strm : Stream, bufIO : BufIOEvent) =
        x.AddReadReq(input, strm, bufIO.buf, bufIO.offset, bufIO.cnt, bufIO.event, bufIO.amt)

    member x.SyncRead(input : 'TIn, strm : Stream, buf : byte[], offset : int, cnt : int) : int =
        let index = getIndex(input)
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
        x.TryWrite(input, getIndex(input), strm, buf, offset, cnt, furtherCb)

    member private x.DoWrite(input : 'TIn, index : 'T, strm : Stream, buf : byte[], offset : int, cnt : int, furtherCb) =
        let (q, s, outstanding, max) = q.[index]
        x.WriteInternal(input, index, strm, buf, offset, cnt, outstanding, s, furtherCb)

    member x.AddWriteReq(input  : 'TIn, strm : Stream, buf : byte[], offset : int, cnt : int, furtherCb) =
        let index = getIndex(input)
        let (q, s, outstanding, max) = q.[index]
        let bDone = x.TryWrite(input, index, strm, buf, offset, cnt, furtherCb)
        if not bDone then
            q.Enqueue(new StrmReq<'TIn,'T>(input, index, strm, furtherCb, x.DoWrite, buf, offset, cnt))

    member x.AddWriteReq(input  : 'TIn, strm : Stream, buf : byte[], offset : int, cnt : int, event : EventWaitHandle) =
        let index = getIndex(input)
        let (q, s, outstanding, max) = q.[index]
        let furtherCb = Some(x.FireEventCb (event, ref 0))
        event.Reset() |> ignore
        let bDone = x.TryWrite(input, index, strm, buf, offset, cnt, furtherCb)
        if not bDone then
            q.Enqueue(new StrmReq<'TIn,'T>(input, index, strm, furtherCb, x.DoWrite, buf, offset, cnt))

    member x.SyncWrite(input : 'TIn, strm : Stream, buf : byte[], offset : int, cnt : int) =
        let index = getIndex(input)
        let (q, s, outstanding, max) = q.[index]
        s.WaitOne() |> ignore
        Interlocked.Increment(outstanding) |> ignore
        x.WriteInternal(input, index, strm, buf, offset, cnt, outstanding, s, None)

[<Extension>]
type IOReq() =
    static member DiskMap(disk : string) =
        disk.[0]

    static member NewDiskIO(writeLoc : (string*int)[]) =
        let x = new StrmIOReq<string, char>()
        x.Init(writeLoc, IOReq.DiskMap)
        x

    [<Extension>]
    static member AddReadReq(ioReq : StrmIOReq<'TIn,'T>, input : 'TIn, buf : byte[], offset : int, cnt : int, event : EventWaitHandle, amt : int ref) =
        ioReq.AddReadReq(input, null, buf, offset, cnt, event, amt)

    [<Extension>]
    static member AddReadReq(ioReq : StrmIOReq<'TIn,'T>, input : 'TIn, buf : BufIOEvent) =
        ioReq.AddReadReq(input, null, buf)

    [<Extension>]
    static member SyncWrite(ioReq : StrmIOReq<'TIn,'T>, input : 'TIn, buf : byte[], offset : int, cnt : int) =
        ioReq.SyncWrite(input, null, buf, offset, cnt)

//module A =
//    let v() =
//        let ioReq = IOReq.NewDiskIO([||])
//        ioReq.AddReadReq("", Array.zeroCreate<byte>(5), 0, 5, null, ref 0)
