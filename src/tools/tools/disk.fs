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

type internal StrmReq<'T> =
    struct
        val mutable index : 'T
        val mutable strm : Stream
        val mutable cb : Option<Stream*byte[]*int*int*int->unit>
        val mutable oper : 'T*Stream*byte[]*int*int*Option<Stream*byte[]*int*int*int->unit>->unit
        val mutable buf : byte[]
        val mutable offset : int
        val mutable cnt : int
        internal new (_index, _strm, _cb, _oper, _buf, _offset, _cnt) = {
            index = _index
            strm = _strm
            cb = _cb
            oper = _oper
            buf = _buf
            offset = _offset
            cnt = _cnt
        }
    end

type StrmIOReq<'TIn,'T> internal () =
    let q = ConcurrentDictionary<'T, ConcurrentQueue<StrmReq<'T>>*Semaphore*int ref*int>()
    let mutable getIndex : 'TIn->'T = (fun _ -> Unchecked.defaultof<'T>)
    let disposed = ref 0

    interface IDisposable with
        override x.Dispose() =
            if (Interlocked.CompareExchange(disposed, 1, 0) = 0) then
                for e in q do
                    let (a,s,b,c) = e.Value
                    s.Dispose()

    member x.Init(input : ('TIn*int)[], mapper : 'TIn->'T) =
        getIndex <- mapper
        for i = 0 to input.Length-1 do
            let (inStr, max) = input.[i]
            let index = getIndex(inStr)
            q.[index] <- (ConcurrentQueue<StrmReq<'T>>(), new Semaphore(max, max), ref 0, max)

    member private x.OperLoop(index : 'T) =
        let (q, s, outstanding, max) = q.[index]
        let mutable bDone = false
        while (q.Count > 0 && not bDone) do
            if (Interlocked.Increment(outstanding) <= max) then
                let (ret, elem) = q.TryDequeue()
                if (ret) then
                    s.WaitOne() |> ignore
                    elem.oper(index, elem.strm, elem.buf, elem.offset, elem.cnt, elem.cb)
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

    // Read operations
    member private x.ReadCb(ar : IAsyncResult) =
        let (index, strm, buf, offset, cnt, outstanding, s, furtherCb) = ar.AsyncState :?> ('T*Stream*byte[]*int*int*int ref*Semaphore*Option<Stream*byte[]*int*int*int->unit>)
        let amtRead = strm.EndRead(ar)
        Interlocked.Decrement(outstanding) |> ignore
        s.Release() |> ignore
        match furtherCb with
            | None -> ()
            | Some(cb) -> cb(strm, buf, offset, cnt, amtRead)
        ThreadPool.QueueUserWorkItem(WaitCallback(x.OperLoopExec), index) |> ignore 

    member private x.ReadInternal(index : 'T, strm : Stream, buf : byte[], offset : int, cnt : int, outstanding : int ref, s : Semaphore, furtherCb : Option<Stream*byte[]*int*int*int->unit>) =
        strm.BeginRead(buf, offset, cnt, AsyncCallback(x.ReadCb), (index, strm, buf, offset, cnt, outstanding, s, furtherCb)) |> ignore

    member x.TryRead(index : 'T, strm : Stream, buf : byte[], offset : int, cnt : int, furtherCb) =
        let (q, s, outstanding, max) = q.[index]
        if (Interlocked.Increment(outstanding) <= max) then
            s.WaitOne() |> ignore
            x.ReadInternal(index, strm, buf, offset, cnt, outstanding, s, furtherCb)
            true
        else
            Interlocked.Decrement(outstanding) |> ignore
            false

    member x.TryReadIn(input : 'TIn, strm : Stream, buf : byte[], offset : int, cnt : int, furtherCb) =
        x.TryRead(getIndex(input), strm, buf, offset, cnt, furtherCb)

    member private x.DoRead(index : 'T, strm : Stream, buf : byte[], offset : int, cnt : int, furtherCb) =
        let (q, s, outstanding, max) = q.[index]
        x.ReadInternal(index, strm, buf, offset, cnt, outstanding, s, furtherCb)

    member x.AddReadReq(input  : 'TIn, strm : Stream, buf : byte[], offset : int, cnt : int, furtherCb) =
        let index = getIndex(input)
        let (q, s, outstanding, max) = q.[index]
        let bDone = x.TryRead(index, strm, buf, offset, cnt, furtherCb)
        if not bDone then
            q.Enqueue(new StrmReq<'T>(index, strm, furtherCb, x.DoRead, buf, offset, cnt))

    member x.AddReadReq(input  : 'TIn, strm : Stream, buf : byte[], offset : int, cnt : int, event : EventWaitHandle, amtReadRef : int ref) =
        let index = getIndex(input)
        let (q, s, outstanding, max) = q.[index]
        let furtherCb = Some(x.FireEventCb (event, amtReadRef))
        event.Reset() |> ignore
        let bDone = x.TryRead(index, strm, buf, offset, cnt, furtherCb)
        if not bDone then
            q.Enqueue(new StrmReq<'T>(index, strm, furtherCb, x.DoRead, buf, offset, cnt))

    member x.SyncRead(input : 'TIn, strm : Stream, buf : byte[], offset : int, cnt : int) : int =
        let index = getIndex(input)
        let (q, s, outstanding, max) = q.[index]
        let amtReadRef = ref 0
        let furtherCb = Some(x.SetLen amtReadRef)
        let mutable bDone = x.TryRead(index, strm, buf, offset, cnt, furtherCb)
        while not bDone do
            s.WaitOne() |> ignore
            if (Interlocked.Increment(outstanding) <= max) then
                x.ReadInternal(index, strm, buf, offset, cnt, outstanding, s, furtherCb)
                bDone <- true
            else
                Interlocked.Decrement(outstanding) |> ignore
                s.Release() |> ignore
        !amtReadRef

    // Write operations
    member private x.WriteCb(ar : IAsyncResult) =
        let (index, strm, buf, offset, cnt, outstanding, s, furtherCb) = ar.AsyncState :?> ('T*Stream*byte[]*int*int*int ref*Semaphore*Option<Stream*byte[]*int*int*int->unit>)
        strm.EndWrite(ar)
        Interlocked.Decrement(outstanding) |> ignore
        s.Release() |> ignore
        match furtherCb with
            | None -> ()
            | Some(cb) -> cb(strm, buf, offset, cnt, 0)
        ThreadPool.QueueUserWorkItem(WaitCallback(x.OperLoopExec), index) |> ignore 

    member private x.WriteInternal(index : 'T, strm : Stream, buf : byte[], offset : int, cnt : int, outstanding : int ref, s : Semaphore, furtherCb : Option<Stream*byte[]*int*int*int->unit>) =
        strm.BeginWrite(buf, offset, cnt, AsyncCallback(x.WriteCb), (index, strm, buf, offset, cnt, outstanding, s, furtherCb)) |> ignore

    member x.TryWrite(index : 'T, strm : Stream, buf : byte[], offset : int, cnt : int, furtherCb) =
        let (q, s, outstanding, max) = q.[index]
        if (Interlocked.Increment(outstanding) <= max) then
            s.WaitOne() |> ignore
            x.WriteInternal(index, strm, buf, offset, cnt, outstanding, s, furtherCb)
            true
        else
            Interlocked.Decrement(outstanding) |> ignore
            false

    member x.TryWriteIn(input : 'TIn, strm : Stream, buf : byte[], offset : int, cnt : int, furtherCb) =
        x.TryWrite(getIndex(input), strm, buf, offset, cnt, furtherCb)

    member private x.DoWrite(index : 'T, strm : Stream, buf : byte[], offset : int, cnt : int, furtherCb) =
        let (q, s, outstanding, max) = q.[index]
        x.WriteInternal(index, strm, buf, offset, cnt, outstanding, s, furtherCb)

    member x.AddWriteReq(input  : 'TIn, strm : Stream, buf : byte[], offset : int, cnt : int, furtherCb) =
        let index = getIndex(input)
        let (q, s, outstanding, max) = q.[index]
        let bDone = x.TryWrite(index, strm, buf, offset, cnt, furtherCb)
        if not bDone then
            q.Enqueue(new StrmReq<'T>(index, strm, furtherCb, x.DoWrite, buf, offset, cnt))

    member x.AddWriteReq(input  : 'TIn, strm : Stream, buf : byte[], offset : int, cnt : int, event : EventWaitHandle, amtWriteRef : int ref) =
        let index = getIndex(input)
        let (q, s, outstanding, max) = q.[index]
        let furtherCb = Some(x.FireEventCb (event, amtWriteRef))
        event.Reset() |> ignore
        let bDone = x.TryWrite(index, strm, buf, offset, cnt, furtherCb)
        if not bDone then
            q.Enqueue(new StrmReq<'T>(index, strm, furtherCb, x.DoWrite, buf, offset, cnt))

    member x.SyncWrite(input : 'TIn, strm : Stream, buf : byte[], offset : int, cnt : int) =
        let index = getIndex(input)
        let (q, s, outstanding, max) = q.[index]
        let mutable bDone = x.TryWrite(index, strm, buf, offset, cnt, None)
        while not bDone do
            s.WaitOne() |> ignore
            if (Interlocked.Increment(outstanding) <= max) then
                x.WriteInternal(index, strm, buf, offset, cnt, outstanding, s, None)
                bDone <- true
            else
                Interlocked.Decrement(outstanding) |> ignore
                s.Release() |> ignore    
