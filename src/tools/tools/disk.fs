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

open System.IO
open System.Threading
open Prajna.Tools.Queue
open System.Collections.Concurrent

type internal DiskReq =
    struct
        val mutable file : File
        val mutable fileName : string
        val mutable cb : byte[]*int*int->unit
        val mutable buf : byte[]
        val mutable offset : int
        val mutable cnt : int
        internal new(_file : File, _cb : byte[]*int*int->unit, _buf : byte[], _offset : int, _cnt : int) = {
            file = _file;
            fileName = "";
            cb = _cb;
            buf = _buf;
            offset = _offset;
            cnt = _cnt;
        } 
        internal new(_fileName : string, _cb : byte[]*int*int->unit, _buf : byte[], _offset : int, _cnt : int) = {
            file = null;
            fileName = _fileName;
            cb = _cb;
            buf = null;
            offset = 0;
            cnt = 0;
        } 
    end

#if false
type DiskReadReq<'T> internal () =
    let q = ConcurrentDictionary<'T, ConcurrentQueue<DiskReq>*int ref*int>()
    let mutable getIndex : string -> 'T = (fun _ -> Unchecked.defaultof<'T>)
            
    member x.InitReader(disks : string[], maxOutstanding : int[], mapper : string->'T) =
        getIndex <- mapper
        for i = 0 to disks.Length-1 do
            let index = getIndex(disks.[i])
            q.[index] <- (ConcurrentQueue<DiskReq>(), ref 0, maxOutstanding.[i])

    member x.TryStart(q : ConcurrentQueue<DiskReq>, outstanding : int ref, maxOutstanding : int) =
        while (Interlocked.Increment(outstanding) <= maxOutstanding) do
            let ret = q.TryDequeue()

    member x.AddReadReq(fileName : string, cb : byte[]*int*int->unit, buf : byte[], offset : int, cnt : int) =
        let item = DiskReq(fileName, cb, buf, offset, cnt)
        let index = getIndex(fileName)
        let (readQ, outstanding, maxOutstanding) = q.[index]
        readQ.Enqueue(item)
        x.TryStart(readQ, oustanding, maxOutstanding)

type DiskReader() =
    static member GetDisk(name : string) =
        name.[0]

    static member DiskReader(disks : string[], maxOutstanding : int[]) : DiskReadReq<char> =
        let x = new DiskReadReq<char>()
        x.InitReader(disks, maxOutstanding, fun name -> name.[0])
        x
#endif