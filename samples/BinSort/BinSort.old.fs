﻿open System
open System.IO
open System.Diagnostics
open System.Runtime.InteropServices
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading

open Microsoft.FSharp.NativeInterop

open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.StringTools

open Prajna.Core
open Prajna.Api.FSharp
open Prajna.Api.FSharp

open NativeSort

let Usage = "
    Usage: Benchmark performance for distributed sort. \n\
    Command line arguments:\n\
    -cluster    Cluster to use\n\
    -dim        Dimension of each record\n\
    -records N  Number of records in total is N\n\
    -nfile N    Number of input partitions per node (# of files per node) is N\n\
    -nump N     Number of output partitions per node is N\n\
    -fnump N    Further binning per partition to improve sort performance when using bin sort\n\
    "

[<Serializable>]
// create an instance of this class at remote side
type Remote(dim : int, numNodes : int, numInPartPerNode : int, numOutPartPerNode : int, furtherPartition : int, recordsPerNode : int64) =
    static let readBlockRecords = 1024*1000
    static let maxWritePerDrive = 1
    static let numWriteRange = maxWritePerDrive + 1

    let totalRecords = int64(numNodes) * recordsPerNode
    let totalInPartitions = int64(numNodes) * int64(numInPartPerNode)
    let totalOutPartitions = int64(numNodes) * int64(numOutPartPerNode)
    let outSegmentsPerNode = int64(numOutPartPerNode) * int64(furtherPartition)
    let totalOutSegments = totalOutPartitions * int64(furtherPartition)
    let recordsPerInPartition = totalRecords / totalInPartitions
    let perFileInLen = recordsPerInPartition * int64(dim)
    let totalSizeInByte = perFileInLen * int64(totalInPartitions)
    let maxPartitionLen = totalSizeInByte * 3L / (totalOutPartitions * 2L) // 150% of avg size per partition
    let maxSubPartitionLen = totalSizeInByte * 3L / (totalOutSegments * 2L) // 150% of avg size per partition
    let mutable partBoundary : int[] = null
    let mutable segBoundary : int[] = null
    let mutable minSegVal : int[] = null

    // for file I/O
    let alignLen = (dim + 7)/8*8
    // make it multiple of numWriteRange
    //let cacheLenPerSegment = (1000000L/int64 numWriteRange*int64 numWriteRange) * int64(alignLen) / int64(dim)
    let cacheLenPerSegment = ((1000000L/4L)/int64 numWriteRange*int64 numWriteRange) * int64(alignLen) / int64(dim)

    // properties
    member x.Dim with get() = dim
    member x.InPartitions with get() = totalInPartitions
    member x.OutPartitions with get() = totalOutPartitions
    member x.TotalSizeInByte with get() = totalSizeInByte
    member x.NumNodes with get() = numNodes
    member x.FurtherPartition with get() = furtherPartition

    static member val Current : Remote = Unchecked.defaultof<Remote> with get, set

    member val internal AllocCache : ConcurrentQueue<_> = ConcurrentQueue<ArrAlign<byte>>() with get
    member val internal SubPartitionN = ConcurrentDictionary<uint32, (int64*int64 ref*ArrAlign<byte>)[]>() with get

    // only for disk
    member val CacheSegement = new ConcurrentDictionary<int64, int ref*int ref*ManualResetEvent*ManualResetEvent*BufIOEvent[]>() with get
    member val WriteRange = ConcurrentDictionary<int64, int*int64 ref*int64*ManualResetEvent>() with get
    member val internal SortFile = ConcurrentDictionary<uint32, ArrAlign<byte>>() with get
    member val DiskIO : StrmIOReq<string, char> = Unchecked.defaultof<StrmIOReq<string, char>> with get, set

    // init and start of remote instance ===================   
    member x.InitInstance(inMemory : bool) =
        let allocLen =
            if (inMemory) then
                ArrAlign<byte>.AlignSize(int maxSubPartitionLen, sizeof<uint64>)
            else
                ArrAlign<byte>.AlignSize(int cacheLenPerSegment, sizeof<uint64>)

        let rnd = Random()
        let buf = Array.zeroCreate<byte>(allocLen)
        rnd.NextBytes(buf)
        for i = 0 to int(outSegmentsPerNode)-1 do
            let arr = new ArrAlign<byte>(int maxSubPartitionLen, sizeof<uint64>)
            // write something to array
            Buffer.BlockCopy(buf, 0, arr.Arr, arr.Offset, buf.Length)
            // enqueue
            x.AllocCache.Enqueue(arr)

        // boundaries for repartitioning (Shuffling)
        partBoundary <- Array.init 65536 (fun i -> Math.Min(int(totalOutPartitions)-1,(int)(((int64 i)*(int64 totalOutPartitions))/65536L)))
        // boundaries for further binning at each node
        let maxPerPartition = (65536 + int(totalOutPartitions) - 1) / int(totalOutPartitions)
        let maxValPartition = (maxPerPartition <<< 8) + 256
        minSegVal <- Array.init (int(totalOutPartitions)) (fun i -> int((65536L * (int64 i) + (totalOutPartitions - 1L))/totalOutPartitions))
        segBoundary <- Array.init maxValPartition (fun i -> (int)(((int64 i)*(int64 furtherPartition))/(int64 maxValPartition)))

        // for Disk I/O
        x.DiskIO <- IOReq.NewDiskIO(x.DiskDir)

    member x.StopInstance() =
        (x :> IDisposable).Dispose()

    static member StartRemoteInstance(dim : int, numNodes : int, numInPartPerNode : int, numOutPartPerNode : int, furtherPartition : int, recordsPerNode : int64, inMemory : bool) () =
        Remote.Current <- new Remote(dim, numNodes, numInPartPerNode, numOutPartPerNode, furtherPartition, recordsPerNode)
        Remote.Current.InitInstance(inMemory)

    static member StopRemoteInstance() =
        Remote.Current.StopInstance()

    // transfer a local instance to remote
    static member TransferInstance (rmt) () =
        Remote.Current <- rmt

    member val IsDisposed = ref 0 with get
    member x.Dispose(bDisposing : bool) =
        if (Interlocked.CompareExchange(x.IsDisposed, 1, 0) = 0) then
            // unmanaged stuff always release
            if bDisposing then
                // managed stuff only release if disposing
                let mutable elem = Unchecked.defaultof<ArrAlign<byte>>
                while (x.AllocCache.Count > 0) do
                    let ret = x.AllocCache.TryDequeue(&elem)
                    if (ret) then
                        (elem :> IDisposable).Dispose()
                for s in x.SubPartitionN do
                    for e in s.Value do
                        let (segIndex, cnt, arr) = e
                        (arr :> IDisposable).Dispose()
                for s in x.SortFile do
                    (s.Value :> IDisposable).Dispose()
                //for r in x.WriteRange do
                //    let (a,b,d,e1) = r.Value
                //    e1.Dispose()
                for r in x.CacheSegement do
                    let (a,b,c,d,d1) = r.Value
                    c.Dispose()
                    for e in d1 do
                        (e :> IDisposable).Dispose()
                    //x.CacheSegement.[r.Key] <- (a,b,null)
                x.CacheSegement.Clear()
                (x.DiskIO :> IDisposable).Dispose()

    // no need for finalize as GCHandle is managed resource with finalize
    //override x.Finalize() =
    //    x.Dispose(false)
    interface IDisposable with
        override x.Dispose() =
            x.Dispose(true)
            GC.SuppressFinalize(x)

    // =======================================================================
    // Generic partition getters to perform sort

    member x.GetCachePtr(parti : int) : seq<uint32> =
        Seq.singleton(uint32 parti)

    static member GetCachePtr parti =
        Remote.Current.GetCachePtr(parti)

    // ================================================================
    // Make sure it is aligned by allocating 64-bit integer arrays
    member val SegmentIndex = ref -1 with get

    member private x.AddSubPartition (sizePerSegment : int64) (parti : uint32) =
        let createArrFn (i : int) =
            //let segIndex = Interlocked.Increment(x.SegmentIndex)
            let segIndex = int64(parti)*int64(furtherPartition) + int64(i)
            let (ret, arrHandle) = x.AllocCache.TryDequeue()
            if (ret) then
                (segIndex, ref 0L, arrHandle)
            else
                Logger.LogF(LogLevel.Error, fun _ -> "Preallocted cache is finished, creating new one")
                let arr = new ArrAlign<byte>(int sizePerSegment, sizeof<uint64>)
                (segIndex, ref 0L, arr)
        Array.init<_> furtherPartition createArrFn

    member x.FurtherPartitionCacheInRAMAndDisposeN(ms : StreamBase<byte>) =
        ms.Seek(0L, SeekOrigin.Begin) |> ignore
        let parti = ms.ReadUInt32()
        let partArr = x.SubPartitionN.GetOrAdd(parti, x.AddSubPartition maxSubPartitionLen)
        let alignLen = (dim + 7)/8*8
        let vec = Array.zeroCreate<byte>(alignLen)
        while (ms.Read(vec, 0, dim) = dim) do
            let index0 = ((int vec.[0]) <<< 8) ||| (int vec.[1])
            let index1 = ((index0 - minSegVal.[int parti]) <<< 8) ||| (int vec.[2])
            let (segIndex, cnt, arr) = partArr.[segBoundary.[index1]]
            let start = Interlocked.Add(cnt, int64 alignLen) - (int64 alignLen)
            if (start + (int64 alignLen) > maxSubPartitionLen) then
                Interlocked.Add(cnt, int64 -alignLen) |> ignore
               // throw away, not enough space in cache
                Logger.LogF(LogLevel.Error, fun _ -> "Error: Max Length exceeded")
            else
                //Marshal.Copy(vec, 0, IntPtr.Add(arr.Ptr, int start), alignLen)
                Buffer.BlockCopy(vec, 0, arr.Arr, arr.Offset + int start, alignLen)
        (ms :> IDisposable).Dispose()

    static member FurtherPartitionCacheInRAMAndDisposeN ms =
        Remote.Current.FurtherPartitionCacheInRAMAndDisposeN(ms)

    member internal x.GetCacheMemSubPartN(parti : int) : seq<_> =
        if (x.SubPartitionN.ContainsKey(uint32 parti)) then 
            Seq.ofArray(x.SubPartitionN.[uint32 parti])
        else
            Seq.empty

    static member internal GetCacheMemSubPartN parti =
        Remote.Current.GetCacheMemSubPartN(parti)

    member x.ClearCacheMemSubPartN(parti : uint32) =
        if (x.SubPartitionN.ContainsKey(parti)) then 
            if (Utils.IsNotNull x.SubPartitionN.[parti]) then
                for elem in x.SubPartitionN.[parti] do
                    let (segIndex, cnt, arr) = elem
                    cnt := 0L

    static member ClearCacheMemSubPartN parti =
        Remote.Current.ClearCacheMemSubPartN(parti)        

    // =======================================================================

    member private x.AddSubPartitionByte (sizePerSegment : int64) (parti : uint32) =
        let createArrFn (i : int) =
            let segIndex = int64(parti)*int64(furtherPartition) + int64(i)
            let (ret, arr) = x.AllocCache.TryDequeue()
            let (segmentIndex, cnt, arr) =
                if (ret) then
                    (segIndex, ref 0L, arr)
                else
                    Logger.LogF(LogLevel.Error, fun _ -> "Preallocted cache is finished, creating new one")
                    let arr = new ArrAlign<byte>(int sizePerSegment, sizeof<uint64>)
                    (segIndex, ref 0L, arr)
            (segmentIndex, cnt, arr)
        x.SortFile.[parti] <- new ArrAlign<byte>(int maxSubPartitionLen, sizeof<uint64>)
        x.ST.[parti] <- SingleThreadExec()
        Array.init<_> furtherPartition createArrFn

    member x.FlushSegment(parti : int, segIndex : int64) =
        let dirIndex = int(segIndex % int64 x.PartDataDir.Length)
        let segInPart = int(segIndex - (int64)parti*(int64)furtherPartition)
        //Logger.LogF(LogLevel.Warning, fun _ -> sprintf "Obtain segment %d %d %d %d" parti segIndex dirIndex segInPart)
        let (rangeIndex, copyStart, boundary, canWrite) = x.WriteRange.[segIndex]
        let (segIndex, cnt, arr) = x.SubPartitionN.[uint32 parti].[segInPart]
        canWrite.WaitOne() |> ignore
        let fh = File.Open(Path.Combine(x.PartDataDir.[dirIndex], sprintf "%d.bin" segIndex), FileMode.Append)
        let startRange = int64 rangeIndex*cacheLenPerSegment/(int64 numWriteRange)
        fh.Write(arr.Arr, arr.Offset + int startRange, int(!copyStart-startRange))
        fh.Close()

    member x.DoneWrite(ar : IAsyncResult) =
        let (fh, dirIndex, canWrite) = ar.AsyncState :?> (FileStream * int * ManualResetEvent)
        fh.EndWrite(ar)
        fh.Close()
        canWrite.Set() |> ignore
//        x.WriteSemaphore.[dirIndex].Release() |> ignore
 //       Interlocked.Increment(x.SemaphoreCount.[dirIndex]) |> ignore

//    member x.AddElement (segIndex : int64) (arr : ArrAlign<byte>) (dirIndex : int) (vec : byte[]) (copied : int64 ref) (finish : ManualResetEventSlim) () =
//        let (rangeIndex, copyStart, boundary, canWrite) = x.WriteRange.[segIndex]
//        let endRange = Interlocked.Add(copyStart, int64 alignLen)
//        Buffer.BlockCopy(vec, 0, arr.Arr, arr.Offset + int endRange - alignLen, alignLen) 
//        Interlocked.Add(copied, int64 alignLen) |> ignore
//        finish.Set() |> ignore
//        if (endRange = boundary) then
//            canWrite.WaitOne() |> ignore
//            x.WriteSemaphore.[dirIndex].WaitOne() |> ignore
//            let fh = File.Open(Path.Combine(x.PartDataDir.[dirIndex], sprintf "%d.bin" segIndex), FileMode.Append)
//            let startRange = int64 rangeIndex*cacheLenPerSegment/(int64 numWriteRange)
//            Interlocked.Decrement(x.SemaphoreCount.[dirIndex]) |> ignore // outstanding write pending
//            canWrite.Reset() |> ignore
//            fh.BeginWrite(arr.Arr, arr.Offset + int startRange, int(boundary-startRange), AsyncCallback(x.DoneWrite), (fh, dirIndex, canWrite)) |> ignore
//            let mutable nextRangeIndex = rangeIndex + 1
//            if (nextRangeIndex = numWriteRange) then
//                nextRangeIndex <- 0
//                copyStart := 0L
//            x.WriteRange.[segIndex] <- (nextRangeIndex, copyStart, cacheLenPerSegment*(int64 nextRangeIndex+1L)/(int64 numWriteRange), canWrite)

    member x.FurtherPartitionCacheInRAMAndWrite(ms : StreamBase<byte>) =
        ms.Seek(0L, SeekOrigin.Begin) |> ignore
        let parti = ms.ReadUInt32()
        let partArr = x.SubPartitionN.GetOrAdd(parti, x.AddSubPartitionByte cacheLenPerSegment)
        let vec = Array.zeroCreate<byte>(alignLen)
        let toCopy = ref 0L
        let copied = ref 0L
        while (ms.Read(vec, 0, dim) = dim) do
            let index0 = ((int vec.[0]) <<< 8) ||| (int vec.[1])
            let index1 = ((index0 - minSegVal.[int parti]) <<< 8) ||| (int vec.[2])
            let (segIndex, cnt, arr) = partArr.[segBoundary.[index1]]
            let dirIndex = int(segIndex % int64 x.PartDataDir.Length)
            x.CacheSegement.GetOrAdd(segIndex, fun _ ->
                let fh = File.Open(Path.Combine(x.PartDataDir.[dirIndex], sprintf "%d.bin" segIndex), FileMode.Create)
                fh.Close()
                let cacheArr = Array.zeroCreate<BufIOEvent>(numWriteRange)
                for i = 0 to numWriteRange-1 do
                    let start = int(int64 i*cacheLenPerSegment / int64 numWriteRange)
                    let next = int(int64(i+1)*cacheLenPerSegment / int64 numWriteRange)
                    let buf = new BufIOEvent(arr.Arr, arr.Offset + start, next-start)
                    cacheArr.[i] <- buf
                (ref 0, ref 0, new ManualResetEvent(true), new ManualResetEvent(false), cacheArr)
            ) |> ignore
//            lock (x.CacheSegement) (fun () ->
//                
//            )

//            Interlocked.Add(cnt, int64 alignLen) |> ignore
//            let mutable copyEnd = Interlocked.Add(rangeCopy, alignLen)
//            let mutable copyStart = copyEnd - alignLen
//            while (copyEnd >= cache.[!rangeIndex].cnt) do
//                if (copyStart < cache.[!rangeIndex].cnt) then
//                    // this thread moves the cache segment forward
//                    Interlocked.Increment(rangeIndex) |> ignore
//                    rangeCopy := ref 0
//                    eventWrite.WaitOne() |> ignore
//                    eventWrite.Reset() |> ignore
//                    eventCopy.Set() |> ignore
//                eventCopy.WaitOne() |> ignore
//                eventCopy.Reset() |> ignore
//                copyEnd <- Interlocked.Add(rangeCopy, alignLen)
//                copyStart <- copyEnd - alignLen


            Interlocked.Add(toCopy, int64 alignLen) |> ignore


            //else if (copyEnd >= cache.[!rangeIndex].cnt) then
            

//            finish.Reset() |> ignore
//            x.ST.[parti].ExecQ(x.AddElement segIndex arr dirIndex vec copied finish)
//            finish.Wait() |> ignore
//        (ms :> IDisposable).Dispose()

    static member FurtherPartitionCacheInRAMAndWrite ms =
        Remote.Current.FurtherPartitionCacheInRAMAndWrite(ms)

    member x.GetCacheMemSubPartByte(parti : int) : seq<_> =
        if (x.SubPartitionN.ContainsKey(uint32 parti)) then
            seq {
                for elem in x.SubPartitionN.[uint32 parti] do
                    let (segIndex, cnt, arr) = elem
                    if (x.WriteRange.ContainsKey(segIndex)) then
                        yield (parti, segIndex, cnt)
            }
        else
            Seq.empty

    static member GetCacheMemSubPartByte parti =
        Remote.Current.GetCacheMemSubPartByte(parti)

    member x.ClearCacheMemSubPartByte(parti : uint32) =
        if (x.SubPartitionN.ContainsKey(parti)) then 
            if (Utils.IsNotNull x.SubPartitionN.[parti]) then
                for elem in x.SubPartitionN.[parti] do
                    let (segIndex, cnt, arr) = elem
                    cnt := 0L
                    x.WriteRange.TryRemove(segIndex) |> ignore

    static member ClearCacheMemSubPartByte parti =
        Remote.Current.ClearCacheMemSubPartByte(parti)  

    // ================================================================================
    member val private ReadCnt = ref -1 with get
    member val private DiskDir : (char*int)[] = [|('c', maxWritePerDrive); ('d', maxWritePerDrive); ('e', maxWritePerDrive); ('f', maxWritePerDrive)|] with get, set
    member val private RawDataDir : string[] = [|@"c:\sort\raw"; @"d:\sort\raw"; @"e:\sort\raw"; @"f:\sort\raw"|] with get, set
    member val PartDataDir : string[] = [|@"c:\sort\part"; @"d:\sort\part"; @"e:\sort\part"; @"f:\sort\part"|] with get, set
    member val SortDataDir : string[] = [|@"c:\sort\sort"; @"d:\sort\sort"; @"e:\sort\sort"; @"f:\sort\sort"|] with get, set
    member val private ST : ConcurrentDictionary<uint32, SingleThreadExec> = ConcurrentDictionary<uint32, SingleThreadExec>() with get

    member x.ReadFilesToMemStream dim parti =
        let readBlockSize = readBlockRecords * dim
        let tbuf = Array.zeroCreate<byte> readBlockSize          
        let counter = ref 0
        let totalReadLen = ref 0L
        let ret =
            seq {
                let instCnt = Interlocked.Increment(x.ReadCnt)
                let dirIndex = instCnt % x.RawDataDir.Length // pick up directory in round-robin
                let fh = File.Open(Path.Combine(x.RawDataDir.[dirIndex], "raw.bin"), FileMode.Open, FileAccess.Read, FileShare.Read)
                while !totalReadLen < perFileInLen do 
                    let toRead = int32 (Math.Min(int64 readBlockSize, perFileInLen - !totalReadLen))
                    if toRead > 0 then
                        let memBuf = new MemoryStreamB()
                        memBuf.WriteFromStreamAlign(fh, int64 toRead, dim)
                        totalReadLen := !totalReadLen + (int64) toRead
                        fh.Seek(0L, SeekOrigin.Begin) |> ignore
                        counter := !counter + 1
                        yield memBuf
                Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "All data from file has been read"))  
                fh.Close()
            }
        ret

    static member ReadFilesToMemStreamS dim parti =
        Remote.Current.ReadFilesToMemStream dim parti

    member x.ReadFilesToMemStreamF dim parti =
        let readBlockSize = readBlockRecords * dim
        let tbuf = Array.zeroCreate<byte> readBlockSize
        let rand = new Random()
        rand.NextBytes(tbuf)            
        let counter = ref 0
        let totalReadLen = ref 0L
        let ret =
            seq {
                let instCnt = Interlocked.Increment(x.ReadCnt)
                while !totalReadLen < perFileInLen do 
                    let toRead = int32 (Math.Min(int64 readBlockSize, perFileInLen - !totalReadLen))
                    if toRead > 0 then
                        let memBuf = new MemoryStreamB()
                        memBuf.WriteArrAlign(tbuf, 0, toRead, dim)
                        totalReadLen := !totalReadLen + (int64) toRead
                        counter := !counter + 1
                        //if (!counter % 100 = 0) then
                        //    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Read %d bytes from file" !totalReadLen) )
                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "%d Read %d bytes from file total %d - rem %d" instCnt toRead !totalReadLen (perFileInLen-(!totalReadLen))) )
                        yield memBuf
                Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "All data from file has been read"))  
            }
        ret

    static member ReadFilesToMemStreamFS dim parti =
        Remote.Current.ReadFilesToMemStreamF dim parti

    member internal x.RepartitionMemStream (buffer:MemoryStreamB) = 
        if buffer.Length > 0L then
            let retseq = seq {
                let partstream = Array.init<StreamBase<byte>> (int(totalOutPartitions)) (fun i -> null)
                let t1 = DateTime.UtcNow
                let bHasBuf = ref true
                let sr = new StreamReader<byte>(buffer, 0L)

                while !bHasBuf do
                    let (buf, pos, len) = sr.GetMoreBuffer()
                    if (Utils.IsNotNull buf) then
                        let idx = ref pos
                        while (!idx + dim <= len) do
                            let index = (((int) buf.[!idx]) <<< 8) + ((int) buf.[!idx + 1])
                            let parti = partBoundary.[index]

                            if Utils.IsNull partstream.[parti] then
                                let ms = new MemoryStreamB()
                                ms.WriteUInt32(uint32 parti)
                                partstream.[parti] <- ms :> StreamBase<byte>
                            partstream.[parti].Write(buf, !idx, dim)
                            idx := !idx + dim
                    else
                        bHasBuf := false

                (buffer :> IDisposable).Dispose()
                let t2 = DateTime.UtcNow

                for i = 0 to int(totalOutPartitions) - 1 do
                    if Utils.IsNotNull partstream.[i] then
                        if (partstream).[i].Length > 0L then
                            (partstream).[i].Seek(0L, SeekOrigin.Begin) |> ignore
                            yield (partstream).[i]
                        else 
                            ((partstream).[i] :> IDisposable).Dispose()
                        
                Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "repartition: %d records, takes %f s" (buffer.Length / 100L) ((t2-t1).TotalSeconds) ) )          
            }
            retseq
        else
            Seq.empty

    static member RepartitionMemStream (buffer) =
        Remote.Current.RepartitionMemStream (buffer)

    member x.DoSortFile (parti : int, segIndex : int64, cnt : int64 ref) =
        x.FlushSegment(parti, segIndex)
        let dirIndex = int(segIndex % int64 Remote.Current.PartDataDir.Length)
        //let parti = segIndex / int64 Remote.Current.FurtherPartition
        let fileName = Path.Combine(Remote.Current.PartDataDir.[dirIndex], sprintf "%d.bin" segIndex)
        let sortFileName = Path.Combine(Remote.Current.SortDataDir.[dirIndex], sprintf "%d.bin" parti)
        let vec = x.SortFile.[uint32 parti]
        //let err = NativeSort.Sort.SortFile(vec.Arr, vec.Offset, (alignLen>>>3), fileName, sortFileName)
        let err = NativeSort.Sort.SortFile(vec.Ptr, vec.Size, (alignLen>>>3), fileName, sortFileName)
        if (err <> 0) then
            failwith "Sort returns error"
        cnt

    static member DoSortFile (parti : int, segIndex : int64, cnt : int64 ref) =
        Remote.Current.DoSortFile(parti, segIndex, cnt)

// ====================================================

module Interop =
    let inline AlignSort(buffer : byte[], align : int, num : int) =
        let bufferHandle = GCHandle.Alloc(buffer, GCHandleType.Pinned)
        try
            NativeSort.Sort.AlignSort64(bufferHandle.AddrOfPinnedObject(), (align+7)/8*8, num)
        finally
            bufferHandle.Free()

let repartitionFn (ms : StreamBase<byte>) =
    ms.Seek(0L, SeekOrigin.Begin) |> ignore
    let index = ms.ReadUInt32()
    int index

let internal doSortN (alignLen : int) (segIndex : int64, cnt : int64 ref, buf : ArrAlign<byte>) : int64 ref*IntPtr =
    let num = int(!cnt/(int64 alignLen))
    NativeSort.Sort.AlignSort64(buf.Ptr, alignLen>>>3, num)
    (cnt, buf.Ptr)

let aggrFn (cnt1 : int64) (cnt2 : int64) =
    cnt1 + cnt2

let cntLenByteArr (alignLen : int) (cnt : int64) (newCnt : int64 ref) =
    cnt + !newCnt/(int64 alignLen)

let cntLenByteArrNFn (dim : int) (alignLen : int) (cnt : int64) (cntPlusArr : int64 ref*IntPtr) =
    let (cntArrR, arr) = cntPlusArr
    cnt + !cntArrR/(int64 alignLen)

// In memory sort
let inMemSort(sort : Remote, remote : DSet<_>) =
    let startDSet = DSet<_>( Name = "SortGen", SerializationLimit = 1) 
    startDSet.NumParallelExecution <- 16 

    let watch = Stopwatch.StartNew()

    // Read Data into DSet
    let dset1 = startDSet |> DSet.sourceI (int sort.InPartitions) (Remote.ReadFilesToMemStreamFS sort.Dim)
    dset1.NumParallelExecution <- 16 
    dset1.SerializationLimit <- 1

    // Map to find new partition index
    let dset3 = dset1 |> DSet.map Remote.RepartitionMemStream
    dset3.NumParallelExecution <- 16 
    dset3.SerializationLimit <- 1

    // Collect all memstream into new DSet                
    let dset4 = dset3 |> DSet.collect Operators.id
    dset4.NumParallelExecution <- 16 
                
    // Repartition using index
    let param = new DParam()
    param.NumPartitions <- int sort.OutPartitions
    let dset5 = dset4 |> DSet.repartitionP param repartitionFn

    // Iterate through and cache in RAM
    dset5 |> DSet.iter Remote.FurtherPartitionCacheInRAMAndDisposeN
    let cnt = sort.TotalSizeInByte / (int64 sort.Dim)
    Logger.LogF(LogLevel.Info, fun _ -> sprintf "Creating remap + repartition + cacheInRam stream takes: %f seconds num: %d rate per node: %f Gbps" watch.Elapsed.TotalSeconds cnt ((double cnt)*(double sort.Dim)*8.0/1.0e9/(double sort.NumNodes)/watch.Elapsed.TotalSeconds))

    // now sort in RAM
    let startRepart = DSet<_>(Name = "SortVec", SerializationLimit = 1)
    startRepart.NumParallelExecution <- 16

    // count # of sorted vectors to verify result
    let dset6 = startRepart |> DSet.sourceI dset5.NumPartitions Remote.GetCacheMemSubPartN
    let alignLen = (sort.Dim + 7)/8*8
    let dset7 = dset6 |> DSet.map (doSortN alignLen)
    let cnt = dset7 |> DSet.fold (cntLenByteArrNFn sort.Dim alignLen) aggrFn 0L
    Logger.LogF(LogLevel.Info, fun _ -> sprintf "Creating remap + repartition stream + cache + sort takes: %f seconds num: %d rate per node: %f Gbps" watch.Elapsed.TotalSeconds cnt ((double cnt)*(double sort.Dim)*8.0/1.0e9/(double sort.NumNodes)/watch.Elapsed.TotalSeconds))

    // now clear the memory cache
    let dset8 = DSet<_>(Name = "ClearCache", SerializationLimit = 1) |> DSet.sourceI dset5.NumPartitions Remote.GetCachePtr
    dset8 |> DSet.iter Remote.ClearCacheMemSubPartN

// Full sort
let fullSort(sort : Remote, remote : DSet<_>) =
    let startDSet = DSet<_>( Name = "SortGen", SerializationLimit = 1) 
    startDSet.NumParallelExecution <- 16 

    let watch = Stopwatch.StartNew()

    // Read Data into DSet
    let dset1 = startDSet |> DSet.sourceI (int sort.InPartitions) (Remote.ReadFilesToMemStreamS sort.Dim)
    dset1.NumParallelExecution <- 16 
    dset1.SerializationLimit <- 1

    // Map to find new partition index
    let dset3 = dset1 |> DSet.map Remote.RepartitionMemStream
    dset3.NumParallelExecution <- 16 
    dset3.SerializationLimit <- 1

    // Collect all memstream into new DSet                
    let dset4 = dset3 |> DSet.collect Operators.id
    dset4.NumParallelExecution <- 16 
                
    // Repartition using index
    let param = new DParam()
    param.NumPartitions <- int sort.OutPartitions
    let dset5 = dset4 |> DSet.repartitionP param repartitionFn

    // Iterate through and cache in RAM
    dset5 |> DSet.iter Remote.FurtherPartitionCacheInRAMAndWrite
    let cnt = sort.TotalSizeInByte / (int64 sort.Dim)
    Logger.LogF(LogLevel.Info, fun _ -> sprintf "Creating remap + repartition + cacheInRam stream takes: %f seconds num: %d rate per node: %f Gbps" watch.Elapsed.TotalSeconds cnt ((double cnt)*(double sort.Dim)*8.0/1.0e9/(double sort.NumNodes)/watch.Elapsed.TotalSeconds))

    // now sort
    let startRepart = DSet<_>(Name = "SortVec", SerializationLimit = 1)
    startRepart.NumParallelExecution <- 16

    // count # of sorted vectors to verify result
    let dset6 = startRepart |> DSet.sourceI dset5.NumPartitions Remote.GetCacheMemSubPartByte
    let alignLen = (sort.Dim + 7)/8*8
    let dset7 = dset6 |> DSet.map Remote.DoSortFile
    let cnt = dset7 |> DSet.fold (cntLenByteArr alignLen) aggrFn 0L
    Logger.LogF(LogLevel.Info, fun _ -> sprintf "Creating remap + repartition stream + cache + sort takes: %f seconds num: %d rate per node: %f Gbps" watch.Elapsed.TotalSeconds cnt ((double cnt)*(double sort.Dim)*8.0/1.0e9/(double sort.NumNodes)/watch.Elapsed.TotalSeconds))

    // now clear the memory cache
    let dset8 = DSet<_>(Name = "ClearCache", SerializationLimit = 1) |> DSet.sourceI dset5.NumPartitions Remote.GetCachePtr
    dset8 |> DSet.iter Remote.ClearCacheMemSubPartByte

[<EntryPoint>]
let main orgargs = 
    let args = Array.copy orgargs
    let parse = ArgumentParser(args)
    let PrajnaClusterFile = parse.ParseString( "-cluster", "c:\onenet\cluster\onenet21-25.inf" )
    let nDim = parse.ParseInt( "-dim", 100 )
    let recordsPerNode = parse.ParseInt64( "-records", 250000000L ) // records per node
    //let recordsPerNode = parse.ParseInt64("-records", 100000000L)
    let numInPartPerNode = parse.ParseInt( "-nfile", 8 ) // number of partitions (input)
    let numOutPartPerNode = parse.ParseInt( "-nump", 8 ) // number of partitions (output)
    let furtherPartition = parse.ParseInt("-fnump", 2500) // further binning for improved sort performance
    let inMemory = parse.ParseBoolean("-inmem", false)

    let bAllParsed = parse.AllParsed Usage
    let mutable bExecute = false

    if (bAllParsed) then
        // start cluster
        Cluster.Start( null, PrajnaClusterFile )

        // add other dependencies
        let curJob = JobDependencies.setCurrentJob "SortGen"
        JobDependencies.Current.Add([|"nativesort.dll"|])
        let proc = Process.GetCurrentProcess()
        let dir = Path.GetDirectoryName(proc.MainModule.FileName)
        curJob.AddDataDirectory( dir ) |> ignore

        let cluster = Cluster.GetCurrent()
        let numNodes = cluster.NumNodes

        // create local copy
        let sort = new Remote(nDim, numNodes, numInPartPerNode, numOutPartPerNode, furtherPartition, recordsPerNode)

        // do sort
        let remoteExec = DSet<_>(Name = "Remote")
        let watch = Stopwatch.StartNew()

        remoteExec.Execute(fun () -> ())
        Logger.LogF(LogLevel.Info, fun _ -> sprintf "Init takes %f seconds" watch.Elapsed.TotalSeconds)

        //remoteExec.Execute(RemoteFunc.TransferInstance(sort)) // transfer local to remote via serialization
        remoteExec.Execute(Remote.StartRemoteInstance(nDim, numNodes, numInPartPerNode, numOutPartPerNode, furtherPartition, recordsPerNode, inMemory))
        Logger.LogF(LogLevel.Info, fun _ -> sprintf "Init plus alloc takes %f seconds" watch.Elapsed.TotalSeconds)

        if (inMemory) then
            inMemSort(sort, remoteExec)
            // repeat twice
            inMemSort(sort, remoteExec)
        else
            fullSort(sort, remoteExec)
            fullSort(sort, remoteExec)

        // stop remote instances
        remoteExec.Execute(Remote.StopRemoteInstance)

        Cluster.Stop()

        bExecute <- true

    // Make sure we don't print the usage information twice. 
    if not bExecute && bAllParsed then 
        parse.PrintUsage Usage

    0