open System
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

    let totalRecords = int64(numNodes) * recordsPerNode
    let totalInPartitions = int64(numNodes) * int64(numInPartPerNode)
    let totalOutPartitions = int64(numNodes) * int64(numOutPartPerNode)
    let outSegementsPerNode = int64(numOutPartPerNode) * int64(furtherPartition)
    let totalOutSegments = totalOutPartitions * int64(furtherPartition)
    let recordsPerInPartition = totalRecords / totalInPartitions
    let perFileInLen = recordsPerInPartition * int64(dim)
    let totalSizeInByte = perFileInLen * int64(numInPartPerNode)
    let maxPartitionLen = totalSizeInByte * 3L / (totalOutPartitions * 2L) // 150% of avg size per partition
    let maxSubPartitionLen = totalSizeInByte * 3L / (totalOutSegments * 2L) // 150% of avg size per partition
    let mutable partBoundary : int[] = null
    let mutable segBoundary : int[] = null
    let mutable minSegVal : int[] = null

    // properties
    member x.Dim with get() = dim
    member x.InPartitions with get() = totalInPartitions
    member x.OutPartitions with get() = totalOutPartitions
    member x.TotalSizeInByte with get() = totalSizeInByte
    member x.NumNodes with get() = numNodes

    static member val Current : Remote = Unchecked.defaultof<Remote> with get, set

    member val Partition = ConcurrentDictionary<uint32, int64 ref*byte[]>() with get
    member val SubPartition = ConcurrentDictionary<uint32, (int64 ref*byte[])[]>() with get
    member val SubPartitionN = ConcurrentDictionary<uint32, (int64 ref*GCHandle*uint64[])[]>() with get

    // init and start of remote instance ===================
    member val AllocCache : ConcurrentQueue<_> = ConcurrentQueue<GCHandle*uint64[]>() with get
   
    member x.InitInstance() =
        let allocLen = (int maxSubPartitionLen + sizeof<uint64> - 1) / sizeof<uint64>
        let rnd = Random()
        let buf = Array.zeroCreate<byte>(allocLen*sizeof<uint64>)
        rnd.NextBytes(buf)
        for i = 0 to int(outSegementsPerNode)-1 do
            let arr = Array.zeroCreate<uint64>(allocLen)
            let handle = GCHandle.Alloc(arr, GCHandleType.Pinned)
            // write something to array
            Buffer.BlockCopy(buf, 0, arr, 0, buf.Length)
            // enqueue
            x.AllocCache.Enqueue((handle, arr))

        // boundaries for repartitioning (Shuffling)
        partBoundary <- Array.init 65536 (fun i -> Math.Min(int(totalOutPartitions)-1,(int)(((int64 i)*(int64 totalOutPartitions))/65536L)))
        // boundaries for further binning at each node
        let maxPerPartition = (65536 + int(totalOutPartitions) - 1) / int(totalOutPartitions)
        let maxValPartition = (maxPerPartition <<< 8) + 256
        minSegVal <- Array.init (int(totalOutPartitions)) (fun i -> int((65536L * (int64 i) + (totalOutPartitions - 1L))/totalOutPartitions))
        segBoundary <- Array.init maxValPartition (fun i -> (int)(((int64 i)*(int64 furtherPartition))/(int64 maxValPartition)))

    member x.StopInstance() =
        (x :> IDisposable).Dispose()

    static member StartRemoteInstance(dim : int, numNodes : int, numInPartPerNode : int, numOutPartPerNode : int, furtherPartition : int, recordsPerNode : int64) () =
        Remote.Current <- new Remote(dim, numNodes, numInPartPerNode, numOutPartPerNode, furtherPartition, recordsPerNode)
        Remote.Current.InitInstance()

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
                let mutable elem = Unchecked.defaultof<GCHandle*uint64[]>
                while (x.AllocCache.Count > 0) do
                    let ret = x.AllocCache.TryDequeue(&elem)
                    if (ret) then
                        (fst elem).Free()
                for s in x.SubPartitionN do
                    for e in s.Value do
                        let (cnt, arrHandle, arr) = e
                        arrHandle.Free()

    // no need for finalize as GCHandle is managed resource with finalize
    //override x.Finalize() =
    //    x.Dispose(false)
    interface IDisposable with
        override x.Dispose() =
            x.Dispose(true)
            GC.SuppressFinalize(x)

    // ====================================================================

    member x.CacheInRAMAndDispose(ms : StreamBase<byte>) =
        ms.Seek(0L, SeekOrigin.Begin) |> ignore
        let parti = ms.ReadUInt32()
        let len = ms.Length - (int64 sizeof<uint32>)
        let addFn (parti : uint32) =
            let ret = (ref 0L, Array.zeroCreate<byte>(int32 maxPartitionLen))
            ret
        let (cnt, part) = x.Partition.GetOrAdd(parti, addFn)
        let start = Interlocked.Add(cnt, len) - len
        if (start + len > maxPartitionLen) then
            Interlocked.Add(cnt, -len) |> ignore
            // throw away, not enough space in cache
            Logger.LogF(LogLevel.Error, fun _ -> "Error: Max Length exceeded")
        else
            let amtRead = ms.Read(part, int start, int len)
            if (amtRead <> int len) then
                failwith (sprintf "Not enough data want: %d actual: %d" len amtRead)
        (ms :> IDisposable).Dispose()

    // essentially only one element per partition
    member x.GetCacheMem(parti : int) : seq<int64 ref*byte[]> =
        seq {
            if (x.Partition.ContainsKey(uint32 parti)) then 
                let (cntR, arr) = x.Partition.[uint32 parti]
                yield (cntR, arr)
            else
                yield (ref 0L, null)
        }

    static member GetCacheMem parti =
        Remote.Current.GetCacheMem(parti)

    // =======================================================================
    // Generic partition getters to perform sort

    member x.GetCachePtr(parti : int) : seq<uint32> =
        Seq.singleton(uint32 parti)

    static member GetCachePtr parti =
        Remote.Current.GetCachePtr(parti)

    // ========================================================================
    // Sub partitions - sort using byte array (possible mis-alignment)
    member x.FurtherPartitionCacheInRAMAndDispose(ms : StreamBase<byte>) =
        ms.Seek(0L, SeekOrigin.Begin) |> ignore
        let parti = ms.ReadUInt32()
        let addFn (parti : uint32) =
            Array.init<int64 ref*byte[]> furtherPartition (fun i -> (ref 0L, Array.zeroCreate<byte>(int maxSubPartitionLen)))
        let partArr = x.SubPartition.GetOrAdd(parti, addFn)
        let alignLen = (dim + 7)/8*8
        let vec = Array.zeroCreate<byte>(alignLen)
        while (ms.Read(vec, 0, dim) = dim) do
            let index0 = ((int vec.[0]) <<< 8) ||| (int vec.[1])
            //assert(int parti = stageTwoPartitionBoundary.[index0])
            let index1 = ((index0 - minSegVal.[int parti]) <<< 8) ||| (int vec.[2])
            let (cnt, arr) = partArr.[segBoundary.[index1]]
            let start = Interlocked.Add(cnt, int64 alignLen) - (int64 alignLen)
            if (start + (int64 alignLen) > maxSubPartitionLen) then
                Interlocked.Add(cnt, int64 -alignLen) |> ignore
               // throw away, not enough space in cache
                Logger.LogF(LogLevel.Error, fun _ -> "Error: Max Length exceeded")
            else
                Buffer.BlockCopy(vec, 0, arr, int start, alignLen)
        (ms :> IDisposable).Dispose()

    static member FurtherPartitionCacheInRAMAndDispose ms =
        Remote.Current.FurtherPartitionCacheInRAMAndDispose(ms)

    member x.GetCacheMemSubPart(parti : int) : seq<int64 ref*byte[]> =
        if (x.SubPartition.ContainsKey(uint32 parti)) then 
            Seq.ofArray(x.SubPartition.[uint32 parti])
        else
            Seq.empty

    static member GetCacheMemSubPart parti =
        Remote.Current.GetCacheMemSubPart(parti)

    member x.ClearCacheMemSubPart(parti : uint32) =
        if (x.SubPartition.ContainsKey(parti)) then 
            if (Utils.IsNotNull x.SubPartition.[parti]) then
                for elem in x.SubPartition.[parti] do
                    let (cnt, arr) = elem
                    cnt := 0L

    static member ClearCacheMemSubPart parti =
        Remote.Current.ClearCacheMemSubPart(parti)

    // ================================================================
    // Make sure it is aligned by allocating 64-bit integer arrays
    member x.FurtherPartitionCacheInRAMAndDisposeN(ms : StreamBase<byte>) =
        ms.Seek(0L, SeekOrigin.Begin) |> ignore
        let parti = ms.ReadUInt32()
        let addFn (parti : uint32) =
            let createArrFn (i : int) =
                let (ret, arrHandle) = x.AllocCache.TryDequeue()
                if (ret) then
                    (ref 0L, fst arrHandle, snd arrHandle)
                else
                    Logger.LogF(LogLevel.Error, fun _ -> "Preallocted cache is finished, creating new one")
                    let allocLen = (int maxSubPartitionLen + sizeof<uint64> - 1)/ sizeof<uint64>
                    let arr = Array.zeroCreate<uint64>(allocLen)
                    let handle = GCHandle.Alloc(arr, GCHandleType.Pinned)
                    (ref 0L, handle, arr)
            Array.init<int64 ref*GCHandle*uint64[]> furtherPartition createArrFn
        let partArr = x.SubPartitionN.GetOrAdd(parti, addFn)
        let alignLen = (dim + 7)/8*8
        let vec = Array.zeroCreate<byte>(alignLen)
        while (ms.Read(vec, 0, dim) = dim) do
            let index0 = ((int vec.[0]) <<< 8) ||| (int vec.[1])
            //assert(int parti = stageTwoPartitionBoundary.[index0])
            let index1 = ((index0 - minSegVal.[int parti]) <<< 8) ||| (int vec.[2])
            let (cnt, arrHandle, arr) = partArr.[segBoundary.[index1]]
            let arrPtr = arrHandle.AddrOfPinnedObject()
            let start = Interlocked.Add(cnt, int64 alignLen) - (int64 alignLen)
            if (start + (int64 alignLen) > maxSubPartitionLen) then
                Interlocked.Add(cnt, int64 -alignLen) |> ignore
               // throw away, not enough space in cache
                Logger.LogF(LogLevel.Error, fun _ -> "Error: Max Length exceeded")
            else
                //Marshal.Copy(vec, 0, IntPtr.Add(arrPtr, int start), alignLen)
                Buffer.BlockCopy(vec, 0, arr, int(start>>>3), alignLen)
        (ms :> IDisposable).Dispose()

    static member FurtherPartitionCacheInRAMAndDisposeN ms =
        Remote.Current.FurtherPartitionCacheInRAMAndDisposeN(ms)

    member x.GetCacheMemSubPartN(parti : int) : seq<int64 ref*GCHandle*uint64[]> =
        //if (x.PartitionIndex.ContainsKey(parti)) then
        //    yield (snd x.Partition.[x.PartitionIndex.[parti]])
        if (x.SubPartitionN.ContainsKey(uint32 parti)) then 
            Seq.ofArray(x.SubPartitionN.[uint32 parti])
        else
            Seq.empty

    static member GetCacheMemSubPartN parti =
        Remote.Current.GetCacheMemSubPartN(parti)

    member x.ClearCacheMemSubPartN(parti : uint32) =
        if (x.SubPartitionN.ContainsKey(parti)) then 
            if (Utils.IsNotNull x.SubPartitionN.[parti]) then
                for elem in x.SubPartitionN.[parti] do
                    let (cnt, arrHandle, arr) = elem
                    cnt := 0L

    static member ClearCacheMemSubPartN parti =
        Remote.Current.ClearCacheMemSubPartN(parti)        

    // ================================================================================
    member val ReadCnt = ref -1 with get

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
                use sr = new StreamReader<byte>(buffer, 0L)

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
                sr.Release()
                let t2 = DateTime.UtcNow

                for i = 0 to int(totalOutPartitions) - 1 do
                    if Utils.IsNotNull partstream.[i] then
                        if (partstream).[i].Length > 0L then
                            (partstream).[i].Seek(0L, SeekOrigin.Begin) |> ignore
                            yield (partstream).[i]
                        else 
                            ((partstream).[i] :> IDisposable).Dispose()
                        
                Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "repartition: %d records, takes %f s" (buffer.Length / 100L) ((t2-t1).TotalSeconds) ) )          
            }
            retseq
        else
            Seq.empty

    static member RepartitionMemStream (buffer) =
        Remote.Current.RepartitionMemStream (buffer)

// ====================================================

module Interop =
    let inline AlignSort(buffer : byte[], align : int, num : int) =
        let bufferHandle = GCHandle.Alloc(buffer, GCHandleType.Pinned)
        try
            NativeSort.Sort.AlignSort64(bufferHandle.AddrOfPinnedObject(), (align+7)/8*8, num)
        finally
            bufferHandle.Free()

let cntLenFn (dim : int) (cnt : int64) (ms : StreamBase<byte>) =
    let ret = cnt + (ms.Length-(int64 sizeof<uint32>))/(int64 dim)
    (ms :> IDisposable).Dispose()
    ret

let aggrFn (cnt1 : int64) (cnt2 : int64) =
    cnt1 + cnt2

let repartitionFn (ms : StreamBase<byte>) =
    ms.Seek(0L, SeekOrigin.Begin) |> ignore
    let index = ms.ReadUInt32()
    int index

let doSort (alignLen : int) (cnt : int64 ref, buf : byte[]) : int64 ref*byte[] =
    let num = int(!cnt/(int64 alignLen))
    Interop.AlignSort(buf, alignLen, num)
    (cnt, buf)

let doSortN (alignLen : int) (cnt : int64 ref, buf : GCHandle, bufArr : uint64[]) : int64 ref*IntPtr =
    let num = int(!cnt/(int64 alignLen))
    NativeSort.Sort.AlignSort64(buf.AddrOfPinnedObject(), alignLen>>>3, num)
    (cnt, buf.AddrOfPinnedObject())

let cntLenByteArrFn (dim : int) (alignLen : int) (cnt : int64) (cntPlusArr : int64 ref*byte[]) =
    let (cntArrR, arr) = cntPlusArr
    cnt + !cntArrR/(int64 alignLen)

let cntLenByteArrNFn (dim : int) (alignLen : int) (cnt : int64) (cntPlusArr : int64 ref*IntPtr) =
    let (cntArrR, arr) = cntPlusArr
    cnt + !cntArrR/(int64 alignLen)

let inMemSort(sort : Remote, remote : DSet<_>) =
    let startDSet = DSet<_>( Name = "SortGen", SerializationLimit = 1) 
    startDSet.NumParallelExecution <- 16 

    let watch = Stopwatch.StartNew()

    let dset1 = startDSet |> DSet.sourceI (int sort.InPartitions) (Remote.ReadFilesToMemStreamFS sort.Dim)
    dset1.NumParallelExecution <- 16 
    dset1.SerializationLimit <- 1

    let dset3 = dset1 |> DSet.map Remote.RepartitionMemStream
    dset3.NumParallelExecution <- 16 
    dset3.SerializationLimit <- 1
                
    let dset4 = dset3 |> DSet.collect Operators.id
    dset4.NumParallelExecution <- 16 

    //let cnt = dset4 |> DSet.fold (cntLenFn sort.Dim) aggrFn 0L
    //Logger.LogF(LogLevel.Info, fun _ -> sprintf "Creating remap stream takes: %f seconds num: %d rate per node: %f Gbps" watch.Elapsed.TotalSeconds cnt ((double cnt)*(double rmtPart.dim)*8.0/1.0e9/(double cluster.NumNodes)/watch.Elapsed.TotalSeconds))
                
    let param = new DParam()
    param.NumPartitions <- int sort.OutPartitions
    let dset5 = dset4 |> DSet.repartitionP param repartitionFn

    // simple fold: count # of elems - gives approx 3.8Gbps
    //let cnt = dset5 |> DSet.fold (cntLenFn sort.Dim) aggrFn 0L
    //Logger.LogF(LogLevel.Info, fun _ -> sprintf "Creating remap + repartition stream takes: %f seconds num: %d rate per node: %f Gbps" watch.Elapsed.TotalSeconds cnt ((double cnt)*(double rmtPart.dim)*8.0/1.0e9/(double cluster.NumNodes)/watch.Elapsed.TotalSeconds))

    // cache in RAM: - gives approx 3Gbps (mostly limited by allocation)
    // gives 3.6Gbps on 2nd try
    //dset5 |> DSet.iter Remote.CacheInRAMAndDispose
    //dset5 |> DSet.iter Remote.FurtherPartitionCacheInRAMAndDispose
    dset5 |> DSet.iter Remote.FurtherPartitionCacheInRAMAndDisposeN
    let cnt = sort.TotalSizeInByte / (int64 sort.Dim)
    Logger.LogF(LogLevel.Info, fun _ -> sprintf "Creating remap + repartition + cacheInRam stream takes: %f seconds num: %d rate per node: %f Gbps" watch.Elapsed.TotalSeconds cnt ((double cnt)*(double sort.Dim)*8.0/1.0e9/(double sort.NumNodes)/watch.Elapsed.TotalSeconds))

    // now sort
    let startRepart = DSet<_>(Name = "SortVec", SerializationLimit = 1)
    startRepart.NumParallelExecution <- 16

    // mapping must also match dset5, hopefully just setting NumPartitions will do the trick
    //let dset6 = startRepart |> DSet.sourceI dset5.NumPartitions Remote.GetCacheMem
    //let dset6 = startRepart |> DSet.sourceI dset5.NumPartitions Remote.GetCacheMemSubPart
    let dset6 = startRepart |> DSet.sourceI dset5.NumPartitions Remote.GetCacheMemSubPartN
    let alignLen = (sort.Dim + 7)/8*8
    let dset7 = dset6 |> DSet.map (doSortN alignLen)
    let cnt = dset7 |> DSet.fold (cntLenByteArrNFn sort.Dim alignLen) aggrFn 0L
    Logger.LogF(LogLevel.Info, fun _ -> sprintf "Creating remap + repartition stream + cache + sort takes: %f seconds num: %d rate per node: %f Gbps" watch.Elapsed.TotalSeconds cnt ((double cnt)*(double sort.Dim)*8.0/1.0e9/(double sort.NumNodes)/watch.Elapsed.TotalSeconds))

    let dset8 = DSet<_>(Name = "ClearCache", SerializationLimit = 1) |> DSet.sourceI dset5.NumPartitions Remote.GetCachePtr
    //dset8 |> DSet.iter Remote.ClearCacheMemSubPart
    dset8 |> DSet.iter Remote.ClearCacheMemSubPartN

[<EntryPoint>]
let main orgargs = 
    let args = Array.copy orgargs
    let parse = ArgumentParser(args)
    let PrajnaClusterFile = parse.ParseString( "-cluster", "c:\onenet\cluster\onenet21-25.inf" )
    let nDim = parse.ParseInt( "-dim", 100 )
    let recordsPerNode = parse.ParseInt64( "-records", 250000000L ) // records per node
    let numInPartPerNode = parse.ParseInt( "-nfile", 8 ) // number of partitions (input)
    let numOutPartPerNode = parse.ParseInt( "-nump", 8 ) // number of partitions (output)
    let furtherPartition = parse.ParseInt("-fnump", 2500) // further binning for improved sort performance

    let bAllParsed = parse.AllParsed Usage
    let mutable bExecute = false

    if (bAllParsed) then
        // start cluster
        Cluster.Start( null, PrajnaClusterFile )

        // add other dependencies
        let curJob = JobDependencies.setCurrentJob "SortGen"
        JobDependencies.Current.Add([|"qsort.dll"|])
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
        remoteExec.Execute(Remote.StartRemoteInstance(nDim, numNodes, numInPartPerNode, numOutPartPerNode, furtherPartition, recordsPerNode))
        Logger.LogF(LogLevel.Info, fun _ -> sprintf "Init plus alloc takes %f seconds" watch.Elapsed.TotalSeconds)

        inMemSort(sort, remoteExec)
        // repeat twice
        inMemSort(sort, remoteExec)

        // stop remote instances
        remoteExec.Execute(Remote.StopRemoteInstance)

        Cluster.Stop()

        bExecute <- true

    // Make sure we don't print the usage information twice. 
    if not bExecute && bAllParsed then 
        parse.PrintUsage Usage

    0
