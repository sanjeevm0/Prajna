(*---------------------------------------------------------------------------
	Copyright 2015 Microsoft

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.                                                      

	Author: Sanjeev Mehrotra
 ---------------------------------------------------------------------------*)

namespace Prajna.Tools

open System
open System.Threading
open System.Collections.Generic
open System.Collections.Concurrent
open Prajna.Tools
open Prajna.Tools.FSharp

type SingleThreadExecT() =
    let counter = ref 0
    let q = new ConcurrentQueue<unit->unit>()

    // execute function only on one thread - but at least one time after call
    member x.ExecOnce(f : unit->unit) =
        if (Interlocked.Increment(counter) = 1) then
            let mutable bDone = false
            while (not bDone) do
                // get count prior to executing
                let curCount = !counter
                f()
                bDone <- (Interlocked.Add(counter, -curCount) = 0)

    member x.ExecQ(f : unit->unit) =
        q.Enqueue(f)
        if (Interlocked.Increment(counter) = 1) then
            let mutable bDone = false
            let fn = ref (fun () -> ())
            while (not bDone) do
                let ret = q.TryDequeue(fn)
                if (ret) then
                    (!fn)()
                    bDone <- (Interlocked.Decrement(counter) = 0)

// =====================================================================

type [<AbstractClass>] [<AllowNullLiteral>] ThreadBase(pool : ThreadPoolBase) =
    let mutable thread : Thread = null
    let mutable terminate = false
    let mutable stop = false

    //static member val BackgroundThreads = false // prevents process from terminating
    static member val BackgroundThreads = true
    static member val LogLevel = LogLevel.WildVerbose with get

    member val ControlHandle : EventWaitHandle = null with get, set

    member val StopHandle = new ManualResetEvent(false) with get

    abstract Stop : unit->unit
    default x.Stop() =
        stop <- true
        if (Utils.IsNotNull x.ControlHandle) then
            x.ControlHandle.Set() |> ignore

    abstract Terminate : unit->unit
    default x.Terminate() =
        terminate <- true
        if (Utils.IsNotNull x.ControlHandle) then
            x.ControlHandle.Set() |> ignore

    member val Id = -1L with get, set

    member x.Thread with get() = thread

    member x.Start() =
        thread.Start()
    
    abstract Init : unit->unit
    default x.Init() =
        let threadStart = new ThreadStart(x.Process)
        thread <- new Thread(threadStart)
        thread.IsBackground <- ThreadBase.BackgroundThreads

    abstract ToStop : (unit->bool) with get
    abstract ProcessOne : unit->unit

    member x.Process() =
        while not (terminate || (stop && x.ToStop())) do
            x.ProcessOne()
        x.StopHandle.Set() |> ignore

    override x.Finalize() =
        if (ThreadBase.BackgroundThreads = false) then
            x.Terminate()

    interface IDisposable with
        override x.Dispose() =
            x.Finalize()
            GC.SuppressFinalize(x)

and [<AbstractClass>] ThreadPoolBase() =
    let id = ref -1L
    let mutable bStop = false
    let mutable bTerminate = false
    let mutable inCreation = false
    let stopList = new List<WaitHandle>()
    let threads = new ConcurrentDictionary<int64, ThreadBase>()
    
    member x.Terminate() =
        bTerminate <- true
        let curId = !id
        let sp = new SpinWait()
        while (inCreation && !id=curId) do
            sp.SpinOnce()
        for t in threads do
            t.Value.Terminate()

    member x.Stop() =
        bStop <- true
        let curId = !id
        let sp = new SpinWait()
        while (inCreation && !id=curId) do
            sp.SpinOnce()
        for t in threads do
            t.Value.Stop()

    member x.StopAndWait(timeout : int) =
        x.Stop()
        let wh = stopList.ToArray()
        WaitHandle.WaitAll(wh, timeout) |> ignore

    member x.Threads with get() = threads

    member val ST = new SingleThreadExecT() with get

    abstract CreateNew : (ThreadPoolBase -> ThreadBase) with get, set

    member x.CreateNewThread() =        
        let ntId = Interlocked.Increment(id)
        inCreation <- true
        if (not bTerminate && not bStop) then
            let nt = x.CreateNew(x)
            nt.Id <- ntId
            nt.Init()
            threads.[ntId] <- nt
            stopList.Add(nt.StopHandle)
            nt.Start()
        inCreation <- false

//    override x.Finalize() =
//        x.Terminate()
//
//    interface IDisposable with
//        override x.Dispose() =
//            x.Finalize()
//            GC.SuppressFinalize(x)

// =====================================================================================

type WaitThread(cwait : Wait, waitPerThread : int) as x =
    inherit ThreadBase(cwait :> ThreadPoolBase)

    let handles = new ConcurrentDictionary<int64, int64*WaitHandle*(unit->unit)*bool*(string)>()
    let waitArr = Array.zeroCreate<WaitHandle> (waitPerThread+1)
    let waitMap = Array.zeroCreate<int64*(unit->unit)*bool*(string)> (waitPerThread+1)

    let mutable id = ref -1L

    let pendingAdd = ref 0L

    member val NumWaits = ref 0 with get

    member x.WaitPerThread with get() = waitPerThread

    override x.Init() =
        base.Init()
        x.ControlHandle <- new ManualResetEvent(false)
        //let pos = Interlocked.Increment(id)
        //handles.[pos] <- ((pos, x.ControlHandle :> WaitHandle, (fun () -> ()), false, "Control Handle"))

    member x.AddWaitHandle(handleToQ : WaitHandle, func : unit->unit, bRemove : bool, info : string) =
        let pos = Interlocked.Increment(id)
        handles.[pos] <- ((pos, handleToQ, func, bRemove, info))
        Logger.LogF(ThreadBase.LogLevel, fun _ -> "AddedHandle: " + info)
        Interlocked.Increment(pendingAdd) |> ignore
        x.ControlHandle.Set() |> ignore

    override val ToStop = (fun () -> !(x.NumWaits)=0) with get

    override x.ProcessOne() =
        // start a wait
        let mutable cnt = 0
        let pendingCnt = !pendingAdd
        for h in handles do
            let (id, handle, func, toRemove, info) = h.Value
            if (Utils.IsNotNull handle) then
                waitArr.[cnt] <- handle
                waitMap.[cnt] <- (id, func, toRemove, info)
                cnt <- cnt + 1
        waitArr.[cnt] <- x.ControlHandle :> WaitHandle
        WaitHandle.WaitAny(Array.sub waitArr 0 (cnt+1)) |> ignore
        Interlocked.Add(pendingAdd, -pendingCnt) |> ignore
        if (0L = !pendingAdd) then
            x.ControlHandle.Reset() |> ignore
            if (!pendingAdd > 0L) then
                x.ControlHandle.Set() |> ignore
        for i = 0 to cnt-1 do
            // perform continuation
            let (id, func, toRemove, info) = waitMap.[i]
            if (waitArr.[i].WaitOne(0)) then
                Logger.LogF(ThreadBase.LogLevel, fun _ -> "WaitSatisfied: " + info)
                func()
                if (toRemove) then
                    //handles.Remove(id) |> ignore
                    handles.TryRemove(id) |> ignore
                    Interlocked.Decrement(x.NumWaits) |> ignore
                    Interlocked.Decrement(cwait.TotalWaitUsed) |> ignore
                else
                    Logger.LogF(ThreadBase.LogLevel, fun _ -> "NoRemove: " + info)

and Wait private () =
    inherit ThreadPoolBase()

    static let current = new Wait()
    static let checkThreadIntervalCount = 200

    let waitPerThread = 63

    let toAdd = new ConcurrentQueue<WaitHandle*(unit->unit)*bool*(string)>()
    let toAddCnt = ref 0L
    let mutable countSinceLastCheck = 0
    let mutable peakWaits = 0L
    let getNew = (fun (tp : ThreadPoolBase) ->
        new WaitThread(tp :?> Wait, waitPerThread) :> ThreadBase
    )

    static member Current with get() = current // only Wait.Current exists, other instantiation not allowed

    static member StopCurrent() =
        current.StopAndWait(-1)

    override val CreateNew = getNew with get, set

    member val TotalWaitAvail = 0L with get, set
    member val TotalWaitUsed :int64 ref = ref 0L with get

    member x.Process() =
        // get count prior to increasing thread count to make sure enough threads available
        let mutable countToAdd = !toAddCnt
        Interlocked.Add(toAddCnt, -countToAdd) |> ignore
        // increase thread count to satisfy number of waits
        while (!x.TotalWaitUsed > x.TotalWaitAvail) do
            x.CreateNewThread()
            x.TotalWaitAvail <- x.TotalWaitAvail + int64 waitPerThread
        // add new waits
        let r = ref Unchecked.defaultof<WaitHandle*(unit->unit)*bool*(string)>
        // don't go all the way to toAdd.Count being zero as ther may not be enough threads available
        // next call to Process through ExecOnce will take care of the remainder
        while (countToAdd > 0L) do
            let ret = toAdd.TryDequeue(r)
            if (ret) then
                let mutable bDone = false
                let mutable bFound = false
                let wEnum = x.Threads.GetEnumerator()
                while not bDone do
                    if (wEnum.MoveNext()) then
                        let w = wEnum.Current.Value :?> WaitThread
                        if (!w.NumWaits < w.WaitPerThread) then
                            Interlocked.Increment(w.NumWaits) |> ignore
                            w.AddWaitHandle(!r)
                            bFound <- true
                            bDone <- true
                            peakWaits <- Math.Max(peakWaits, !x.TotalWaitUsed)
                            countSinceLastCheck <- countSinceLastCheck + 1
                    else
                        bDone <- true
                assert(bFound)
                countToAdd <- countToAdd - 1L
        // decrease thread count
        if (countSinceLastCheck > checkThreadIntervalCount) then
            let wEnum = x.Threads.GetEnumerator()
            let mutable bDone = false
            while (not bDone && x.TotalWaitAvail > peakWaits + int64 waitPerThread) do // leaves one extra thread available
                if (wEnum.MoveNext()) then
                    let w = wEnum.Current
                    x.TotalWaitAvail <- x.TotalWaitAvail - int64 waitPerThread
                    w.Value.Stop()
                    x.Threads.TryRemove(w.Key) |> ignore
            peakWaits <- !x.TotalWaitUsed
            countSinceLastCheck <- 0                        

    member x.WaitForHandle (h : WaitHandle, cont : unit->unit, bRepeat : bool, info : string) =
        Interlocked.Increment(x.TotalWaitUsed) |> ignore
        Logger.LogF(ThreadBase.LogLevel, fun _ -> sprintf "WaitForHandle: %s Total: %d" info !x.TotalWaitUsed)
        toAdd.Enqueue((h, cont, not bRepeat, info))
        Interlocked.Increment(toAddCnt) |> ignore
        x.ST.ExecOnce(x.Process)

// ==============================================================================

[<AllowNullLiteral>]
type TPThread(tp : ThreadPool, minTaskCnt : int) =
    inherit ThreadBase(tp :> ThreadPoolBase)

    let handle = new ManualResetEvent(true)
    let mutable inWait = false
    let mutable inRun = false

    member x.Handle with get() = handle

    override x.Init() =
        base.Init()
        x.ControlHandle <- x.Handle :> EventWaitHandle
        tp.WaitList.[minTaskCnt] <- (x.ControlHandle, x)

    //override val ToStop = (fun () -> !tp.TaskTotal <= minTaskCnt) with get
    override val ToStop = (fun () -> true) with get

    override x.ProcessOne() =
        let (ret, r) = tp.TaskQ.TryDequeue()
        if (ret) then
            let (task, bRemove, finishCb, info) = r
            Logger.LogF(ThreadBase.LogLevel, fun _ -> "RunTask: " + info)
            inRun <- true
            let (event, finish) = task()
            inRun <- false
            if (not bRemove && not finish && Utils.IsNull event) then
                // immediately requeue
                Logger.LogF(ThreadBase.LogLevel, fun _ -> "FinishRequeue: " + info)
                tp.TaskQ.Enqueue(r)
            else
                // remove
                Interlocked.Decrement(tp.TaskTotal) |> ignore
                if (not bRemove && not finish) then
                    Logger.LogF(ThreadBase.LogLevel, fun _ -> "NotFinishWait: " + info)
                    Wait.Current.WaitForHandle(event, (fun _ -> tp.AddWorkItem(task, not bRemove, finishCb, info)), false, "Requeue" + info)
                else
                    // execute the finish callback
                    match finishCb with
                        | None -> ()
                        | Some(cb) -> cb()
                    Logger.LogF(ThreadBase.LogLevel, fun _ -> "RemoveTask: " + info)
                    // make call to adjust thread count
                    tp.ST.ExecOnce(tp.AdjustThreadCount)
        // wait if needed
        if (!tp.TaskTotal <= minTaskCnt) then
            handle.Reset() |> ignore
            if (!tp.TaskTotal <= minTaskCnt) then
                inWait <- true
                handle.WaitOne() |> ignore
                inWait <- false
            else
                handle.Set() |> ignore
    
and ThreadPool() as x =
    inherit ThreadPoolBase()

    static let current = new ThreadPool()

    static let checkTaskInterval = 100
    static let checkTaskIntervalTime = 3000L // in milliseconds

    let watch = Diagnostics.Stopwatch.StartNew()
    let mutable minTasksOverWindow = Int32.MaxValue
    let mutable maxTasksOverWindow = 0
    let mutable taskIntervalCount = 0
    let mutable taskIntervalLastTime = watch.ElapsedMilliseconds
    let createNew  = (fun (tpb : ThreadPoolBase) ->
        let waitListId = x.WaitList.Count
        let t = new TPThread(tpb :?> ThreadPool, waitListId)
        t :> ThreadBase
    )
    let minThreads, minIOThreads = ThreadPool.GetMinThreads()
    let maxThreads, maxIOThreads = ThreadPool.GetMaxThreads()

    new (minCount : int, maxCount : int) as x =
        new ThreadPool()
        then
            x.SetThreads(minCount, maxCount)

    static member Current with get() = current

    static member StopCurrent() =
        current.StopAndWait(-1)

    member x.SetThreads(minCount : int, maxCount : int) =
        x.MinThreads <- minCount
        x.MaxThreads <- Math.Max(maxCount, x.MinThreads)
        x.ST.ExecOnce(x.AdjustThreadCount)

    member val TaskQ : ConcurrentQueue<(unit->WaitHandle*bool)*bool*Option<unit->unit>*string> = new ConcurrentQueue<_>() with get
    member val WaitList : ConcurrentDictionary<int, EventWaitHandle*TPThread> = new ConcurrentDictionary<_,_>() with get
    member val private MinThreads = 4 with get, set
    //member val private MaxThreads = Environment.ProcessorCount * 4 with get, set
    member val private MaxThreads = maxThreads with get, set
    override val CreateNew = createNew with get, set

    //member val TaskInQ : int ref = ref 0 with get // not really useful
    member val TaskTotal : int ref = ref 0 with get

    member x.AdjustThreadCount() =
        minTasksOverWindow <- Math.Min(!x.TaskTotal, minTasksOverWindow)
        maxTasksOverWindow <- Math.Max(!x.TaskTotal, maxTasksOverWindow)
        taskIntervalCount <- taskIntervalCount + 1
        let mutable newThreads = x.Threads.Count
        if (taskIntervalCount > checkTaskInterval ||
            watch.ElapsedMilliseconds - taskIntervalLastTime > checkTaskIntervalTime) then
//            if (minTasksOverWindow > x.Threads.Count*2) then
//                newThreads <- minTasksOverWindow*3/2
//            else if (maxTasksOverWindow < x.Threads.Count/2) then
//                newThreads <- maxTasksOverWindow*2/3
            newThreads <- maxTasksOverWindow // allow reduction of thread count
            minTasksOverWindow <- !x.TaskTotal
            maxTasksOverWindow <- !x.TaskTotal
            taskIntervalCount <- 0
            taskIntervalLastTime <- watch.ElapsedMilliseconds
        newThreads <- Math.Max(newThreads, !x.TaskTotal)
        newThreads <- Math.Max(newThreads, x.MinThreads)
        newThreads <- Math.Min(newThreads, x.MaxThreads)
        let newThreads = newThreads
//        Logger.LogF(LogLevel.Info, fun _ -> 
//            sprintf "TaskCnt: %d ThreadCnt: %d NewThreads: %d MinTasksOverWindow: %d MaxTasksOverWindow: %d" 
//                !x.TaskTotal x.Threads.Count newThreads minTasksOverWindow maxTasksOverWindow
//        )
        while (x.Threads.Count < newThreads) do
            x.CreateNewThread()
        while (x.Threads.Count > newThreads) do
            // remove last one
            let remove = x.Threads.Count - 1
            let (wh, t) = x.WaitList.[remove]
            t.Stop()
            x.WaitList.TryRemove(remove) |> ignore
            x.Threads.TryRemove(t.Id) |> ignore

    member x.AddWorkItem (cont : unit->WaitHandle*bool, bRepeat : bool, finishCb : Option<unit->unit>, info : string) =
        let newTaskCount = Interlocked.Increment(x.TaskTotal)
        Logger.LogF(ThreadBase.LogLevel, fun _ -> "AddTask: " + info)
        x.TaskQ.Enqueue((cont, not bRepeat, finishCb, info))
        let (ret, t) = x.WaitList.TryGetValue(newTaskCount-1)
        if (ret) then
            let (wh, t) = t
            t.ControlHandle.Set() |> ignore
        x.ST.ExecOnce(x.AdjustThreadCount)

    member x.AddWorkItem (cont : unit->ManualResetEvent*bool, finishCb : Option<unit->unit>, info : string) =
        let wrappedFunc() : WaitHandle*bool =
            let (event, b) = cont()
            (event :> WaitHandle, b)
        x.AddWorkItem(wrappedFunc, true, finishCb, info)

    member x.WaitAndContinueOnThreadPool(h : WaitHandle, cont : unit->unit, info : string) =
        let func() : WaitHandle*bool =
            cont()
            (null, true)
        let funcAddWork() =
            ThreadPool.Current.AddWorkItem(func, false, None, "AddWorkItem" + info)
        Wait.Current.WaitForHandle(h, funcAddWork, false, info)

// ==================================================================

type Wait with
    member x.WaitAndContinueOnThreadPool(h : WaitHandle, cont : unit->unit, info : string) =
        ThreadPool.Current.WaitAndContinueOnThreadPool(h, cont, info)

