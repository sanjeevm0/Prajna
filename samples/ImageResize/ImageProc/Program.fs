open System
open System.IO
open System.Windows.Forms
open System.Drawing
open System.Drawing.Imaging
open System
open Prajna.Core
open Prajna.Api.FSharp
open Prajna.Tools

let imgProc = new ImageProcUM.ProcUM()

let ImageDraw(buf : byte[]) =
    let ms = new MemStream(buf)
    let bmp = new System.Drawing.Bitmap(ms)
    bmp

let ImageProc(buf : byte[]) =
    let msIn = new MemStream(buf)
    let bmpIn = new System.Drawing.Bitmap(msIn)
    let bmpDataIn = bmpIn.LockBits(new Rectangle(0, 0, bmpIn.Width, bmpIn.Height), ImageLockMode.ReadOnly, PixelFormat.Format24bppRgb)
    let ptrIn = bmpDataIn.Scan0
    let bmpOut = new System.Drawing.Bitmap(bmpIn.Width/2, bmpIn.Height/2)
    let bmpDataOut = bmpOut.LockBits(new Rectangle(0, 0, bmpOut.Width, bmpOut.Height), ImageLockMode.ReadWrite, PixelFormat.Format24bppRgb)
    let ptrOut = bmpDataOut.Scan0
    // process
    imgProc.Process(ptrIn, ptrOut, bmpIn.Width, bmpIn.Height, bmpDataIn.Stride, bmpDataOut.Stride)
    // create buffer of output
    let msOut = new MemStream()
    bmpIn.UnlockBits(bmpDataIn)
    bmpOut.UnlockBits(bmpDataOut)
    bmpOut.Save(msOut, ImageFormat.Jpeg)
    // dispose
    bmpIn.Dispose()
    bmpOut.Dispose()
    // return
    msOut.GetBuffer()

let ImageProc2(buf : byte[]) =
    buf

// perform image processing using unmanaged code on DKV
[<EntryPoint>]
let main argv =  
    let parse = ArgumentParser(argv)
    let PrajnaClusterFile = parse.ParseString( "-cluster", "" )
    let localdir = parse.ParseString( "-local", "" )
    let remoteDKVname = parse.ParseString( "-remote", "" )
    let versionInfo = parse.ParseString( "-ver", "" )
    let ver = if versionInfo.Length=0 then DateTime.Now else StringTools.VersionFromString( versionInfo) 

    //DKVAction.DefaultTypeOfJobMask <- PrajnaTaskType.ApplicationMask

    Cluster.Start( "", PrajnaClusterFile )
    // add other file dependencies
    //PrajnaJobDependencies.AddTo([|("a.txt", "test/a.txt"); ("b.txt", "test/b.txt")|])

    let t1 = DateTime.Now
    let mutable curDKV = DSet<string*byte[]>( Name = remoteDKVname,
                                              Version = ver) |> DSet.loadSource
    let mutable procDKVSeq = curDKV.MapByValue(ImageProc).ToSeq()
    let (numFiles, total) = DSet.RetrieveFolderRecursive(localdir, procDKVSeq)
    let t2 = DateTime.Now
    let elapse = t2.Subtract(t1)
    Logger.Log( LogLevel.Info, ( sprintf "Processed %d Files with total %dB in %f sec, throughput = %f MB/s" numFiles total elapse.TotalSeconds ((float total)/elapse.TotalSeconds/1000000.) ))

    0

(*
[<EntryPoint>]
let main argv =  
    let parse = ArgumentParser(argv)
    let file = parse.ParseString( "-file", "" )

    let bufOut = ImageProc(File.ReadAllBytes(file))
    File.WriteAllBytes(file+"_testproc.jpg", bufOut)

    0
*)

(*
[<EntryPoint>]
let main2 argv =
    let parse = ArgumentParser(argv)
    let file = parse.ParseString( "-file", "" )
    let form = new Form()
    let pbox = new PictureBox()
    let button = new Button()

    form.Controls.Add(pbox)
    pbox.Image <- ImageDraw(File.ReadAllBytes(file))
    form.ShowDialog() |> ignore

    0
*)
