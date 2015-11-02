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
		LoggerCSharp.fs
  
	Description: 
		Logger APIs for CSharp users

	Author:																	
 		Jin Li, Partner Research Manager
 		Microsoft Research, One Microsoft Way
 		Email: jinl at microsoft dot com
    Date:
        Apr. 2013
	
 ---------------------------------------------------------------------------*)
namespace Prajna.Tools.CSharp

open System
open Prajna.Tools

/// Provides logging interface and utility functions. 
type Logger private() =
    inherit Prajna.Tools.Logger()

    /// Log the message generated by "messageFunc", "messageFunc" is evaluated only if logLevel <= Logger.DefaultLogLevel
    static member LogF(logLevel : LogLevel, messageFunc : Func<string>) =
        if logLevel <= Logger.DefaultLogLevel then
            Logger.LoggerProvider.Log(logLevel, messageFunc.Invoke())

    /// Log message generated by "messageFunc" using "logId", 
    /// "messageFunc" is evaluated only if "logLevel" is less than or equal to the log level for "logId"
    static member LogF(logId : string, logLevel : LogLevel,messageFunc : Func<string>) =
        if Logger.LoggerProvider.IsEnabled(logId, logLevel) then
            Logger.LoggerProvider.Log(logId, logLevel, messageFunc.Invoke())

    /// Log message generated by "messageFunc" using "JobId", 
    /// "messageFunc" is evaluated only if "logLevel" is less than or equal to the log level for "logId"
    static member inline LogF(jobID : Guid, logLevel : LogLevel,messageFunc : Func<string>) =
        if logLevel <= Logger.DefaultLogLevel then
            Logger.LoggerProvider.Log(jobID, logLevel, messageFunc.Invoke())

    /// Log message generated by "messageFunc" using "JobId", 
    /// "messageFunc" is evaluated only if "logLevel" is less than or equal to the log level for "logId"
    static member LogF(logId: string, jobID : Guid, logLevel : LogLevel,messageFunc : Func<string>) =
         if Logger.LoggerProvider.IsEnabled(logId, logLevel) then
            Logger.LoggerProvider.Log(logId,jobID, logLevel, messageFunc.Invoke())

    /// Execute 'action' if logLevel <= Logger.DefaultLogLevel 
    static member Do(logLevel : LogLevel, action : Action<unit> ) =
        if logLevel <= Logger.DefaultLogLevel then
            action.Invoke()

    /// Execute 'action' if "logLevel" is less than or equal to the log level for "logId"
    static member Do(logId : string, logLevel : LogLevel, action : Action<unit> ) =
        if Logger.LoggerProvider.IsEnabled(logId, logLevel) then
            action.Invoke()
