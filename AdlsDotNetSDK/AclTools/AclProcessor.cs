using Microsoft.Azure.DataLake.Store.Acl;
using Microsoft.Azure.DataLake.Store.AclTools.Jobs;
using Microsoft.Azure.DataLake.Store.QueueTools;
using NLog;
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;

namespace Microsoft.Azure.DataLake.Store.AclTools
{
    /// <summary>
    /// Types of ACL operations
    /// </summary>
    public enum RequestedAclType
    {
        /// <summary>
        /// Adds new Acl entries
        /// </summary>
        ModifyAcl,
        /// <summary>
        /// Resets the acl entries
        /// </summary>
        SetAcl,
        /// <summary>
        /// Removes the specified ACL entries
        /// </summary>
        RemoveAcl
    }
    /// <summary>
    /// This class is used to Acl Processor and Acl Verification. Acl Verification is insternal
    /// </summary>
    internal class AclProcessor
    {
        private static readonly Logger AclLog = LogManager.GetLogger("adls.dotnet.AclTool");
        private static readonly Logger AclJobLog = LogManager.GetLogger("adls.dotnet.AclTool.Job");
        private readonly string _inputPath;
        /// <summary>
        /// Adls Client
        /// </summary>
        internal AdlsClient Client { get; }
        /// <summary>
        /// Acl entries that should be used for Acl modify/set/remove
        /// </summary>
        internal List<AclEntry> AclEntries { get; }
        /// <summary>
        /// AclEntries without the default Acl entries since default does not apply for files
        /// </summary>
        internal List<AclEntry> FileAclEntries { get; }
        /// <summary>
        /// Priority Queue that queues the Acl jobs
        /// </summary>
        internal PriorityQueueWrapper<BaseJob> Queue { get; }
        // Verify flag- when set verifies whether acls have been changed correctly
        private readonly bool _isVerify;
        /// <summary>
        /// Client exception if any raised by any thread
        /// </summary>
        private Exception _clientException;
        /// <summary>
        /// Number of threads that process the Acl
        /// </summary>
        private int NumThreads { get; }

        /// <summary>
        /// Array of thread workers
        /// </summary>
        private readonly Thread[] _threadWorker;
        /// <summary>
        /// Mutex Lock object for doing synchronized setting and getting the clientexception
        /// </summary>
        private readonly Object _thisLock = new Object();
        /// <summary>
        /// Job type-Set Acl or Modify Acl or Remove Acl or ACl set verify or Acl modify verify or Acl remove Verfiy
        /// </summary>
        internal RequestedAclType Type { get; }
        /// <summary>
        /// Total Files processed
        /// </summary>
        private int _filesCount;
        /// <summary>
        /// Total Directories processed
        /// </summary>
        private int _directoryCount;
        /// <summary>
        /// Number of files for which Acl change was correct. This is used for internal use.
        /// </summary>
        private int _filesCorrect;
        /// <summary>
        /// Number of directories for which Acl change was correct. This is used for internal use.
        /// </summary>
        private int _directoryCorrect;
        private AclProcessor(string path,AdlsClient client, List<AclEntry> aclEntries, RequestedAclType type, int threadCount,bool verify=false)
        {
            _inputPath = path;
            Client = client;
            NumThreads = threadCount < 0 ? AdlsClient.DefaultNumThreads: threadCount;
            Queue = new PriorityQueueWrapper<BaseJob>(NumThreads);
            _threadWorker = new Thread[NumThreads];
            AclEntries = aclEntries;
            FileAclEntries = new List<AclEntry>(AclEntries.Count);
            foreach (var entry in AclEntries)
            {
                if (entry.Scope == AclScope.Access)
                {
                    FileAclEntries.Add(entry);
                }
            }
            Type = type;
            _isVerify = verify;
            if (AclLog.IsDebugEnabled)
            {
                AclLog.Debug($"AclProcessor, Name: {_inputPath}, Threads: {NumThreads}, AclChangeType: {Type}, InputAcl: {string.Join(":",AclEntries)}{(_isVerify?", RunInVerifyMode":string.Empty)}");
            }
        }
        /// <summary>
        /// Atomically sets the client exception
        /// </summary>
        /// <param name="ex"></param>
        private void SetException(Exception ex)
        {
            lock (_thisLock)
            {
                _clientException = ex;
            }
        }
        /// <summary>
        /// Atomically gets the client exception
        /// </summary>
        /// <returns></returns>
        private Exception GetException()
        {
            lock (_thisLock)
            {
                return _clientException;
            }
        }
        /// <summary>
        /// Api to call Acl Processor. Runs Acl Processor and returns the results.
        /// </summary>
        /// <param name="path">Root path from where the Acl recursive processor will start</param>
        /// <param name="client">ADLS Client</param>
        /// <param name="aclEntries">Acl Entries to change</param>
        /// <param name="type">Type of Acl Job: Acl modify or Acl set or acl remove</param>
        /// <param name="threadCount">Custom number of threads</param>
        /// <returns></returns>
        internal static AclProcessorStats RunAclProcessor(string path,AdlsClient client, List<AclEntry> aclEntries, RequestedAclType type, int threadCount = -1)
        {
            return new AclProcessor(path,client, aclEntries, type, threadCount).ProcessAcl();
        }
        /// <summary>
        /// Internal test Api to verify Acl Processor. Runs Acl verifier and returns number of files and directories processed correctly.
        /// </summary>
        /// <param name="path">Root path from where the Acl recursive verifier will start</param>
        /// <param name="client">ADLS Client</param>
        /// <param name="aclEntries">Acl Entries to verify</param>
        /// <param name="type">Type of Acl Job: Acl modify verify or Acl set verify or acl remove verify</param>
        /// <param name="threadCount">Custom number of threads</param>
        /// <returns></returns>
        internal static AclProcessorStats RunAclVerifier(string path, AdlsClient client, List<AclEntry> aclEntries,
            RequestedAclType type, int threadCount = -1)
        {
            return new AclProcessor(path, client, aclEntries, type, threadCount,true).ProcessAcl();
        }
        /// <summary>
        /// Starts the Acl Processor threads. Returns the results or throws any exceptions.
        /// </summary>
        /// <returns>Acl Processor: Number of files and directories processed or ACl Verification: Number of files and directories processed and number of files and directories correctly processed by Acl Processor</returns>
        private AclProcessorStats ProcessAcl()
        {
            DirectoryEntry dir = Client.GetDirectoryEntry(_inputPath);
            for (int i = 0; i < NumThreads; i++)
            {
                _threadWorker[i] = new Thread(Run);
            }
            ProcessDirectoryEntry(dir);
            for (int i = 0; i < NumThreads; i++)
            {
                _threadWorker[i].Start();
            }
            for (int i = 0; i < NumThreads; i++)
            {
                _threadWorker[i].Join();
            }
            if (GetException() != null)
            {
                throw GetException();
            }
            return _isVerify?new AclProcessorStats(_filesCount,_directoryCount,_filesCorrect,_directoryCorrect):new AclProcessorStats(_filesCount, _directoryCount) ;
        }
        internal void ProcessDirectoryEntry(DirectoryEntry dir)
        {
            if (dir.Type == DirectoryEntryType.DIRECTORY)
            {
                Queue.Add(new EnumerateDirectoryChangeAclJob(this, dir.FullName));
                Interlocked.Increment(ref _directoryCount);
            }
            else
            {
                Interlocked.Increment(ref _filesCount);
            }
            if (_isVerify)
            {
                Queue.Add(new VerifyChangeAclJob(this, dir.FullName, dir.Type));
            }
            else
            {
                Queue.Add(new ChangeAclJob(this, dir.FullName, dir.Type));
            }
        }
        /// <summary>
        /// Increments the correct count of files and directories
        /// </summary>
        /// <param name="type">Type of Directory entry</param>
        internal void IncrementCorrectCount(DirectoryEntryType type)
        {
            if (type == DirectoryEntryType.DIRECTORY)
            {
                Interlocked.Increment(ref _directoryCorrect);
            }
            else
            {
                Interlocked.Increment(ref _filesCorrect);
            }
        }
        /// <summary>
        /// Method run by a single thread. Polls a directory entry. If job is of type EnumerateDirectory then enumerates contents in it and queues them. 
        /// If job is of type Acl Change then performs the Acl change. If job type is Acl Verify then does acl verification
        /// </summary>
        private void Run()
        {
            while (true)
            {
                var job = Queue.Poll();
                if (GetException()!=null||job == null||job is PoisonJob)//Exception or Poision block (all threads are waiting)
                {
                    Queue.Add(new PoisonJob());
                    return;
                }
                
                try
                {
                    job.DoRun(AclJobLog);
                }
                catch (AdlsException ex)
                {
                    if (ex.HttpStatus != HttpStatusCode.NotFound)//Do not stop acl processor if the file/directory is deleted
                    {
                        SetException(ex);//Sets the global exception to signal other threads to close
                        Queue.Add(new PoisonJob());//Handle corner cases like when exception is raised other threads can be in wait state
                        return;
                    }
                }
                catch (Exception ex)
                {
                    SetException(ex);
                    Queue.Add(new PoisonJob());//Handle corner cases like when exception is raised other threads can be in wait state
                    return;
                }
            }
        }
        

    }
}
