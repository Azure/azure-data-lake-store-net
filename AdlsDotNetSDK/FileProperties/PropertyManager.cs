using System;
using System.IO;
using System.Threading;
using Microsoft.Azure.DataLake.Store.FileProperties.Jobs;
using Microsoft.Azure.DataLake.Store.QueueTools;
using NLog;
using System.Net;
namespace Microsoft.Azure.DataLake.Store.FileProperties
{
    internal class PropertyManager
    {
        private static readonly Logger PropertyLog = LogManager.GetLogger("adls.dotnet.PropertyManager");
        internal static readonly Logger PropertyJobLog = LogManager.GetLogger("adls.dotnet.PropertyManager.Jobs");
        internal AdlsClient Client;
        internal PriorityQueueWrapper<BaseJob> ConsumerQueue;

        private readonly int _numThreads;
        private Thread[] _threadConsumer;
        private Exception _excep;
        internal PropertyTreeNode HeadNode;
        private readonly object _lock = new object();

        internal long MaxDepth;

        internal bool GetSizeProperty;
        internal bool DisplayFiles;

        internal bool GetAclProperty;
        internal bool DisplayConsistentAclTree;

        internal bool SaveToLocal;
        internal string DumpFileName;

        internal StreamWriter PropertyDumpWriter;

        internal bool DontDeleteChildNodes;
        private const char OuputLineSeparator = '\t';
        private void SetException(Exception ex)
        {
            lock (_lock)
            {
                _excep = ex;
            }
        }

        private Exception GetException()
        {
            lock (_lock)
            {
                return _excep;
            }
        }

        private PropertyManager(AdlsClient client, bool getAclProperty, bool getDiskUsage, string saveFileName, bool saveToLocal, int numThreads, bool displayFiles, bool displayConsistentAcl, long maxDepth)
        {
            Client = client;
            SaveToLocal = saveToLocal;
            if (string.IsNullOrEmpty(saveFileName))
            {
                throw new ArgumentNullException(nameof(saveFileName));
            }
            DumpFileName = saveFileName;
            GetSizeProperty = getDiskUsage;
            GetAclProperty = getAclProperty;
            _numThreads = numThreads < 0 ? AdlsClient.DefaultNumThreads : numThreads;
            DisplayFiles = displayFiles;
            DisplayConsistentAclTree = GetAclProperty && displayConsistentAcl;
            MaxDepth = maxDepth;
            ConsumerQueue = new PriorityQueueWrapper<BaseJob>(_numThreads);
            Stream underLyingStream;
            if (saveToLocal)
            {
                Directory.CreateDirectory(Path.GetDirectoryName(DumpFileName));
                underLyingStream = new FileStream(DumpFileName, FileMode.Create, FileAccess.ReadWrite);
            }
            else
            {
                underLyingStream = client.CreateFile(DumpFileName, IfExists.Overwrite);
            }
            PropertyDumpWriter = new StreamWriter(underLyingStream);
            WriteHeader();
        }

        private void WriteHeader()
        {
            string output = "";
            if (GetSizeProperty)
            {
                output =
                    $"Total size of direct child files and directories{OuputLineSeparator}Total number of direct files{OuputLineSeparator}Total number of direct directories{OuputLineSeparator}Total size{OuputLineSeparator}Total number of files{OuputLineSeparator}Total number of directories";
            }
            if (GetAclProperty)
            {
                output +=
                        $"{(string.IsNullOrEmpty(output) ? "" : $"{OuputLineSeparator}")}Acl Entries{OuputLineSeparator}Whether Acl is same for all descendants";
            }
            lock (PropertyDumpWriter)
            {
                PropertyDumpWriter.WriteLine($"Entry name{OuputLineSeparator}{output}");
            }
        }
        // Kept this in this class so that others can use it
        internal void WritePropertyTreeNodeToFile(PropertyTreeNode node)
        {
            string output = "";
            if (GetSizeProperty)
            {
                output =
                    $"{node.DirectChildSize}{OuputLineSeparator}{node.DirectChildFiles}{OuputLineSeparator}{node.DirectChildDirec}{OuputLineSeparator}{node.TotChildSize}{OuputLineSeparator}{node.TotChildFiles}{OuputLineSeparator}{node.TotChildDirec}";
            }
            if (GetAclProperty)
            {
                if (!node.SkipAclOutput)
                {
                    output +=
                        $"{(string.IsNullOrEmpty(output) ? "" : $"{OuputLineSeparator}")}{string.Join("|", node.Acls.Entries)}{OuputLineSeparator}{node.AllChildSameAcl}";
                }
            }
            lock (PropertyDumpWriter)
            {
                PropertyDumpWriter.WriteLine($"{node.FullPath}{OuputLineSeparator}{output}");
            }
        }
        private PropertyTreeNode RunGetProperty(string path)
        {
            try
            {
                if (PropertyLog.IsDebugEnabled)
                {
                    PropertyLog.Debug($"FileProperty, SourcePath: {path}, GetDiskUsage: {GetSizeProperty}, GetAclProperty: {GetAclProperty}{(GetAclProperty ? $", AclConsistency: {DisplayConsistentAclTree}" : string.Empty)}, SaveToLocal: {SaveToLocal}");
                }
                // Create DumpFile
                var dir = Client.GetDirectoryEntry(path); // If the path does not exist then it will throw an exception
                HeadNode = new PropertyTreeNode(dir.FullName, dir.Type, dir.Length, null, DisplayFiles || GetAclProperty);
                if (dir.Type == DirectoryEntryType.FILE)
                {
                    WritePropertyTreeNodeToFile(HeadNode);
                    return HeadNode;
                }
                ConsumerQueue.Add(new EnumerateAndGetPropertyJob(HeadNode, this));
                _threadConsumer = new Thread[_numThreads];
                for (int i = 0; i < _numThreads; i++)
                {
                    _threadConsumer[i] = new Thread(ConsumerRun);
                    _threadConsumer[i].Start();
                }
                for (int i = 0; i < _numThreads; i++)
                {
                    _threadConsumer[i].Join();
                }

                if (GetException() != null)
                {
                    throw GetException();
                }
                WritePropertyTreeNodeToFile(HeadNode);
                return HeadNode;
            }
            finally
            {
                PropertyDumpWriter.Dispose();
            }
        }
        // For testing purposes, makes sure child nodes are not deleted after written to file
        private PropertyTreeNode RunTestGetProperty(string path)
        {
            DontDeleteChildNodes = true;
            return RunGetProperty(path);
        }
        // Unit test purpose
        internal static PropertyTreeNode TestGetProperty(string path, AdlsClient client, bool getDiskUsage,
            bool getAclProperty, string dumpFileName, bool saveToLocal, int numThreads = -1, bool displayFiles = false,
            bool hideConsistentAcl = true, long maxDepth = Int64.MaxValue)
        {
            return new PropertyManager(client, getAclProperty, getDiskUsage, dumpFileName, saveToLocal, numThreads, displayFiles, hideConsistentAcl, maxDepth).RunTestGetProperty(path);
        }
        /// <summary>
        /// Dumps file property to a local or adl file
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="client">Adls client</param>
        /// <param name="getAclProperty">True if we want Acl usage</param>
        /// <param name="getDiskUsage">True if we want disk usage</param>
        /// <param name="dumpFileName">Filename containing the dump</param>
        /// <param name="saveToLocal">True if we want to save to local file</param>
        /// <param name="numThreads">Number of threads</param>
        /// <param name="displayFiles">True if we want to display properties of files</param>
        /// <param name="hideConsistentAcl">True if we want to view consistent acl property only</param>
        /// <param name="maxDepth">Maximum depth till which we want to view the properties</param>
        internal static PropertyTreeNode GetFileProperty(string path, AdlsClient client, bool getAclProperty, bool getDiskUsage, string dumpFileName, bool saveToLocal, int numThreads = -1, bool displayFiles = false, bool hideConsistentAcl = true, long maxDepth = Int64.MaxValue)
        {
            return new PropertyManager(client, getAclProperty, getDiskUsage, dumpFileName, saveToLocal, numThreads, displayFiles, hideConsistentAcl, maxDepth).RunGetProperty(path);
        }
        private void ConsumerRun()
        {
            while (true)
            {
                var job = ConsumerQueue.Poll();
                if (GetException() != null || job == null || job is PoisonJob)
                {
                    ConsumerQueue.Add(new PoisonJob());
                    return;
                }
                try
                {
                    job.DoRun(PropertyJobLog);
                }
                catch (AdlsException ex)
                {
                    if (ex.HttpStatus != HttpStatusCode.NotFound)//Do not stop acl processor if the file/directory is deleted
                    {
                        SetException(ex);//Sets the global exception to signal other threads to close
                        ConsumerQueue.Add(new PoisonJob());//Handle corner cases like when exception is raised other threads can be in wait state
                        return;
                    }
                }
                catch (Exception ex)
                {
                    SetException(ex);
                    ConsumerQueue.Add(new PoisonJob());//Handle corner cases like when exception is raised other threads can be in wait state
                    return;
                }
            }
        }
    }
}
