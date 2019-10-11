
using System;
using System.Collections.Concurrent;

namespace Microsoft.Azure.DataLake.Store.AclTools
{
    /// <summary>
    /// Acl Processor stats
    /// </summary>
    public class AclProcessorStats
    {
        /// <summary>
        /// List of directories incorrectly processed
        /// </summary>
        internal long IncorrectDirectoryCount { get; set; }
        /// <summary>
        /// List of files incorrectly processed
        /// </summary>
        internal long IncorrectFileCount { get; set; }
        
        /// <summary>
        /// Number of files processed
        /// </summary>
        public long FilesProcessed { get; internal set; }
        /// <summary>
        /// Number of directories processed
        /// </summary>
        public long DirectoryProcessed { get; internal set; }

        /// <summary>
        /// Array of links found
        /// </summary>
        internal string[] LinkPaths { get; set; }

        internal AclProcessorStats(long fileProcessed, long dirProcessed, long fileIncorrect, long dirIncorrect, ConcurrentBag<string> linkPaths)
        {
            FilesProcessed = fileProcessed;
            DirectoryProcessed = dirProcessed;
            IncorrectFileCount = fileIncorrect;
            IncorrectDirectoryCount = dirIncorrect;
            LinkPaths = linkPaths.ToArray();
        }

    }
}
