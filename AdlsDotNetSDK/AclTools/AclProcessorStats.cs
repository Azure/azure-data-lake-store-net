
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

        public AclProcessorStats(long fileProcessed, long dirProcessed)
        {
            FilesProcessed = fileProcessed;
            DirectoryProcessed = dirProcessed;
        }

        internal AclProcessorStats(long fileProcessed, long dirProcessed, long fileIncorrect, long dirIncorrect):this(fileProcessed,dirProcessed)
        {
            IncorrectFileCount = fileIncorrect;
            IncorrectDirectoryCount = dirIncorrect;
        }

    }
}
