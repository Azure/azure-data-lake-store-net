
namespace Microsoft.Azure.DataLake.Store.AclTools
{
    /// <summary>
    /// Acl Processor stats
    /// </summary>
    public class AclProcessorStats
    {
        /// <summary>
        /// Number of files correctly processed
        /// </summary>
        internal long FilesCorrect { get; set; }
        /// <summary>
        /// Number of directories correctly processed
        /// </summary>
        internal long DirectoriesCorrect { get; set; }
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

        internal AclProcessorStats(long fileProcessed, long dirProcessed, long fileCorrect, long dirCorrect):this(fileProcessed,dirProcessed)
        {
            FilesCorrect = fileCorrect;
            DirectoriesCorrect = dirCorrect;
        }

    }
}
