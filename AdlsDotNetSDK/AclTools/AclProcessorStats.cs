
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
        internal int FilesCorrect { get; set; }
        /// <summary>
        /// Number of directories correctly processed
        /// </summary>
        internal int DirectoriesCorrect { get; set; }
        /// <summary>
        /// Number of files processed
        /// </summary>
        public int FilesProcessed { get; internal set; }
        /// <summary>
        /// Number of directories processed
        /// </summary>
        public int DirectoryProcessed { get; internal set; }

        internal AclProcessorStats(int fileProcessed, int dirProcessed)
        {
            FilesProcessed = fileProcessed;
            DirectoryProcessed = dirProcessed;
        }

        internal AclProcessorStats(int fileProcessed, int dirProcessed,int fileCorrect,int dirCorrect):this(fileProcessed,dirProcessed)
        {
            FilesCorrect = fileCorrect;
            DirectoriesCorrect = dirCorrect;
        }

    }
}
