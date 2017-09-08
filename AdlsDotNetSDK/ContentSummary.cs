namespace Microsoft.Azure.DataLake.Store
{
    /// <summary>
    /// Encapsulates total directory count, file count, space consumed
    /// </summary>
    public class ContentSummary
    {
        /// <summary>
        /// Total directory count
        /// </summary>
        public long DirectoryCount { get; internal set; }
        /// <summary>
        /// Total file count
        /// </summary>
        public long FileCount { get; internal set; }
        /// <summary>
        /// Total file sizes
        /// </summary>
        public long Length { get; internal set; }
        /// <summary>
        /// Total space consumed
        /// </summary>
        public long SpaceConsumed { get; internal set; }

        internal ContentSummary(long directoryCnt, long fileCnt, long length, long spaceConsumed)
        {
            DirectoryCount = directoryCnt;
            FileCount = fileCnt;
            Length = length;
            SpaceConsumed = spaceConsumed;
        }
    }
}
