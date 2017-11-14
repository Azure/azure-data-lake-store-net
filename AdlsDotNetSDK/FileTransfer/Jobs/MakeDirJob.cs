using System;
using System.IO;

namespace Microsoft.Azure.DataLake.Store.FileTransfer.Jobs
{
    /// <summary>
    /// Job to create empty directory
    /// </summary>
    internal class MakeDirJob : BaseJob
    {
        private string SourceDirectoryNm { get; }

        /// <summary>
        /// Directory name
        /// </summary>
        private string DestDirectoryNm { get; }
        /// <summary>
        /// Adls client
        /// </summary>
        private AdlsClient Client { get; }
        /// <summary>
        /// Whether it is uploader or downloader
        /// </summary>
        private bool IsUpload { get; }

        internal MakeDirJob(string sourceDir,string dirNm,AdlsClient client,bool isUpload) : base(0)
        {
            SourceDirectoryNm = sourceDir;
            DestDirectoryNm = dirNm;
            Client = client;
            IsUpload = isUpload;
        }

        protected override object DoJob()
        {
            try
            {
                if (IsUpload)
                {
                    Client.CreateDirectory(DestDirectoryNm);
                }
                else
                {
                    Directory.CreateDirectory(DestDirectoryNm);
                }
                return new SingleEntryTransferStatus(SourceDirectoryNm,DestDirectoryNm, "",EntryType.Directory, SingleChunkStatus.Successful);
            }
            catch(Exception e)
            {
                return new SingleEntryTransferStatus(SourceDirectoryNm,DestDirectoryNm, e.Message, EntryType.Directory, SingleChunkStatus.Failed);
            }
        }

        protected override string JobType()
        {
            return "FileTransfer.MakeDirJob";
        }

        protected override string JobDetails()
        {
            return $"Directory: {DestDirectoryNm}";
        }
    }
}
