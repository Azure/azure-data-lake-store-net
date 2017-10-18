using System;
using System.IO;

namespace Microsoft.Azure.DataLake.Store.FileTransfer.Jobs
{
    /// <summary>
    /// Job to create empty directory
    /// </summary>
    internal class MakeDirJob : Job
    {
        /// <summary>
        /// Directory name
        /// </summary>
        private string DirectoryNm { get; }
        /// <summary>
        /// Adls client
        /// </summary>
        private AdlsClient Client { get; }
        /// <summary>
        /// Whether it is uploader or downloader
        /// </summary>
        private bool IsUpload { get; }

        internal MakeDirJob(string dirNm,AdlsClient client,bool isUpload) : base(0)
        {
            DirectoryNm = dirNm;
            Client = client;
            IsUpload = isUpload;
        }

        protected override SingleEntryTransferStatus DoJob()
        {
            try
            {
                if (IsUpload)
                {
                    Client.CreateDirectory(DirectoryNm);
                }
                else
                {
                    Directory.CreateDirectory(DirectoryNm);
                }
                return new SingleEntryTransferStatus(DirectoryNm, "", EntryType.Directory, SingleChunkStatus.Successful);
            }
            catch(Exception e)
            {
                return new SingleEntryTransferStatus(DirectoryNm, e.Message, EntryType.Directory, SingleChunkStatus.Failed);
            }
        }

        protected override string JobType()
        {
            return "MakeDirJob";
        }

        protected override string JobDetails()
        {
            return $"Directory: {DirectoryNm}";
        }
    }
}
