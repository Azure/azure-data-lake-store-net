using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TestDataCreator;
using Microsoft.Azure.DataLake.Store.FileTransfer;
using Microsoft.Azure.DataLake.Store.FileTransfer.Jobs;

namespace Microsoft.Azure.DataLake.Store.UnitTest
{
    [TestClass]
    public class TransferUnitTest
    {
        private static AdlsClient _adlsClient;
        private static string BasePath;
        private static readonly string LocalRelativePath = @".\" + SdkUnitTest.TestId;
        private static readonly string LocalPath = Directory.GetCurrentDirectory() + @"\" + SdkUnitTest.TestId;
        private static string RemotePath;
        private static readonly string LocalPathUpload1 = $"{LocalPath}\\B";
        private static readonly string LocalPathUpload2 = $"{LocalRelativePath}\\C";
        private static string RemotePathUpload1;
        private static string RemotePathUpload2;
        private static string RemotePathDownload;
        private static readonly string LocalPathDownload = $"{LocalPath}\\A";
        private static int TransferChunkSize = 240 * 1024;
        private static int LowFileSize = 100 * 1024;
        private static int HighFileSize = 500 * 1024;
        private static int DataCreatorBuffSize = 2 * 1024; // At this offset there will be new lines while writing, this should be less than ADlsOutputStream.Buffercapacity.
        private static int CopyBufferSize = 4 * 1024; // Buffer size for FileCopy and ADlsOutputStream, not necessary at all just kept to catch corner cases
        private static int ReadBufferForwardSize = 8; // For nonbinary we read forward, this is the size we should read forward
        private static bool symlinkTestsDisabled = true;
        [ClassInitialize]
        public static void SetupClassTests(TestContext context)
        {
            BasePath = context.Properties["BasePath"].ToString();
            RemotePath ="/" + BasePath+  "/Test1" + SdkUnitTest.TestId;
            RemotePathUpload1 = $"{RemotePath}/Uploader/B";
            RemotePathUpload2 = $"{RemotePath}/Uploader/C";
            RemotePathDownload = $"{RemotePath}/Downloader/A";

            _adlsClient = SdkUnitTest.SetupSuperClient();

            _adlsClient.DeleteRecursive(RemotePath);
            if (!Directory.Exists(LocalPath))
            {
                Directory.CreateDirectory(LocalPath);
            }
            DataCreator.CreateDirRecursiveRemote(_adlsClient, RemotePathDownload, 2, 3, 3, 4, LowFileSize, HighFileSize, true);
            DataCreator.BuffSize = DataCreatorBuffSize;
            
            DataCreator.CreateDirRecursiveLocal(LocalPathUpload1, 1, 3, 3, 4, LowFileSize, HighFileSize, "", true);
            DataCreator.CreateDirRecursiveLocal(LocalPathUpload2, 1, 3, 3, 4, LowFileSize, HighFileSize, "", true);

            // Certain tests are expected to fail over symlink, so we skip them.
            if (BasePath.ToLower().Contains("symlink"))
            {
                symlinkTestsDisabled = false;
            } 
        }

        [TestInitialize]
        public void SetupTest()
        {
            CopyFileJob.ReadForwardBuffSize = ReadBufferForwardSize;
            AdlsOutputStream.BufferMaxCapacity = CopyBufferSize;
            AdlsOutputStream.BufferMinCapacity = 0;
            // Below are the settings that forces download to be chunked for sizes greater than chunk size
            FileDownloader.SkipChunkingWeightThreshold = TransferChunkSize;
            FileDownloader.NumLargeFileThreshold = Int64.MaxValue;
        }

        [TestMethod]
        public void TestUploadNonBinary()
        {
            if (!symlinkTestsDisabled)
            {
                Assert.Inconclusive("Skipping TestUploadNonBinary because concat is not supported over symlink.");
            }
            TransferStatus status = FileUploader.Upload(LocalPathUpload1, RemotePathUpload1, _adlsClient, 10, IfExists.Overwrite, null, false, false, false, false, default(CancellationToken), false, TransferChunkSize);
            Assert.IsTrue(status.EntriesFailed.Count == 0);
            Assert.IsTrue(status.EntriesSkipped.Count == 0);
            long origSuccess = status.FilesTransfered;
            Queue<DirectoryInfo> localQueue = new Queue<DirectoryInfo>();
            Queue<DirectoryEntry> remoteQueue = new Queue<DirectoryEntry>();
            localQueue.Enqueue(new DirectoryInfo(LocalPathUpload1));
            remoteQueue.Enqueue(_adlsClient.GetDirectoryEntry(RemotePathUpload1));
            Verify(localQueue, remoteQueue);
            status = FileUploader.Upload(LocalPathUpload1, RemotePathUpload1, _adlsClient, 10, IfExists.Fail, null, false, false, false, false, default(CancellationToken), false, TransferChunkSize);
            Assert.IsTrue(origSuccess == status.EntriesSkipped.Count);
        }
        [TestMethod]
        public void TestUploadBinary()
        {
            if (!symlinkTestsDisabled)
            {
                Assert.Inconclusive("Skipping TestUploadBinary because concat is not supported over symlink.");
            }
            TransferStatus status = FileUploader.Upload(LocalPathUpload2, RemotePathUpload2, _adlsClient, 10, IfExists.Overwrite, null, false, true, false, true, default(CancellationToken), false, TransferChunkSize);
            Assert.IsTrue(status.EntriesFailed.Count == 0);
            Assert.IsTrue(status.EntriesSkipped.Count == 0);
            long origSuccess = status.FilesTransfered;
            Queue<DirectoryInfo> localQueue = new Queue<DirectoryInfo>();
            Queue<DirectoryEntry> remoteQueue = new Queue<DirectoryEntry>();
            localQueue.Enqueue(new DirectoryInfo(LocalPathUpload2));
            remoteQueue.Enqueue(_adlsClient.GetDirectoryEntry(RemotePathUpload2));
            Verify(localQueue, remoteQueue);
            status = FileUploader.Upload(LocalPathUpload2, RemotePathUpload2, _adlsClient, 10, IfExists.Fail, null, false,false, false, true, default(CancellationToken), false, TransferChunkSize);
            Assert.IsTrue(origSuccess == status.EntriesSkipped.Count);
        }
        [TestMethod]
        public void TestUploadResumeAndDisableTransferLogging()
        {
            try
            {
                _adlsClient.BulkUpload(LocalPathUpload2, RemotePathUpload2, 10, IfExists.Overwrite, true, null, false, true, true, default(CancellationToken));
            }
            catch(ArgumentException ex)
            {
                Assert.IsTrue(ex.Message.Contains("resume and disablelogging both cannot be true"));
            }
        }
        [TestMethod]
        public void TestDownload()
        {
            TransferStatus status = FileDownloader.Download(RemotePathDownload, LocalPathDownload, _adlsClient, 25, IfExists.Overwrite, null, false, false, false, default(CancellationToken), false, 4194304, TransferChunkSize);//,null,IfExists.Overwrite,false,4194304,251658240L,true);
            Assert.IsTrue(status.EntriesFailed.Count == 0);
            Assert.IsTrue(status.EntriesSkipped.Count == 0);
            long origSuccess = status.FilesTransfered;
            Queue<DirectoryInfo> localQueue = new Queue<DirectoryInfo>();
            Queue<DirectoryEntry> remoteQueue = new Queue<DirectoryEntry>();
            localQueue.Enqueue(new DirectoryInfo(LocalPathDownload));
            remoteQueue.Enqueue(_adlsClient.GetDirectoryEntry(RemotePathDownload));
            Verify(localQueue, remoteQueue);
            status = FileDownloader.Download(RemotePathDownload, LocalPathDownload, _adlsClient, 10, IfExists.Fail);
            Assert.IsTrue(origSuccess == status.EntriesSkipped.Count);
        }
        private int Read(Stream stream, byte[] arr)
        {
            int bytesToRead = arr.Length;
            int totBytesRead = 0;
            while (bytesToRead > 0)
            {
                int bytesRead = stream.Read(arr, totBytesRead, bytesToRead);
                if (bytesRead == 0)
                {
                    break;
                }
                bytesToRead -= bytesRead;
                totBytesRead += bytesRead;
            }
            return totBytesRead;
        }

        private bool ByteArrayComparer(byte[] localBuff, byte[] remoteBuff)
        {
            for (int i = 0; i < localBuff.Length; i++)
            {
                if (localBuff[i] != remoteBuff[i])
                {
                    return false;
                }
            }
            return true;
        }
        private void Verify(Queue<DirectoryInfo> localQueue, Queue<DirectoryEntry> remoteQueue)
        {
            while (localQueue.Count > 0 && remoteQueue.Count > 0)
            {
                SortedDictionary<string, FileSystemInfo> localDict = new SortedDictionary<string, FileSystemInfo>();
                DirectoryInfo localDir = localQueue.Dequeue();
                IEnumerable<DirectoryInfo> enumDir = localDir.EnumerateDirectories();
                foreach (var subDir in enumDir)
                {
                    localDict.Add(subDir.Name, subDir);

                }
                IEnumerable<FileInfo> enumFiles = localDir.EnumerateFiles();
                foreach (var file in enumFiles)
                {
                    if (file.Name.EndsWith("-transfer.dat"))
                    {
                        continue;
                    }
                    localDict.Add(file.Name, file);
                }
                int remoteEntries = 0;
                DirectoryEntry remoteDir = remoteQueue.Dequeue();
                var fop = _adlsClient.EnumerateDirectory(remoteDir.FullName);
                foreach (var dir in fop)
                {
                    if (!localDict.ContainsKey(dir.Name))
                    {
                        Assert.Fail(dir.Name + " should exist in local.");
                    }
                    FileSystemInfo localEntry = localDict[dir.Name];
                    remoteEntries++;
                    if (dir.Type == DirectoryEntryType.DIRECTORY)
                    {
                        if (!(localEntry is DirectoryInfo))
                        {
                            Assert.Fail(localEntry + " should also be a directory");
                        }
                        localQueue.Enqueue((DirectoryInfo)localEntry);
                        remoteQueue.Enqueue(dir);
                    }
                    else
                    {
                        if (!(localEntry is FileInfo))
                        {
                            Assert.Fail(localEntry + " should also be a file");
                        }
                        var localFileEntry = (FileInfo)localEntry;
                        using (Stream localStream = localFileEntry.OpenRead(),
                            remoteStream = _adlsClient.GetReadStream(dir.FullName))
                        {
                            Assert.IsTrue(localStream.Length == remoteStream.Length);
                            long lengthToread = localStream.Length;
                            byte[] localBuff = new byte[16 * 1024 * 1024];
                            byte[] remoteBuff = new byte[16 * 1024 * 1024];
                            while (lengthToread > 0)
                            {
                                int localBytesRead = Read(localStream, localBuff);
                                int remoteBytesRead = Read(remoteStream, remoteBuff);
                                Assert.IsTrue(remoteBytesRead == localBytesRead);
                                Assert.IsTrue(ByteArrayComparer(localBuff, remoteBuff));
                                if (localBytesRead == 0 && lengthToread > 0)
                                {
                                    Assert.Fail("Unexpected problem");
                                }
                                lengthToread -= localBytesRead;
                            }
                        }
                    }
                }
                Assert.IsTrue(localDict.Count == remoteEntries);
            }
        }

        [TestCleanup]
        public void CleanupTest()
        {
            CopyFileJob.ReadForwardBuffSize = 8 * 1024;
            AdlsOutputStream.BufferMaxCapacity = 4 * 1024 * 1024;
            AdlsOutputStream.BufferMinCapacity = 1 * 1024 * 1024;
            FileDownloader.SkipChunkingWeightThreshold = 1 * 1024 * 1024 * 1024L;
            FileDownloader.NumLargeFileThreshold = 20;
        }

        [ClassCleanup]
        public static void CleanUpClassTests()
        {
            _adlsClient.DeleteRecursive(RemotePath);
            DataCreator.DeleteRecursiveLocal(new DirectoryInfo(LocalPath));
        }
    }
}
