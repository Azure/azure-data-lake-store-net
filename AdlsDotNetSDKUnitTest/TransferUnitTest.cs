using System;
using System.Collections;
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
        private static string localPath = "C:\\Data";
        private static string remotePath = "/Test1/Uploader";
        private static string remotePathDownload = "/Test1/Downloader/A";
        private static string localPathDownload = "C:\\Data\\A";
        private static int TransferChunkSize = 240 * 1024;
        private static int LowFileSize = 100 * 1024;
        private static int HighFileSize = 500 * 1024;
        private static int DataCreatorBuffSize = 2 * 1024; // At this offset there will be new lines while writing, this should be less than ADlsOutputStream.Buffercapacity.
        private static int CopyBufferSize = 4 * 1024; // Buffer size for FileCopy and ADlsOutputStream, not necessary at all just kept to catch corner cases
        private static int ReadBufferForwardSize = 8; // For nonbinary we read forward, this is the size we should read forward
        [ClassInitialize]
        public static void SetupClient(TestContext context)
        {
            _adlsClient = SdkUnitTest.SetupSuperClient();

            _adlsClient.DeleteRecursive("/Test1");
            _adlsClient.CreateDirectory(remotePath, "775");
            if (!Directory.Exists(localPath))
            {
                Directory.CreateDirectory(localPath);
            }
            DataCreator.CreateDirRecursiveRemote(_adlsClient, remotePathDownload, 2, 3, 3, 4, LowFileSize, HighFileSize, true);
            DataCreator.BuffSize = DataCreatorBuffSize;
            CopyFileJob.ReadForwardBuffSize = ReadBufferForwardSize;
            AdlsOutputStream.BufferCapacity = CopyBufferSize;
            // Below are the settings that forces download to be chunked for sizes greater than chunk size
            FileDownloader.SkipChunkingWeightThreshold = TransferChunkSize;
            FileDownloader.NumLargeFileThreshold = Int64.MaxValue;

            DataCreator.CreateDirRecursiveLocal(localPath + "\\B", 1, 3, 3, 4, LowFileSize, HighFileSize, "", true);
            DataCreator.CreateDirRecursiveLocal(localPath + "\\C", 1, 3, 3, 4, LowFileSize, HighFileSize, "", true);

        }

        [TestMethod]
        public void TestUploadNonBinary()
        {
            TransferStatus status = FileUploader.Upload(localPath + "\\B", remotePath, _adlsClient, 10, IfExists.Overwrite, null, false, false, false, default(CancellationToken), false, TransferChunkSize);
            Assert.IsTrue(status.EntriesFailed.Count == 0);
            Assert.IsTrue(status.EntriesSkipped.Count == 0);
            long origSuccess = status.FilesTransfered;
            Queue<DirectoryInfo> localQueue = new Queue<DirectoryInfo>();
            Queue<DirectoryEntry> remoteQueue = new Queue<DirectoryEntry>();
            localQueue.Enqueue(new DirectoryInfo(localPath + "\\B"));
            remoteQueue.Enqueue(_adlsClient.GetDirectoryEntry(remotePath));
            Verify(localQueue, remoteQueue);
            status = FileUploader.Upload(localPath + "\\B", remotePath, _adlsClient, 10, IfExists.Fail, null, false, false, false, default(CancellationToken), false, TransferChunkSize);
            Assert.IsTrue(origSuccess == status.EntriesSkipped.Count);
        }
        [TestMethod]
        public void TestUploadBinary()
        {
            TransferStatus status = FileUploader.Upload(localPath + "\\B", remotePath, _adlsClient, 10, IfExists.Overwrite, null, false, false, true, default(CancellationToken), false, TransferChunkSize);
            Assert.IsTrue(status.EntriesFailed.Count == 0);
            Assert.IsTrue(status.EntriesSkipped.Count == 0);
            long origSuccess = status.FilesTransfered;
            Queue<DirectoryInfo> localQueue = new Queue<DirectoryInfo>();
            Queue<DirectoryEntry> remoteQueue = new Queue<DirectoryEntry>();
            localQueue.Enqueue(new DirectoryInfo(localPath + "\\B"));
            remoteQueue.Enqueue(_adlsClient.GetDirectoryEntry(remotePath));
            Verify(localQueue, remoteQueue);
            status = FileUploader.Upload(localPath + "\\B", remotePath, _adlsClient, 10, IfExists.Fail, null, false, false, true, default(CancellationToken), false, TransferChunkSize);
            Assert.IsTrue(origSuccess == status.EntriesSkipped.Count);
        }
        [TestMethod]
        public void TestDownload()
        {
            TransferStatus status = FileDownloader.Download(remotePathDownload, localPathDownload, _adlsClient, 25, IfExists.Overwrite, null, false, false, default(CancellationToken), false, 4194304, TransferChunkSize);//,null,IfExists.Overwrite,false,4194304,251658240L,true);
            Assert.IsTrue(status.EntriesFailed.Count == 0);
            Assert.IsTrue(status.EntriesSkipped.Count == 0);
            long origSuccess = status.FilesTransfered;
            Queue<DirectoryInfo> localQueue = new Queue<DirectoryInfo>();
            Queue<DirectoryEntry> remoteQueue = new Queue<DirectoryEntry>();
            localQueue.Enqueue(new DirectoryInfo(localPathDownload));
            remoteQueue.Enqueue(_adlsClient.GetDirectoryEntry(remotePathDownload));
            Verify(localQueue, remoteQueue);
            status = FileDownloader.Download(remotePathDownload, localPathDownload, _adlsClient, 10, IfExists.Fail, null);
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
            //bool flag = true;
            int lastnotEqual = 0;
            for (int i = 0; i < localBuff.Length; i++)
            {
                if (localBuff[i] != remoteBuff[i])
                {
                    return false;
                }
            }
            return true;
            //return StructuralComparisons.StructuralEqualityComparer.Equals(localBuff, remoteBuff);
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
        [ClassCleanup]
        public static void CleanTests()
        {
            _adlsClient.DeleteRecursive("/Test1");
            DataCreator.DeleteRecursiveLocal(new DirectoryInfo(localPath));
        }
    }
}
