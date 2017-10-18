using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TestDataCreator;
using Microsoft.Azure.DataLake.Store.FileTransfer;

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
            DataCreator.CreateDirRecursiveLocal(localPath + "\\B", 1, 3, 4, 4, 50, 500);
            DataCreator.CreateDirRecursiveRemote(_adlsClient,remotePathDownload,1,3,2,2,200,500);
        }
        
        [TestMethod]
        public void TestUpload()
        {
            TransferStatus status = FileUploader.Upload(localPath + "\\B", remotePath, _adlsClient, 10);
            Console.WriteLine("BytesTransferred: " + status.SizeTransfered);
            Console.WriteLine("FilesTransferred: " + status.FilesTransfered);
            Console.WriteLine("ChunksTransfered: " + status.ChunksTransfered);
            Console.WriteLine("NonChunksTransfered: " + status.NonChunkedFileTransferred);
            Console.WriteLine("DirectoriesTransfered: " + status.DirectoriesTransferred);
            Console.WriteLine("SkiipedEntries: " + status.EntriesSkipped.Count);
            Assert.IsTrue(status.EntriesFailed.Count == 0);
            Assert.IsTrue(status.EntriesSkipped.Count == 0);
            long origSuccess = status.FilesTransfered;
            Queue<DirectoryInfo> localQueue = new Queue<DirectoryInfo>();
            Queue<DirectoryEntry> remoteQueue = new Queue<DirectoryEntry>();
            localQueue.Enqueue(new DirectoryInfo(localPath + "\\B"));
            remoteQueue.Enqueue(_adlsClient.GetDirectoryEntry(remotePath));
            Verify(localQueue, remoteQueue);
            status = FileUploader.Upload(localPath + "\\B", remotePath, _adlsClient, 10,IfExists.Fail,null);
            Console.WriteLine(origSuccess);
            Console.WriteLine(status.EntriesSkipped.Count);
            Assert.IsTrue(origSuccess==status.EntriesSkipped.Count);
        }
        
        [TestMethod]
        public void TestDownload()
        {
            TransferStatus status = FileDownloader.Download(remotePathDownload, localPathDownload, _adlsClient, 10);//,null,IfExists.Overwrite,false,4194304,251658240L,true);
            Console.WriteLine("BytesTransferred: " + status.SizeTransfered);
            Console.WriteLine("FilesTransferred: " + status.FilesTransfered);
            Console.WriteLine("ChunksTransfered: " + status.ChunksTransfered);
            Console.WriteLine("DirectoriesTransfered: " + status.DirectoriesTransferred);
            Assert.IsTrue(status.EntriesFailed.Count == 0);
            Assert.IsTrue(status.EntriesSkipped.Count == 0);
            long origSuccess = status.FilesTransfered;
            Queue<DirectoryInfo> localQueue = new Queue<DirectoryInfo>();
            Queue<DirectoryEntry> remoteQueue = new Queue<DirectoryEntry>();
            localQueue.Enqueue(new DirectoryInfo(localPathDownload));
            remoteQueue.Enqueue(_adlsClient.GetDirectoryEntry(remotePathDownload));
            Verify(localQueue, remoteQueue);
            status = FileDownloader.Download(remotePathDownload, localPathDownload, _adlsClient, 10,IfExists.Fail,null);
            Assert.IsTrue(origSuccess==status.EntriesSkipped.Count);
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
            return StructuralComparisons.StructuralEqualityComparer.Equals(localBuff, remoteBuff);
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
