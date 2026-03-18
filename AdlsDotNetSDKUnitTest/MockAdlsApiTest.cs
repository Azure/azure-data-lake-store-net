using Microsoft.Azure.DataLake.Store.AclTools;
using Microsoft.Azure.DataLake.Store.AclTools.Jobs;
using Microsoft.Azure.DataLake.Store.MockAdlsFileSystem;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace Microsoft.Azure.DataLake.Store.UnitTest
{
    /// <summary>
    /// Tests retry mechanisms, token cancellation, Account validation and Acl serializing deserializing
    /// </summary>
    [TestClass]
    public class MockAdlsApiTest{
        private static AdlsClient _adlsClient = MockAdlsFileSystem.MockAdlsClient.GetMockClient();
        private static string BasePath;
        private static string rootPath;
        private static string TestString = "Hello";
        private static Random random = new Random();

        [ClassInitialize]
        public static void SetupClient(TestContext context)
        {
            BasePath = context.Properties["BasePath"].ToString();
            rootPath  = "/" + BasePath + "/a";
            _adlsClient.CreateDirectory(rootPath);
            _adlsClient.CreateDirectory(rootPath+"/b0");
            var testByte= Encoding.UTF8.GetBytes(TestString);
            using(var stream = _adlsClient.CreateFile(rootPath+"/bFile01", IfExists.Overwrite)){
                stream.Write(testByte,0, testByte.Length);
            }
            _adlsClient.CreateDirectory(rootPath+"/b0/c0");
        }

        private Tuple<EnumerateDeletedItemsProgress, Progress<EnumerateDeletedItemsProgress>> GetProgressTracker()
        {
            var progressTracker = new Progress<EnumerateDeletedItemsProgress>();
            EnumerateDeletedItemsProgress progress = new EnumerateDeletedItemsProgress();

            progressTracker.ProgressChanged += (s, e) =>
            {
                lock (progress)
                {
                    progress.NumSearched = e.NumSearched;
                    progress.NumFound = e.NumFound;
                    progress.NextListAfter = e.NextListAfter;
                }
            };
            return Tuple.Create<EnumerateDeletedItemsProgress, Progress<EnumerateDeletedItemsProgress>>(progress, progressTracker);
        }
 
        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestMockClientAccountValidation()
        {
            MockAdlsClient.GetMockClient("rdutta.azuredatalakestore.net/");
        }

        [TestMethod]
        public void TestModifyAndRemoveAclRecursively()
        {
            var acls=FilePropertiesUnitTest.GetAclEntryForModifyAndRemove();
            var stats = _adlsClient.ChangeAcl(rootPath, acls, RequestedAclType.ModifyAcl, 1, null, default(CancellationToken));
            Assert.IsTrue(stats.DirectoryProcessed == 3);
            Assert.IsTrue(stats.FilesProcessed == 1);
            Assert.IsTrue(VerifyChangeAclJob.CheckAclListContains(_adlsClient.GetAclStatus(rootPath).Entries, acls));
            Assert.IsTrue(VerifyChangeAclJob.CheckAclListContains(_adlsClient.GetAclStatus(rootPath + "/b0/c0").Entries, acls));
            Assert.IsTrue(VerifyChangeAclJob.CheckAclListContains(_adlsClient.GetAclStatus(rootPath + "/bFile01").Entries, acls));
            stats = _adlsClient.ChangeAcl(rootPath, acls, RequestedAclType.RemoveAcl, 1, null, default(CancellationToken));
            Assert.IsTrue(stats.DirectoryProcessed == 3);
            Assert.IsTrue(stats.FilesProcessed == 1);
            Assert.IsTrue(VerifyChangeAclJob.CheckAclListContains(_adlsClient.GetAclStatus(rootPath).Entries, acls, true));
            Assert.IsTrue(VerifyChangeAclJob.CheckAclListContains(_adlsClient.GetAclStatus(rootPath + "/b0/c0").Entries, acls, true));
            Assert.IsTrue(VerifyChangeAclJob.CheckAclListContains(_adlsClient.GetAclStatus(rootPath + "/bFile01").Entries, acls, true));
        }

        [TestMethod]
        public void TestGetContentSummary()
        {
            var summary= _adlsClient.GetContentSummary(rootPath);
            Assert.IsTrue(summary.DirectoryCount == 2);
            Assert.IsTrue(summary.FileCount == 1);
            Assert.IsTrue(summary.Length == TestString.Length);
        }

        [TestMethod]
        public void TestExportFileProperties()
        {
            var acls = FilePropertiesUnitTest.GetAclEntryForSet();
            var stats = _adlsClient.ChangeAcl(rootPath, acls, RequestedAclType.SetAcl);
            _adlsClient.GetFileProperties(rootPath, true, @"C:\Data\logFile");
            Assert.IsTrue(File.Exists(@"C:\Data\logFile"));
            _adlsClient.GetFileProperties(rootPath, true, "/Data/logFile", true, false);
            Assert.IsTrue(_adlsClient.GetDirectoryEntry("/Data/logFile") != null);
        }

        [TestMethod]
        public void TestCreateWithOverwrite()
        {
            var pathname = "/filename";
            _adlsClient.CreateFile(pathname, IfExists.Fail);
            Assert.IsTrue(_adlsClient.GetDirectoryEntry(pathname) != null);
            _adlsClient.CreateFile(pathname, IfExists.Overwrite);
            Assert.IsTrue(_adlsClient.GetDirectoryEntry(pathname) != null);
        }

        [TestMethod]
        public void TestEnumerateDeletedItems()
        {
            TestEnumerateDeletedItemsParameterized(DirectoryEntryType.FILE);
            TestEnumerateDeletedItemsParameterized(DirectoryEntryType.DIRECTORY);
        }

        [TestMethod]
        public void TestRestoreDeletedItems()
        {
            TestRestoreDeletedItemsParameterized(DirectoryEntryType.FILE);
            TestRestoreDeletedItemsParameterized(DirectoryEntryType.DIRECTORY);
        }

        private void TestRestoreDeletedItemsParameterized(DirectoryEntryType type)
        {
            int N = 5;
            string prefix = "item" + random.Next(100000000) + "_";

            List<string> names = new List<string>();
            for (int i = 0; i < N; i++)
            {
                names.Add(prefix + random.Next(100000000));
            }

            foreach (var name in names)
            {
                var path = "/" + name;
                if (type == DirectoryEntryType.FILE)
                {
                    _adlsClient.CreateFile(path, IfExists.Fail);
                }
                else
                {
                    _adlsClient.CreateDirectory(path);
                }

                Assert.IsTrue(_adlsClient.GetDirectoryEntry(path) != null);

                Assert.IsTrue(_adlsClient.Delete(path));
            }

            foreach (var name in names)
            {
                var tuple = GetProgressTracker();
                var trashEntries = _adlsClient.EnumerateDeletedItems(name, "", 100, tuple.Item2);
                _adlsClient.RestoreDeletedItems(trashEntries.ElementAt(0).TrashDirPath, trashEntries.ElementAt(0).OriginalPath,
                    (type == DirectoryEntryType.FILE ? "file" : "folder"));
            }
        }

        private void TestEnumerateDeletedItemsParameterized(DirectoryEntryType type)
        {
            int N = 5;
            string prefix = "item" + random.Next(100000000) + "_";

            List<string> names = new List<string>();
            for (int i = 0; i < N; i++)
            {
                names.Add(prefix + random.Next(100000000));
            }

            foreach (var name in names)
            {
                var path = "/" + name;
                if(type == DirectoryEntryType.FILE)
                {
                    _adlsClient.CreateFile(path, IfExists.Fail);
                }
                else
                {
                    _adlsClient.CreateDirectory(path);
                }
                
                Assert.IsTrue(_adlsClient.GetDirectoryEntry(path) != null);

                Assert.IsTrue(_adlsClient.Delete(path));
            }

            // Search all items in one shot
            var tuple = GetProgressTracker();
            IEnumerable<TrashEntry> trashEntries = _adlsClient.EnumerateDeletedItems(prefix, "", 100, tuple.Item2);
            Thread.Sleep(500);
            Assert.IsTrue(tuple.Item1.NumFound == names.Count);
            Assert.IsTrue(tuple.Item1.NumSearched >= names.Count);
            Assert.IsTrue(trashEntries.Count() == names.Count);

            // Search individual items
            foreach (var name in names)
            {
                tuple = GetProgressTracker();
                trashEntries = _adlsClient.EnumerateDeletedItems(name, "", 100, tuple.Item2);
                Thread.Sleep(500);
                Assert.IsTrue(tuple.Item1.NumFound == 1);
                Assert.IsTrue(tuple.Item1.NumSearched >= names.Count);
                Assert.IsTrue(trashEntries.Count() == 1);
                Assert.IsTrue(trashEntries.ElementAt(0).Type == (type == DirectoryEntryType.FILE ? TrashEntryType.FILE : TrashEntryType.DIRECTORY));
            }

            // Search all items, returning 1 at a time
            string listAfter = "";
            for(int i = 0;i < names.Count; i++)
            {
                tuple = GetProgressTracker();
                trashEntries = _adlsClient.EnumerateDeletedItems(prefix, listAfter, 1, tuple.Item2);
                Thread.Sleep(500);
                Assert.IsTrue(tuple.Item1.NumFound == 1);
                Assert.IsTrue(trashEntries.Count() == 1);
                listAfter = tuple.Item1.NextListAfter;
            }
        }
    }
}