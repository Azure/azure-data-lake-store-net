using System.Collections.Generic;
using System.IO;
using System.Threading;
using Microsoft.Azure.DataLake.Store.Acl;
using Microsoft.Azure.DataLake.Store.AclTools;
using Microsoft.Azure.DataLake.Store.FileProperties;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TestDataCreator;

namespace Microsoft.Azure.DataLake.Store.UnitTest
{
    /// <summary>
    /// Unit test to test recursive Acl change, disk usage dump and Acl dump
    /// </summary>
    [TestClass]
    public class FilePropertiesUnitTest
    {
        /// <summary>
        /// Adls Client
        /// </summary>
        private static AdlsClient _adlsClient;

        private static readonly string RemotePath = "/Test2" + SdkUnitTest.TestId;
        /// <summary>
        /// Unittest path
        /// </summary>
        private static readonly string UnitTestPath = $"{RemotePath}/RecursiveAcl/A";

        private static readonly string LocalPath = @"C:\Data1" + SdkUnitTest.TestId;
        /// <summary>
        /// Semaphore to serialize acl modify and remove
        /// </summary>
        private static readonly Semaphore SemModifyAcl = new Semaphore(1, 1);

        /// <summary>
        /// Semaphore to serialize acl modify and remove
        /// </summary>
        private static readonly Semaphore SemSetAcl = new Semaphore(1, 1);

        private static int _oneLevelDirCount;
        private static int _oneLevelFileCnt;
        private static int _recurseLevel;
        private static int _oneFileSize;
        private static bool _modifyAclRun;
        private static bool _setAclRun;
        [ClassInitialize]
        public static void SetupClient(TestContext context)
        {
            _adlsClient = SdkUnitTest.SetupSuperClient();

            _adlsClient.DeleteRecursive(RemotePath);
            _oneLevelDirCount = 3;
            _oneLevelFileCnt = 2;
            _recurseLevel = 3;
            _oneFileSize = 100;

            DataCreator.CreateDirRecursiveRemote(_adlsClient, UnitTestPath, _recurseLevel, _oneLevelDirCount, _oneLevelFileCnt, _oneLevelFileCnt, _oneFileSize, _oneFileSize);
        }

        private static void GetExpectedOutput(int oneLevelDirecCnt, int oneLevelFileCnt, int recurseLevel,
            int oneFileSize, out int expectedFileCount, out int expectedDirCount, out int expectedFileSize)
        {
            expectedDirCount = 0;
            int power = 1;
            for (int i = 1; i <= recurseLevel; i++)
            {
                power *= oneLevelDirecCnt;
                expectedDirCount += power;
            }
            // last level directory does have files
            expectedFileCount = (expectedDirCount+1) * oneLevelFileCnt;
            expectedFileSize = expectedFileCount * oneFileSize;
        }
        
        /// <summary>
        /// Gets a sample Acl entries - For modify and remove
        /// </summary>
        /// <returns></returns>
        private static List<AclEntry> GetAclEntryForModifyAndRemove()
        {
            return new List<AclEntry>()
            {
                new AclEntry(AclType.user, SdkUnitTest.NonOwner1ObjectId, AclScope.Access, AclAction.ReadWrite),
                new AclEntry(AclType.user, SdkUnitTest.NonOwner2ObjectId, AclScope.Access, AclAction.WriteOnly),
                new AclEntry(AclType.user, SdkUnitTest.Group1Id, AclScope.Default, AclAction.WriteExecute)
            };
        }
        /// <summary>
        /// Gets a sample acl entries
        /// </summary>
        /// <returns></returns>
        private static List<AclEntry> GetAclEntryForSet()
        {
            return new List<AclEntry>()
            {
                new AclEntry(AclType.user, SdkUnitTest.NonOwner1ObjectId, AclScope.Access, AclAction.ReadOnly),
                new AclEntry(AclType.user, SdkUnitTest.NonOwner2ObjectId, AclScope.Access, AclAction.ExecuteOnly)
            };
        }
        /// <summary>
        /// Tests Acl modify
        /// </summary>
        [TestMethod]
        public void TestModifyAcl()
        {
            SemModifyAcl.WaitOne();
            if (_modifyAclRun)
            {
                SemModifyAcl.Release();
                return;
            }
            List<AclEntry> aclEntries1 = GetAclEntryForModifyAndRemove();
            var aclStats = AclProcessor.RunAclProcessor(UnitTestPath + "/B0", _adlsClient, aclEntries1,
                RequestedAclType.ModifyAcl, 25);
            var aclVerifyStats =
                AclProcessor.RunAclVerifier(UnitTestPath + "/B0", _adlsClient, aclEntries1, RequestedAclType.ModifyAcl, 25);
            Assert.IsTrue(aclStats.FilesProcessed == aclVerifyStats.FilesProcessed);
            Assert.IsTrue(aclStats.DirectoryProcessed == aclVerifyStats.DirectoryProcessed);
            Assert.IsTrue(aclStats.FilesProcessed == aclVerifyStats.FilesCorrect);
            Assert.IsTrue(aclStats.DirectoryProcessed == aclVerifyStats.DirectoriesCorrect);
            _modifyAclRun = true;
            SemModifyAcl.Release();
        }
        /// <summary>
        /// Tests remove acl
        /// </summary>
        [TestMethod]
        public void RemoveAcl()
        {
            TestModifyAcl();
            List<AclEntry> aclEntries1 = GetAclEntryForModifyAndRemove();
            var aclStats = AclProcessor.RunAclProcessor(UnitTestPath + "/B0", _adlsClient, aclEntries1,
                RequestedAclType.RemoveAcl, 10);
            var aclVerifyStats =
                AclProcessor.RunAclVerifier(UnitTestPath + "/B0", _adlsClient, aclEntries1, RequestedAclType.RemoveAcl, 10);
            Assert.IsTrue(aclStats.FilesProcessed == aclVerifyStats.FilesProcessed);
            Assert.IsTrue(aclStats.DirectoryProcessed == aclVerifyStats.DirectoryProcessed);
            Assert.IsTrue(aclStats.FilesProcessed == aclVerifyStats.FilesCorrect);
            Assert.IsTrue(aclStats.DirectoryProcessed == aclVerifyStats.DirectoriesCorrect);
        }
        /// <summary>
        /// Tests set acl
        /// </summary>
        [TestMethod]
        public void TestSetAcl()
        {
            SemSetAcl.WaitOne();
            if (_setAclRun)
            {
                SemSetAcl.Release();
                return;
            }
            List<AclEntry> aclEntries2 = GetAclEntryForSet();
            List<AclEntry> aclEntries3 = new List<AclEntry>(aclEntries2)
            {
                //Add the default permission ACLs
                new AclEntry(AclType.user, "", AclScope.Access, AclAction.All),
                new AclEntry(AclType.group, "", AclScope.Access, AclAction.All),
                new AclEntry(AclType.other, "", AclScope.Access, AclAction.None)
            };
            var aclStats = AclProcessor.RunAclProcessor(UnitTestPath + "/B1", _adlsClient, aclEntries3,
                RequestedAclType.SetAcl, 25);
            var aclVerifyStats =
                AclProcessor.RunAclVerifier(UnitTestPath + "/B1", _adlsClient, aclEntries2, RequestedAclType.SetAcl, 10);
            Assert.IsTrue(aclStats.FilesProcessed == aclVerifyStats.FilesProcessed);
            Assert.IsTrue(aclStats.DirectoryProcessed == aclVerifyStats.DirectoryProcessed);
            Assert.IsTrue(aclStats.FilesProcessed == aclVerifyStats.FilesCorrect);
            Assert.IsTrue(aclStats.DirectoryProcessed == aclVerifyStats.DirectoriesCorrect);
            _setAclRun = true;
            SemSetAcl.Release();
        }
        /// <summary>
        /// Test Acl dump and file properties
        /// </summary>
        [TestMethod]
        public void TestFileProperties()
        {
            TestSetAcl();
            var node = PropertyManager.TestGetProperty(UnitTestPath + "/B1", _adlsClient, true, false, LocalPath + @"\logFile", true, 25, true);
            TestTreeNode(node, _recurseLevel - 1);
            node = PropertyManager.TestGetProperty(UnitTestPath + "/B1", _adlsClient, false, true, LocalPath + @"\logFile1", true, 25, true);
            Assert.IsTrue(node.AllChildSameAcl);
            node = PropertyManager.TestGetProperty(UnitTestPath + "/B1", _adlsClient, true, true, LocalPath + @"\logFile2", true, 25);
            TestTreeNode(node, _recurseLevel - 1);
            node = PropertyManager.TestGetProperty(UnitTestPath + "/B1", _adlsClient, true, true, LocalPath + @"\logFile2", true, 25, true);
            TestTreeNode(node, _recurseLevel - 1);
            Assert.IsTrue(node.AllChildSameAcl);
            var entry = new List<AclEntry>()
            {
                new AclEntry(AclType.user, SdkUnitTest.Group1Id, AclScope.Access, AclAction.WriteExecute)
            };

            _adlsClient.ModifyAclEntries(UnitTestPath + "/B1/C0/C0File.txt", entry);
            node = PropertyManager.TestGetProperty(UnitTestPath + "/B1", _adlsClient, false, true, LocalPath + @"\logFile3", true, 25);
            // This will still be true because we are only getting acl of directories and consistent acl wont take account of the file
            Assert.IsTrue(node.AllChildSameAcl);
            node = PropertyManager.TestGetProperty(UnitTestPath + "/B1", _adlsClient, false, true, LocalPath + @"\logFile3", true, 25, true);
            Assert.IsFalse(node.AllChildSameAcl);
        }

        private static void TestTreeNode(PropertyTreeNode node, int recurseLevel)
        {
            GetExpectedOutput(_oneLevelDirCount, _oneLevelFileCnt, recurseLevel, _oneFileSize, out var expectedFileCount, out var expectedDirCount, out var expectedFileSize);
            Assert.IsTrue(node.TotChildFiles == expectedFileCount);
            Assert.IsTrue(node.TotChildDirec == expectedDirCount);
            Assert.IsTrue(node.TotChildSize == expectedFileSize);
            if (recurseLevel > 0)
            {
                Assert.IsTrue(node.DirectChildFiles == _oneLevelFileCnt);
                Assert.IsTrue(node.DirectChildDirec == _oneLevelDirCount);
                Assert.IsTrue(node.DirectChildSize == _oneLevelFileCnt * _oneFileSize);
            }
            foreach (var dirNode in node.ChildDirectoryNodes)
            {
                TestTreeNode(dirNode, recurseLevel - 1);
            }
        }
        /// <summary>
        /// Cleans up the test
        /// </summary>
        [ClassCleanup]
        public static void CleanClient()
        {
            _adlsClient.DeleteRecursive(RemotePath);
            Directory.Delete(LocalPath, true);
        }
    }
}
