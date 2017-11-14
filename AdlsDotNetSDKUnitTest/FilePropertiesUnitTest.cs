using System.Collections.Generic;
using System.IO;
using System.Threading;
using Microsoft.Azure.DataLake.Store.Acl;
using Microsoft.Azure.DataLake.Store.AclTools;
using Microsoft.Azure.DataLake.Store.FileProperties;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest;
using Microsoft.Rest.Azure.Authentication;
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
        /// <summary>
        /// Remote path
        /// </summary>
        private static string remotePath = "/Test2/RecursiveAcl/A";

        private static string localPath = @"C:\Data1";
        /// <summary>
        /// A non objectowner Id- rdutta-app1
        /// </summary>
        private static string _nonOwner1ObjectId;
        /// <summary>
        /// A non-owner object app id- rdutta-app3
        /// </summary>
        private static string _nonOwner2ObjectId;
        /// <summary>
        /// Semaphore to serialize acl modify and remove
        /// </summary>
        private static readonly Semaphore SemModifyAcl = new Semaphore(1, 1);

        /// <summary>
        /// Semaphore to serialize acl modify and remove
        /// </summary>
        private static readonly Semaphore SemSetAcl = new Semaphore(1, 1);
        /// <summary>
        /// Group Id: rdutta-group1 -has rdutta-app3,rdutta-app4
        /// </summary>
        private static string _group1Id;

        private static int _oneLevelDirCount;
        private static int _oneLevelFileCnt;
        private static int _recurseLevel;
        private static int _oneFileSize;
        private static bool _modifyAclRun;
        private static bool _setAclRun;
        [ClassInitialize]
        public static void SetupClient(TestContext context)
        {
            _nonOwner1ObjectId = SdkUnitTest.ReadSetting("NonOwner1ObjectId");
            _nonOwner2ObjectId = SdkUnitTest.ReadSetting("NonOwner2ObjectId");
            _group1Id = SdkUnitTest.ReadSetting("Group1Id");
            _adlsClient = SetupSuperClient();

            _adlsClient.DeleteRecursive("/Test2");
            _oneLevelDirCount = 3;
            _oneLevelFileCnt = 2;
            _recurseLevel = 3;
            _oneFileSize = 100;

            DataCreator.CreateDirRecursiveRemote(_adlsClient, remotePath, _recurseLevel, _oneLevelDirCount, _oneLevelFileCnt, _oneLevelFileCnt, _oneFileSize, _oneFileSize, true);
        }

        private static void GetExpectedOutput(int oneLevelDirecCnt, int oneLevelFileCnt, int recurseLevel,
            int oneFileSize, out int expectedFileCount, out int expectedDirCount, out int expectedFileSize)
        {
            expectedDirCount = expectedFileCount = expectedFileSize = 0;
            if (recurseLevel == 0)
            {
                return;
            }
            int power = 1;
            for (int i = 1; i <= recurseLevel; i++)
            {
                power *= oneLevelDirecCnt;
                expectedDirCount += power;
            }
            // last level directory doesnot have files
            expectedFileCount = (expectedDirCount - power + 1) * oneLevelFileCnt;
            expectedFileSize = expectedFileCount * oneFileSize;
        }
        /// <summary>
        /// Setsup the super client and returns
        /// </summary>
        /// <returns></returns>
        private static AdlsClient SetupSuperClient()
        {
            string clientId = SdkUnitTest.ReadSetting("AccountOwnerClientId");
            string clientSecret = SdkUnitTest.ReadSetting("AccountOwnerClientSecret");
            string domain = SdkUnitTest.ReadSetting("Domain");
            string clientAccountPath = SdkUnitTest.ReadSetting("Account");
            var creds = new ClientCredential(clientId, clientSecret);
            ServiceClientCredentials clientCreds = ApplicationTokenProvider.LoginSilentAsync(domain, creds).GetAwaiter().GetResult();
            return AdlsClient.CreateClient(clientAccountPath, clientCreds);
        }
        /// <summary>
        /// Gets a sample Acl entries - For modify and remove
        /// </summary>
        /// <returns></returns>
        private static List<AclEntry> GetAclEntryForModifyAndRemove()
        {
            return new List<AclEntry>()
            {
                new AclEntry(AclType.user, _nonOwner1ObjectId, AclScope.Access, AclAction.ReadWrite),
                new AclEntry(AclType.user, _nonOwner2ObjectId, AclScope.Access, AclAction.WriteOnly),
                new AclEntry(AclType.user, _group1Id, AclScope.Default, AclAction.WriteExecute)
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
                new AclEntry(AclType.user, _nonOwner1ObjectId, AclScope.Access, AclAction.ReadOnly),
                new AclEntry(AclType.user, _nonOwner2ObjectId, AclScope.Default, AclAction.ExecuteOnly)
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
            var aclStats = AclProcessor.RunAclProcessor(remotePath + "/B0", _adlsClient, aclEntries1,
                RequestedAclType.ModifyAcl, 25);
            var aclVerifyStats =
                AclProcessor.RunAclVerifier(remotePath + "/B0", _adlsClient, aclEntries1, RequestedAclType.ModifyAcl, 25);
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
            var aclStats = AclProcessor.RunAclProcessor(remotePath + "/B0", _adlsClient, aclEntries1,
                RequestedAclType.RemoveAcl, 10);
            var aclVerifyStats =
                AclProcessor.RunAclVerifier(remotePath + "/B0", _adlsClient, aclEntries1, RequestedAclType.RemoveAcl, 10);
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
            var aclStats = AclProcessor.RunAclProcessor(remotePath + "/B1", _adlsClient, aclEntries3,
                RequestedAclType.SetAcl, 25);
            var aclVerifyStats =
                AclProcessor.RunAclVerifier(remotePath + "/B1", _adlsClient, aclEntries2, RequestedAclType.SetAcl, 10);
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
            var node = PropertyManager.TestGetProperty(remotePath + "/B1", _adlsClient, true, true, localPath + @"\logFile", true, 25, true);
            TestTreeNode(node, _recurseLevel - 1);
            Assert.IsTrue(node.AllChildSameAcl);
            var entry = new List<AclEntry>()
            {
                new AclEntry(AclType.user, _group1Id, AclScope.Default, AclAction.WriteExecute)
            };
            _adlsClient.ModifyAclEntries(remotePath + "/B1/C0/D0", entry);
            node = PropertyManager.TestGetProperty(remotePath + "/B1", _adlsClient, true, true, localPath + @"\logFile", true, 25, true);
            Assert.IsFalse(node.AllChildSameAcl);
        }

        private static void TestTreeNode(PropertyTreeNode node, int recurseLevel)
        {
            int expectedDirCount, expectedFileCount, expectedFileSize;
            GetExpectedOutput(_oneLevelDirCount, _oneLevelFileCnt, recurseLevel, _oneFileSize, out expectedFileCount, out expectedDirCount, out expectedFileSize);
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
            _adlsClient.DeleteRecursive("/Test2");
            Directory.Delete(localPath, true);
        }
    }
}
