using Microsoft.Azure.DataLake.Store.Acl;
using Microsoft.Azure.DataLake.Store.AclTools;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Microsoft.Azure.DataLake.Store.UnitTest
{
    // The test setup is for particular accounts i.e. abcxyz.caboaccountdogfood.net. To setup for another account.
    // 1. Create folders under LinkPath1 and LinkPath2 on an account. 
    // 2. These folders need to be shared to a folder on a separate account, say abcxyz-share at a certain path shared_path. You can do this using CaboProvisioningTool.exe
    // 3. Create a group with the format acount_2_PATH_ON_ACCOUNT2_RWX and add AccountOwnerObjectId app to that group as a member
    // 4. Set acls on shared_path on account 2 with rwx for normal/default acls as well as --x acl for all root directories of that path.

    [TestClass]
    public class LinkPathUnitTest
    {
        private static AdlsClient _adlsClient;
        // Assumes shares are at /share/rduttaadls02-share-1 and /mountpoint/submp/rduttaadls02-share-2
        private static string LinkPath1;
        private static string LinkPath1Root;
        private static List<string> createDirsRoot = new List<string>();
        private static List<string> createdDirs = new List<string>();
        private static List<string> createdFiles = new List<string>();
        private static bool _shoudRunLinkTests;
        private static List<AclEntry> _aclEntriesToSet;


        [ClassInitialize]
        public static void SetupTest(TestContext context)
        {
            _shoudRunLinkTests = bool.Parse((string)context.Properties["LinkTestsEnabled"]);
            if (!_shoudRunLinkTests)
                return;

            // TODO Refactor this into separate functions methods
            LinkPath1 = context.Properties["LinkPaths"].ToString().Split(',')[0].TrimEnd('/');
            int slashIndex = 0;
            // After the loop for LinkPath1 = /abc/def/xyz, createDirsRoot = {"/abc", "/abc/def/"}
            while (true)
            {
                slashIndex = LinkPath1.IndexOf('/', slashIndex + 1);
                if (slashIndex == -1)
                {
                    break;
                }

                createDirsRoot.Add(LinkPath1.Substring(0, slashIndex));
            }

            LinkPath1Root = createDirsRoot[0];
            _adlsClient = SdkUnitTest.SetupSuperClient();
            var random = new System.Random();
            foreach (var dirRoot in createDirsRoot)
            {
                foreach (var index in Enumerable.Range(0, random.Next() % 7)) // Create between 0 to 6 directories
                {
                    var dirName = dirRoot + SdkUnitTest.RandomString(10) + "/";
                    createdDirs.Add(dirName);
                    _adlsClient.CreateDirectory(dirName);
                }
            }

            string text = "I am the first line";
            byte[] textByte = Encoding.UTF8.GetBytes(text);
            foreach (var dirs in createdDirs)
            {
                foreach (var index in Enumerable.Range(0, random.Next() % 7)) // Create between 0 to 6 files
                {
                    var fileName = dirs + SdkUnitTest.RandomString(10) + ".txt";
                    createdFiles.Add(fileName);
                    using (var ostream = _adlsClient.CreateFile(fileName, IfExists.Overwrite))
                    {
                        ostream.Write(textByte, 0, textByte.Length);
                    }
                }
            }

            _aclEntriesToSet = new List<AclEntry>()
            {
                new AclEntry(AclType.user, SdkUnitTest.NonOwner1ObjectId, AclScope.Access, AclAction.ReadWrite),
                new AclEntry(AclType.user, SdkUnitTest.NonOwner2ObjectId, AclScope.Access, AclAction.WriteOnly),
                new AclEntry(AclType.user, SdkUnitTest.Group1Id, AclScope.Default, AclAction.WriteExecute)
            };
        }

        [TestMethod]
        public void AclProcessorReturnsLinksTest()
        {
            if (!_shoudRunLinkTests)
            {
                Assert.Inconclusive();
            }

            void verifyAclLinkForPath(string rootPath, string LinkPath)
            {
                // Apply acls
                var aclStats = AclProcessor.RunAclProcessor(rootPath, _adlsClient, _aclEntriesToSet, RequestedAclType.ModifyAcl, 25);

                Assert.AreEqual(aclStats.LinkPaths.Length, 1);
                Assert.AreEqual(aclStats.LinkPaths[0], LinkPath);

                // runc acl verifier
                var aclVerifyStatsPath1 =
                    AclProcessor.RunAclVerifier(rootPath, _adlsClient, _aclEntriesToSet, RequestedAclType.ModifyAcl, 25);
                Assert.IsTrue(aclStats.FilesProcessed == aclVerifyStatsPath1.FilesProcessed);
                Assert.IsTrue(aclStats.DirectoryProcessed == aclVerifyStatsPath1.DirectoryProcessed);
                Assert.IsTrue(0 == aclVerifyStatsPath1.IncorrectFileCount);
                Assert.IsTrue(0 == aclVerifyStatsPath1.IncorrectDirectoryCount);

                // Make sure acl entries are not set on link
                var aclEntriesLinkPath = _adlsClient.GetAclStatus(LinkPath).Entries.ToArray();
                foreach (var aclEntry in _aclEntriesToSet)
                {
                    Assert.IsFalse(aclEntriesLinkPath.Contains(aclEntry));
                }

            }

            verifyAclLinkForPath(LinkPath1Root, LinkPath1);
        }


        [ClassCleanup]
        public static void CleanTests()
        {
            foreach (var dir in createdDirs)
            {
                _adlsClient.DeleteRecursive(dir);
            }

            var aclStatsPaths1 = AclProcessor.RunAclProcessor(LinkPath1Root, _adlsClient, _aclEntriesToSet, RequestedAclType.RemoveAcl, 25);
        }
    }
}