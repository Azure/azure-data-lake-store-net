using Microsoft.Azure.DataLake.Store.Acl;
using Microsoft.Azure.Test.HttpRecorder;
using Microsoft.Rest.ClientRuntime.Azure.TestFramework;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace Microsoft.Azure.DataLake.Store.UnitTest
{
    [TestClass]
    public class MockAzureFrameworkTest
    {

        private static TestEnvironment currentEnvironment;
        private static readonly string UnitTestDir = "/TestAzure";
        [ClassInitialize]
        public static void SetupTest(TestContext context)
        {
            currentEnvironment = TestEnvironmentFactory.GetTestEnvironment();
        }
        [TestMethod]
        public void TestMockCreateAppendRead()
        {
            // Gets the specified stacktrace and get the current frame
            // Get the class and method name so that can pass it to the mockserver which reads/writes to
            // the file named by class+method
            var sf = new StackTrace().GetFrame(0);
            var classCallingType = sf.GetMethod().ReflectedType?.ToString();
            var mockName = sf.GetMethod().Name;
            HttpMockServer.Matcher = new RecordMatcherWithApiExclusion(true, null);
            HttpMockServer.RecordsDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "SessionRecords");
            using (var context = AdlMockContext.Start(classCallingType, mockName))
            {
                var client = AdlsClient.CreateClient(SdkUnitTest.AccntName, currentEnvironment.TokenInfo[TokenAudience.Management], context.GetDelegatingHAndlersForDataPlane(currentEnvironment, new AdlMockDelegatingHandler()));
                string path = $"{UnitTestDir}/testCreateAppend2.txt";
                string text1 = "I am the first line.I am the first line.I am the first line.I am the first line.I am the first line.I am the first line.I am the first line.\n";
                string text2 = "I am the second line.I am the second line.I am the second line.I am the second line.I am the second line.I am the second line.I am the second line.";
                byte[] textByte1 = Encoding.UTF8.GetBytes(text1);
                byte[] textByte2 = Encoding.UTF8.GetBytes(text2);
                using (var ostream = client.CreateFile(path, IfExists.Overwrite, ""))
                {
                    ostream.Write(textByte1, 0, textByte1.Length);
                    ostream.Write(textByte2, 0, textByte2.Length);
                }
                string output = "";
                using (Stream istream = client.GetReadStream(path))
                {
                    int noOfBytes;
                    byte[] buffer = new byte[25];
                    do
                    {
                        noOfBytes = istream.Read(buffer, 0, buffer.Length);
                        output += Encoding.UTF8.GetString(buffer, 0, noOfBytes);
                    } while (noOfBytes > 0);
                }
                Assert.IsTrue(output.Equals(text1 + text2));
                client.Delete(path);
            }
        }

        [TestMethod]
        public void TestMockModifyAndGetAcl()
        {
            string path = $"{UnitTestDir}/TestModifyAndGetAcl.txt";
            var sf = new StackTrace().GetFrame(0);
            var classCallingType = sf.GetMethod().ReflectedType?.ToString();
            var mockName = sf.GetMethod().Name;
            HttpMockServer.Matcher = new RecordMatcherWithApiExclusion(true, null);
            HttpMockServer.RecordsDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "SessionRecords");
            using (var context = AdlMockContext.Start(classCallingType, mockName))
            {
                var client = AdlsClient.CreateClient(SdkUnitTest.AccntName, currentEnvironment.TokenInfo[TokenAudience.Management], context.GetDelegatingHAndlersForDataPlane(currentEnvironment, new AdlMockDelegatingHandler()));
                using (client.CreateFile(path, IfExists.Overwrite, "770"))
                { }
                //Setup Acl Entries
                List<AclEntry> aclList = new List<AclEntry>()
                {
                    new AclEntry(AclType.user, SdkUnitTest.NonOwner1ObjectId, AclScope.Access,
                        AclAction.ReadWrite), //Non owner client 1
                    new AclEntry(AclType.user, SdkUnitTest.NonOwner2ObjectId, AclScope.Access, AclAction.ReadExecute)
                };//NonOwnerClient 2
                client.ModifyAclEntries(path, aclList);
                AclStatus status = client.GetAclStatus(path);
                Assert.IsTrue(status.Owner.Equals(SdkUnitTest.OwnerObjectId));
                Assert.IsTrue(status.Permission.Equals("770"));
                Assert.IsTrue(status.Entries.Contains(new AclEntry(AclType.user, SdkUnitTest.NonOwner1ObjectId, AclScope.Access, AclAction.ReadWrite)));
                Assert.IsTrue(status.Entries.Contains(new AclEntry(AclType.user, SdkUnitTest.NonOwner2ObjectId, AclScope.Access, AclAction.ReadExecute)));
                client.Delete(path);
                Assert.IsFalse(client.CheckExists(path));
            }
        }

        [TestMethod]
        public void TestMockListStatus()
        {
            string path = $"{UnitTestDir}/TestDir";
            string subdir1 = $"{UnitTestDir}/TestDir/dir1";
            string subdir2 = $"{UnitTestDir}/TestDir/dir2";
            string subfile1 = $"{UnitTestDir}/TestDir/file1";
            var sf = new StackTrace().GetFrame(0);
            var classCallingType = sf.GetMethod().ReflectedType?.ToString();
            var mockName = sf.GetMethod().Name;
            HttpMockServer.Matcher = new RecordMatcherWithApiExclusion(true, null);
            HttpMockServer.RecordsDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "SessionRecords");
            using (var context = AdlMockContext.Start(classCallingType, mockName))
            {
                var client = AdlsClient.CreateClient(SdkUnitTest.AccntName, currentEnvironment.TokenInfo[TokenAudience.Management], context.GetDelegatingHAndlersForDataPlane(currentEnvironment, new AdlMockDelegatingHandler()));
                client.CreateDirectory(subdir1);
                client.CreateDirectory(subdir2);
                using (client.CreateFile(subfile1, IfExists.Overwrite, "770"))
                { }
                var dirEntries = new Dictionary<string,DirectoryEntry>();
                foreach(var entry in client.EnumerateDirectory(path))
                {
                    dirEntries.Add(entry.Name,entry);
                }
                Assert.IsTrue(dirEntries.ContainsKey("dir1"));
                Assert.IsTrue(dirEntries["dir1"].Type == DirectoryEntryType.DIRECTORY);
                Assert.IsTrue(dirEntries.ContainsKey("dir2"));
                Assert.IsTrue(dirEntries["dir2"].Type == DirectoryEntryType.DIRECTORY);
                Assert.IsTrue(dirEntries.ContainsKey("file1"));
                Assert.IsTrue(dirEntries["file1"].Type == DirectoryEntryType.FILE);
                client.DeleteRecursive(path);
                Assert.IsFalse(client.CheckExists(path));
            }
        }

        [TestMethod]
        public void TestMockRename()
        {
            string srcpath = $"{UnitTestDir}/srcfile";
            string destpath = $"{UnitTestDir}/destfile";
            string text1 = "I am the second line.I am the second line.I am the second line.I am the second line.I am the second line.I am the second line.I am the second line.";
            byte[] textByte1 = Encoding.UTF8.GetBytes(text1);
            var sf = new StackTrace().GetFrame(0);
            var classCallingType = sf.GetMethod().ReflectedType?.ToString();
            var mockName = sf.GetMethod().Name;
            HttpMockServer.Matcher = new RecordMatcherWithApiExclusion(true, null);
            HttpMockServer.RecordsDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "SessionRecords");
            using (var context = AdlMockContext.Start(classCallingType, mockName))
            {
                var client = AdlsClient.CreateClient(SdkUnitTest.AccntName, currentEnvironment.TokenInfo[TokenAudience.Management], context.GetDelegatingHAndlersForDataPlane(currentEnvironment, new AdlMockDelegatingHandler()));
                using (var stream = client.CreateFile(srcpath, IfExists.Overwrite, "770"))
                {
                    stream.Write(textByte1, 0, textByte1.Length);
                }
                client.Rename(srcpath, destpath);
                Assert.IsFalse(client.CheckExists(srcpath));
                Assert.IsTrue(client.CheckExists(destpath));
                string output = "";
                using (Stream istream = client.GetReadStream(destpath))
                {
                    int noOfBytes;
                    byte[] buffer = new byte[25];
                    do
                    {
                        noOfBytes = istream.Read(buffer, 0, buffer.Length);
                        output += Encoding.UTF8.GetString(buffer, 0, noOfBytes);
                    } while (noOfBytes > 0);
                }
                Assert.IsTrue(output.Equals(text1));
                client.Delete(destpath);
                Assert.IsFalse(client.CheckExists(destpath));
            }
        }

        [TestMethod]
        public void TestMockConcat()
        {
            string srcpath1 = $"{UnitTestDir}/srcfile1";
            string srcpath2 = $"{UnitTestDir}/srcfile2";
            string destpath = $"{UnitTestDir}/destfile";
            string text1 = "I am the second line.I am the second line.I am the second line.I am the second line.I am the second line.I am the second line.I am the second line.";
            byte[] textByte1 = Encoding.UTF8.GetBytes(text1);
            var sf = new StackTrace().GetFrame(0);
            var classCallingType = sf.GetMethod().ReflectedType?.ToString();
            var mockName = sf.GetMethod().Name;
            HttpMockServer.Matcher = new RecordMatcherWithApiExclusion(true, null);
            HttpMockServer.RecordsDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "SessionRecords");
            using (var context = AdlMockContext.Start(classCallingType, mockName))
            {
                var client = AdlsClient.CreateClient(SdkUnitTest.AccntName, currentEnvironment.TokenInfo[TokenAudience.Management], context.GetDelegatingHAndlersForDataPlane(currentEnvironment, new AdlMockDelegatingHandler()));
                using (var stream = client.CreateFile(srcpath1, IfExists.Overwrite, "770"))
                {
                    stream.Write(textByte1, 0, textByte1.Length);
                }
                using (var stream = client.CreateFile(srcpath2, IfExists.Overwrite, "770"))
                {
                    stream.Write(textByte1, 0, textByte1.Length);
                }
                client.ConcatenateFiles(destpath, new List<string>() { srcpath1 ,srcpath2});
                Assert.IsFalse(client.CheckExists(srcpath1));
                Assert.IsFalse(client.CheckExists(srcpath2));
                Assert.IsTrue(client.CheckExists(destpath));
                string output = "";
                using (Stream istream = client.GetReadStream(destpath))
                {
                    int noOfBytes;
                    byte[] buffer = new byte[25];
                    do
                    {
                        noOfBytes = istream.Read(buffer, 0, buffer.Length);
                        output += Encoding.UTF8.GetString(buffer, 0, noOfBytes);
                    } while (noOfBytes > 0);
                }
                Assert.IsTrue(output.Equals(text1+text1));
                client.Delete(destpath);
                Assert.IsFalse(client.CheckExists(destpath));
            }
        }
    }
}
