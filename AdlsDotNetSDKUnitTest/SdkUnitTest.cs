using Microsoft.Azure.DataLake.Store.Acl;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest;
using Microsoft.Rest.Azure.Authentication;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.DataLake.Store.UnitTest
{
    [TestClass]
    public class SdkUnitTest
    {
        internal static readonly string TestId = Guid.NewGuid().ToString();
        private static AdlsClient _adlsClient;
        private static readonly Random Random = new Random();
        /// <summary>
        /// Full Account domain name
        /// </summary>
        internal static string AccntName;
        /// <summary>
        /// Owner service principal object Id
        /// </summary>
        internal static string OwnerObjectId;
        /// <summary>
        /// Owner App Id
        /// </summary>
        private static string _ownerClientId;
        /// <summary>
        /// Owner App secret key
        /// </summary>
        private static string _ownerClientSecret;
        /// <summary>
        /// NonOwner1 service principal object Id
        /// </summary>
        internal static string NonOwner1ObjectId;
        /// <summary>
        /// NonOwner1 app id
        /// </summary>
        private static string _nonOwner1ClientId;
        /// <summary>
        /// Nonowner1 app secret key
        /// </summary>
        private static string _nonOwner1ClientSecret;
        /// <summary>
        /// NonOwner2 service principal object Id
        /// </summary>
        internal static string NonOwner2ObjectId;
        /// <summary>
        /// NonOwner2 app id
        /// </summary>
        private static string _nonOwner2ClientId;
        /// <summary>
        /// Nonowner2 app secret key
        /// </summary>
        private static string _nonOwner2ClientSecret;
        /// <summary>
        /// NonOwner3 service principal object Id
        /// </summary>
        private static string _nonOwner3ObjectId;
        /// <summary>
        /// NonOwner3 app id
        /// </summary>
        private static string _nonOwner3ClientId;
        /// <summary>
        /// Nonowner3 app secret key
        /// </summary>
        private static string _nonOwner3ClientSecret;
        /// <summary>
        /// Group1 Id- Group 1 should include nonowner2 and nonowner3
        /// </summary>
        internal static string Group1Id;
        /// <summary>
        /// Tenant Id
        /// </summary>
        private static string _domain;

        private static string _dogFoodAuthEndPoint;

        private static bool _isAccountTieredStore;

        private static readonly string UnitTestDir = "/Test" + TestId;
        public static string RandomString(int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
            return new string(Enumerable.Repeat(chars, length)
                .Select(s => s[Random.Next(s.Length)]).ToArray());
        }

        /// <summary>
        /// Sets up the unit test
        /// </summary>
        /// <param name="context"></param>
        [AssemblyInitialize]
        public static void SetupUnitTest(TestContext context)
        {
            
            AccntName = (string)context.Properties["Account"];
            OwnerObjectId = (string)context.Properties["AccountOwnerObjectId"];
            _ownerClientId = (string)context.Properties["AccountOwnerClientId"];
            _ownerClientSecret = (string)context.Properties["AccountOwnerClientSecret"];
            NonOwner1ObjectId = (string)context.Properties["NonOwner1ObjectId"];
            _nonOwner1ClientId = (string)context.Properties["NonOwner1ClientId"];
            _nonOwner1ClientSecret = (string)context.Properties["NonOwner1ClientSecret"];
            NonOwner2ObjectId = (string)context.Properties["NonOwner2ObjectId"];
            _nonOwner2ClientId = (string)context.Properties["NonOwner2ClientId"];
            _nonOwner2ClientSecret = (string)context.Properties["NonOwner2ClientSecret"];
            _nonOwner3ObjectId = (string)context.Properties["NonOwner3ObjectId"];
            _nonOwner3ClientId = (string)context.Properties["NonOwner3ClientId"];
            _nonOwner3ClientSecret = (string)context.Properties["NonOwner3ClientSecret"];
            Group1Id = (string)context.Properties["Group1Id"];
            _domain = (string)context.Properties["Domain"];
            _dogFoodAuthEndPoint = (string)context.Properties["DogFoodAuthenticationEndPoint"];
            _isAccountTieredStore = bool.Parse((string)context.Properties["IsAccountTieredStore"]);
            ServicePointManager.DefaultConnectionLimit = AdlsClient.DefaultNumThreads;
        }
        /// <summary>
        /// Setup the client, empties the test directory
        /// </summary>
        /// <param name="context"></param>
        [ClassInitialize]
        public static void SetupTest(TestContext context)
        {
            _adlsClient = SetupSuperClient();
            
            _adlsClient.DeleteRecursive(UnitTestDir);
            _adlsClient.CreateDirectory(UnitTestDir);
            _adlsClient.RemoveAllAcls(UnitTestDir);
            var nonOwnerAclSpec = new List<AclEntry>
            {
                new AclEntry(AclType.user, NonOwner1ObjectId, AclScope.Access, AclAction.ExecuteOnly),
                new AclEntry(AclType.user, NonOwner2ObjectId, AclScope.Access, AclAction.ExecuteOnly),
                new AclEntry(AclType.user, _nonOwner3ObjectId, AclScope.Access, AclAction.ExecuteOnly),
                new AclEntry(AclType.group, Group1Id, AclScope.Access, AclAction.ExecuteOnly)
            };
            _adlsClient.ModifyAclEntries("/", nonOwnerAclSpec);
            _adlsClient.ModifyAclEntries(UnitTestDir, nonOwnerAclSpec);
        }
        #region Setup
        /// <summary>
        /// Setup client from the super owner of the account
        /// </summary>
        /// <returns>AdlsClient</returns>
        internal static AdlsClient SetupSuperClient()
        {
            string clientId = _ownerClientId;
            string clientSecret = _ownerClientSecret;
            return SetupCommonClient(clientId, clientSecret);
        }
        /// <summary>
        /// Setup client from a app that is not owner of the account
        /// </summary>
        /// <returns>ADls Client</returns>
        private static AdlsClient SetupNonOwnerClient1()
        {
            string clientId = _nonOwner1ClientId;
            string clientSecret = _nonOwner1ClientSecret;
            return SetupCommonClient(clientId, clientSecret);
        }
        /// <summary>
        /// Setup client from a app that is not owner of the account
        /// </summary>
        /// <returns>ADls Client</returns>
        private static AdlsClient SetupNonOwnerClient2()
        {
            string clientId = _nonOwner2ClientId;
            string clientSecret = _nonOwner2ClientSecret;
            return SetupCommonClient(clientId, clientSecret);
        }
        /// <summary>
        /// Setup client from a app that is not owner of the account
        /// </summary>
        /// <returns>ADls Client</returns>
        private static AdlsClient SetupNonOwnerClient3()
        {
            string clientId = _nonOwner3ClientId;
            string clientSecret = _nonOwner3ClientSecret;
            return SetupCommonClient(clientId, clientSecret);
        }
        /// <summary>
        /// Sets up a ADLS client
        /// </summary>
        /// <param name="clientId">Client Id of the App</param>
        /// <param name="clientSecret">Client Secret</param>
        /// <returns></returns>
        private static AdlsClient SetupCommonClient(string clientId, string clientSecret)
        {
            string clientAccountPath = AccntName;
            var creds = new ClientCredential(clientId, clientSecret);
            AdlsClient client;
            if (clientAccountPath.EndsWith("azuredatalakestore.net"))
            {
                ServiceClientCredentials clientCreds = ApplicationTokenProvider.LoginSilentAsync(_domain, creds).GetAwaiter().GetResult();
                client = AdlsClient.CreateClient(clientAccountPath, clientCreds);
            }
            else
            {
                var serviceSettings = ActiveDirectoryServiceSettings.Azure;
                serviceSettings.TokenAudience = new Uri("https://management.core.windows.net/");
                serviceSettings.AuthenticationEndpoint = new Uri(_dogFoodAuthEndPoint);
                var clientCreds = ApplicationTokenProvider.LoginSilentAsync(_domain, creds, serviceSettings).GetAwaiter().GetResult();
                client = AdlsClient.CreateClient(clientAccountPath, clientCreds);
            }

            return client;
        }

        private static string GetFileOrFolderName(string type)
        {
            var chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
            Random random = new Random();
            string name = "";
            for (int i = 0; i < 16; i++)
            {
                name += chars[random.Next(chars.Length)];
            }
            
            if(type.Contains("file"))
            {
                name = "file_" + name + ".txt";
            }
            else if(type.Contains("directory"))
            {
                name = "dir_" + name;
            }

            return name;
        }

        private static void SetupTrashFile(string name, string type)
        {
            string path = $"{UnitTestDir}/" + name;
            bool result;
            if (type.Contains("file"))
            {
                _adlsClient.CreateFile(path, IfExists.Overwrite, "732");
            }
            else
            {
                result = _adlsClient.CreateDirectory(path, "777" );
                Assert.IsTrue(result);
            }
   
            result = _adlsClient.Delete(path);
            Assert.IsTrue(result);
        }

        #endregion

        [TestMethod]
        public void TestRequestIdException()
        {
            string path = $"{UnitTestDir}/testRequestIdException.txt";
            string text1 = "I am the first line";
            byte[] textByte1 = Encoding.UTF8.GetBytes(text1);
            using (var ostream = _adlsClient.CreateFile(path, IfExists.Overwrite, ""))
            {
                ostream.Write(textByte1, 0, textByte1.Length);
            }
            var response=new OperationResponse();
            Core.Append(path,"","",SyncFlag.DATA,0,textByte1,0,textByte1.Length,_adlsClient,new RequestOptions(),response );
            Assert.IsTrue(!response.IsSuccessful);
            Assert.IsTrue(response.RequestId != null);
        }

        #region TestCreate
        [TestMethod]
        public void TestCreatePathWithColon()
        {
            string path = $"{UnitTestDir}/testCreateFile:8080";
            _adlsClient.CreateDirectory(path);
        }

        /// <summary>
        /// Unit test for creating a directory using ADL SDK
        /// </summary>
        [TestMethod]
        public void TestCreateMakeDir()
        {
            string path = $"{UnitTestDir}/testDir";
            string permission = "733";
            bool result = _adlsClient.CreateDirectory(path, permission);
            Assert.IsTrue(result);
            DirectoryEntry diren = _adlsClient.GetDirectoryEntry(path);
            Assert.IsTrue(diren.Type == DirectoryEntryType.DIRECTORY);
            Assert.IsTrue(diren.Permission.Equals(permission));
            Assert.IsTrue(diren.Length == 0);
        }
        /// <summary>
        /// Unit test for trying to create a file
        ///   Flag for not creating the parent
        ///   The directories in the path does not exist
        /// </summary>
        [TestMethod]
        [ExpectedException(typeof(AdlsException))]
        public void TestCreateFileExceptionCreateParent()
        {
            string path = $"{UnitTestDir}/dir2/testCreateParentException.txt";
            _adlsClient.CreateFile(path, IfExists.Fail, null, false);
            Assert.Fail("Parent folder does not exist so create should throw an exception");
        }
        /// <summary>
        /// Unit test for trying to create a file
        ///   Flag for not overwriting if the file exists
        ///   The file exists
        /// </summary>
        [TestMethod]
        [ExpectedException(typeof(AdlsException))]
        public void TestCreateFileExceptionOverwrite()
        {
            string path = $"{UnitTestDir}/testCreateOverwriteException.txt";
            using (_adlsClient.CreateFile(path, IfExists.Fail))
            { }
            using (_adlsClient.CreateFile(path, IfExists.Fail))
            { }
            Assert.Fail("File already exists so creating file in non-overwrite mode should throw exception");
        }
        /// <summary>
        /// Unit test for trying to create a file
        ///   Permission is not valid
        /// </summary>
        [TestMethod]
        [ExpectedException(typeof(AdlsException))]
        public void TestCreateFileExceptionPermission()
        {
            string path = $"{UnitTestDir}/testCreatePermissionException.txt";
            using (_adlsClient.CreateFile(path, IfExists.Fail, "3-12"))
            {
            }
            Assert.Fail("Permission is invalid so create file should throw an exception");
        }
        /// <summary>
        /// Unit test for successfully creating a file
        /// </summary>
        [TestMethod]
        public void TestCreate()
        {
            string path = $"{UnitTestDir}/testCreate.txt";
            using (_adlsClient.CreateFile(path, IfExists.Overwrite, "732"))
            {
            }
            DirectoryEntry diren = _adlsClient.GetDirectoryEntry(path);
            Assert.IsTrue(diren.FullName.Equals(path));
            Assert.IsTrue(diren.Length == 0);
            Assert.IsTrue(!diren.HasAcl);
            Assert.IsTrue(diren.Type == DirectoryEntryType.FILE);
            Assert.IsTrue(diren.Permission.Equals("732"));
        }

        [TestMethod]
        public void TestCreateFileWithControlCharacter()
        {
            string dir = $"{UnitTestDir}/ControlCharacterDir";
            // The string below has a delete character in it 7F
            string unicodeFilename = dir + "/Cnt1-A_€__}_R_€_}_]_‚_ˆ_LA_„_}_L_R_X_‚_Z_]_€I_‚_Y_ƒ_‰_‚_[R_]_€_ƒ_Z_X_ˆ_}_ƒ_‚_LL_X_ˆ_]_‚_Z_.";
            try
            {
                using (_adlsClient.CreateFile(unicodeFilename, IfExists.Overwrite))
                {
                    Assert.Fail("This should throw an exception");
                }
            }
            catch (AdlsException ex)
            {
                Assert.IsTrue(ex.HttpStatus == HttpStatusCode.BadRequest);
                Assert.IsTrue(ex.Ex.GetType().Name.Contains("JsonReaderException"));
            }
        }
        
        /// <summary>
        /// Unit test in creating a file with unicode name. Verify by reading the file.
        /// </summary>
        [TestMethod]
        public void TestCreateUnicodeFileName()
        {
            string dir = $"{UnitTestDir}/UnicodeDir";
            string unicodeFilename = dir + "/ch+ ch.官話.官话.עברית.हिंदी.español.~`!@#$%^&*()_.+=-{}[]|;',.<>?.txt";
            string text1 = RandomString(1024);
            byte[] textByte1 = Encoding.UTF8.GetBytes(text1);
            using (var ostream = _adlsClient.CreateFile(unicodeFilename, IfExists.Overwrite))
            {
                ostream.Write(textByte1, 0, textByte1.Length);
            }
            string output = "";
            using (var istream = _adlsClient.GetReadStream(unicodeFilename))
            {
                int noOfBytes;
                byte[] buffer = new byte[1024 * 1024];
                do
                {
                    noOfBytes = istream.Read(buffer, 0, buffer.Length);
                    output += Encoding.UTF8.GetString(buffer, 0, noOfBytes);
                } while (noOfBytes > 0);
            }
            Assert.IsTrue(output.Equals(text1));
            try
            {
                _adlsClient.GetDirectoryEntry(unicodeFilename);
            }
            catch (IOException)
            {
                Assert.Fail("Directory entry list should not throw an exception");
            }
            IEnumerable<DirectoryEntry> diren = _adlsClient.EnumerateDirectory(dir);
            bool isFound = false;
            foreach (var entry in diren)
            {
                if (entry.FullName.Equals(unicodeFilename))
                {
                    isFound = true;
                    break;
                }
            }
            Assert.IsTrue(isFound);
        }
        #endregion

        #region Append
        /// <summary>
        /// Unit test for creating and riting in a file. Verifying it by reading the file.
        /// </summary>
        [TestMethod]
        public void TestCreateAppend()
        {
            string path = $"{UnitTestDir}/testCreateAppend2.txt";
            string text1 = "I am the first line.I am the first line.I am the first line.I am the first line.I am the first line.I am the first line.I am the first line.\n";
            string text2 = "I am the second line.I am the second line.I am the second line.I am the second line.I am the second line.I am the second line.I am the second line.";
            byte[] textByte1 = Encoding.UTF8.GetBytes(text1);
            byte[] textByte2 = Encoding.UTF8.GetBytes(text2);
            using (var ostream = _adlsClient.CreateFile(path, IfExists.Overwrite, ""))
            {
                ostream.Write(textByte1, 0, textByte1.Length);
                ostream.Write(textByte2, 0, textByte2.Length);
            }
            string output = "";
            using (Stream istream = _adlsClient.GetReadStream(path))
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
        }
        /// <summary>
        /// Unit test in trying to append to a non existing file
        /// </summary>
        [TestMethod]
        [ExpectedException(typeof(AdlsException))]
        public void TestAppendExceptionFile()
        {
            string path = $"{UnitTestDir}/testAppendNotExist.txt";
            string text1 = RandomString(300);
            byte[] textByte1 = Encoding.UTF8.GetBytes(text1);
            using (var ostream = _adlsClient.GetAppendStream(path))
            {
                ostream.Write(textByte1, 0, textByte1.Length);
            }
            Assert.Fail("Append for a non-existant file should throw an exception");
        }
        /// <summary>
        /// Unit test to append multiple times of varying length and perform Flushes
        /// Verify by reading the file
        /// </summary>
        [TestMethod]
        public void TestAppendDifferentLengthsMultipleTimes()
        {
            string path = $"{UnitTestDir}/testAppendDifferentLengthMultipleTimes.txt";
            int totLength = 22 * 1024 * 1024;
            string text1 = RandomString(totLength);
            byte[] textByte1 = Encoding.UTF8.GetBytes(text1);
            using (_adlsClient.CreateFile(path, IfExists.Overwrite, ""))
            { }
            using (var ostream = _adlsClient.GetAppendStream(path))
            {
                ostream.Write(textByte1, 0, 3 * 1024 * 1024);
                ostream.Write(textByte1, 3 * 1024 * 1024, 2 * 1024 * 1024);
                ostream.Write(textByte1, 5 * 1024 * 1024, 5 * 1024 * 1024);
                ostream.Flush();
                ostream.Write(textByte1, 10 * 1024 * 1024, 4 * 1024 * 1024);
                ostream.Flush();
                ostream.Write(textByte1, 14 * 1024 * 1024, 8 * 1024 * 1024);
            }
            string output = "";
            using (var istream = _adlsClient.GetReadStream(path))
            {
                int noOfBytes;
                byte[] buffer = new byte[1024 * 1024];
                do
                {
                    noOfBytes = istream.Read(buffer, 0, buffer.Length);
                    output += Encoding.UTF8.GetString(buffer, 0, noOfBytes);
                } while (noOfBytes > 0);
            }
            Assert.IsTrue(output.Equals(text1));
        }
        /// <summary>
        /// Unit test to append multiple times of varying length
        /// Verify by reading the file
        /// </summary>
        [TestMethod]
        public void TestAppendDifferentLengthsMultipleTimes1()
        {
            string path = $"{UnitTestDir}/testAppendDifferentLengthMultipleTimes1.txt";
            int totLength = 16 * 1024 * 1024;
            string text1 = RandomString(totLength);
            byte[] textByte1 = Encoding.UTF8.GetBytes(text1);
            using (_adlsClient.CreateFile(path, IfExists.Overwrite, ""))
            { }
            using (var ostream = _adlsClient.GetAppendStream(path))
            {
                ostream.Write(textByte1, 0, 2 * 1024 * 1024);
                ostream.Write(textByte1, 2 * 1024 * 1024, 1 * 1024 * 1024);
                ostream.Write(textByte1, 3 * 1024 * 1024, 1 * 1024 * 1024);
                ostream.Write(textByte1, 4 * 1024 * 1024, 3 * 1024 * 1024);
                ostream.Write(textByte1, 7 * 1024 * 1024, 2 * 1024 * 1024);
                ostream.Write(textByte1, 9 * 1024 * 1024, 5 * 1024 * 1024);
                ostream.Write(textByte1, 14 * 1024 * 1024, 2 * 1024 * 1024);
            }
            string output = "";
            using (var istream = _adlsClient.GetReadStream(path))
            {
                int noOfBytes;
                byte[] buffer = new byte[1024 * 1024];
                do
                {
                    noOfBytes = istream.Read(buffer, 0, buffer.Length);
                    output += Encoding.UTF8.GetString(buffer, 0, noOfBytes);
                } while (noOfBytes > 0);
            }
            Assert.IsTrue(output.Equals(text1));
        }
        /// <summary>
        /// Unit test to create a file and open a appendstream and make one append to empty file.
        /// Verify by reading.
        /// </summary>
        [TestMethod]
        public void TestAppendEmptyFile()
        {
            string path = $"{UnitTestDir}/testAppendEmptyFile.txt";
            string text1 = RandomString(9 * 1024 * 1024);
            byte[] textByte1 = Encoding.UTF8.GetBytes(text1);
            using (_adlsClient.CreateFile(path, IfExists.Overwrite, ""))
            { }
            using (var ostream = _adlsClient.GetAppendStream(path))
            {
                ostream.Write(textByte1, 0, textByte1.Length);
            }
            string output = "";
            using (var istream = _adlsClient.GetReadStream(path))
            {
                int noOfBytes;
                byte[] buffer = new byte[1024 * 1024];
                do
                {
                    noOfBytes = istream.Read(buffer, 0, buffer.Length);
                    output += Encoding.UTF8.GetString(buffer, 0, noOfBytes);
                } while (noOfBytes > 0);
            }
            Assert.IsTrue(output.Equals(text1));
        }
        /// <summary>
        /// Unit test to create a file and open a appendstream and make one append to empty file.
        /// Verify by reading. This is calling the async API.
        /// </summary>
        [TestMethod]
        public void TestAppendEmptyFileAsync()
        {
            string path = $"{UnitTestDir}/testAppendEmptyFileAsync.txt";
            string text1 = RandomString(6 * 1024 * 1024);
            byte[] textByte1 = Encoding.UTF8.GetBytes(text1);
            using (_adlsClient.CreateFileAsync(path, IfExists.Overwrite, "").GetAwaiter().GetResult())
            { }
            using (var ostream = _adlsClient.GetAppendStream(path))
            {
                ostream.WriteAsync(textByte1, 0, textByte1.Length).GetAwaiter().GetResult();
            }
            string output = "";
            using (var istream = _adlsClient.GetReadStream(path))
            {
                int noOfBytes;
                byte[] buffer = new byte[1024 * 1024];
                do
                {
                    noOfBytes = istream.ReadAsync(buffer, 0, buffer.Length).GetAwaiter().GetResult();
                    output += Encoding.UTF8.GetString(buffer, 0, noOfBytes);
                } while (noOfBytes > 0);
            }
            Assert.IsTrue(output.Equals(text1));
        }
        
        /// <summary>
        /// Unit test to append toa  non empty file. Create a file and append. Close and reopen a append stream and then append to a non-empty file.
        /// Verify by reading
        /// </summary>
        [TestMethod]
        public void TestAppendNotEmptyFile()
        {
            string path = $"{UnitTestDir}/testAppend2.txt";
            string text1 = RandomString(27 * 1024 * 1024);
            byte[] textByte1 = Encoding.UTF8.GetBytes(text1);
            using (var ostream = _adlsClient.CreateFile(path, IfExists.Overwrite, ""))
            {
                ostream.Write(textByte1, 0, textByte1.Length);
            }
            string text2 = RandomString(1024 * 1024);
            string text3 = RandomString(100 * 1024);
            byte[] textByte2 = Encoding.UTF8.GetBytes(text2);
            byte[] textByte3 = Encoding.UTF8.GetBytes(text3);
            using (var ostream = _adlsClient.GetAppendStream(path))
            {
                ostream.Write(textByte2, 0, textByte2.Length);
                ostream.Write(textByte3, 0, textByte3.Length);
            }
            string output = "";
            using (var istream = _adlsClient.GetReadStream(path))
            {
                istream.Seek(textByte1.Length, SeekOrigin.Begin);
                int noOfBytes;
                byte[] buffer = new byte[2 * 1024 * 1024];
                do
                {
                    noOfBytes = istream.Read(buffer, 0, buffer.Length);
                    output += Encoding.UTF8.GetString(buffer, 0, noOfBytes);
                } while (noOfBytes > 0);
            }
            Assert.IsTrue(output.Equals(text2 + text3));
        }
        #endregion

        #region TestSeek
        /// <summary>
        /// Unit test to seek a file from current. 
        ///   Read till 1/8th of total size
        ///   Seek from current for 1/8th of total size
        ///   Read 1/8th of current size
        ///   Seek from current for 1/8th of total size
        ///   Read till end
        /// Verify the expected and actual reads
        /// </summary>
        [TestMethod]
        public void TestSeekCurrent()
        {

            string path = $"{UnitTestDir}/testSeekCurrent.txt";
            int strLength = 24 * 1024 * 1024;
            string text1 = RandomString(strLength);
            int lengthToReadBeforeSeek = strLength / 8;
            byte[] textByte1 = Encoding.UTF8.GetBytes(text1);
            using (var ostream = _adlsClient.CreateFile(path, IfExists.Overwrite, ""))
            {
                ostream.Write(textByte1, 0, textByte1.Length);
            }
            string expectedOp = "";
            string actualOp = "";
            using (var istream = _adlsClient.GetReadStream(path))
            {
                byte[] buff = new byte[strLength];
                int totalBytes = 0, startByte = 0;
                int noBytes;
                int totalLengthToRead = lengthToReadBeforeSeek;
                do
                {
                    noBytes = istream.Read(buff, 0, totalLengthToRead);
                    actualOp += Encoding.UTF8.GetString(buff, 0, noBytes);
                    totalBytes += noBytes;
                    totalLengthToRead -= noBytes;
                } while (noBytes > 0 && totalLengthToRead > 0);
                expectedOp += text1.Substring(startByte, totalBytes);
                istream.Seek(lengthToReadBeforeSeek, SeekOrigin.Current);
                totalBytes += lengthToReadBeforeSeek;
                startByte = totalBytes;
                totalLengthToRead = lengthToReadBeforeSeek;
                do
                {
                    noBytes = istream.Read(buff, 0, totalLengthToRead);
                    actualOp += Encoding.UTF8.GetString(buff, 0, noBytes);
                    totalBytes += noBytes;
                    totalLengthToRead -= noBytes;
                } while (noBytes > 0 && totalLengthToRead > 0);
                expectedOp += text1.Substring(startByte, totalBytes - startByte);
                istream.Seek(lengthToReadBeforeSeek, SeekOrigin.Current);
                try
                {
                    istream.Seek(3 * strLength / 4, SeekOrigin.Current);
                    Assert.Fail("Trying to seek beyond the end of file so it should throw an exception");
                }
                catch (IOException)
                {
                }
                totalBytes += lengthToReadBeforeSeek;
                do
                {
                    noBytes = istream.Read(buff, 0, buff.Length);
                    actualOp += Encoding.UTF8.GetString(buff, 0, noBytes);
                } while (noBytes > 0);
                expectedOp += text1.Substring(totalBytes, text1.Length - totalBytes);
            }
            Assert.IsTrue(actualOp.Equals(expectedOp));
        }
        /// <summary>
        /// Unit test for seek a file from begining
        ///   Read to middle
        ///   Seek to 1/4 from start
        ///   Read to middle
        ///   Seek to 3/4 from start
        ///   Read till end
        /// Verfiy expected and actual reads
        /// </summary>
        [TestMethod]
        public void TestSeekBegin()
        {
            string path = $"{UnitTestDir}/testSeekBegin.txt";
            int strLength = 12 * 1024 * 1024;
            string text1 = RandomString(strLength);
            int readTill = strLength / 2;
            byte[] textByte1 = Encoding.UTF8.GetBytes(text1);
            using (var ostream = _adlsClient.CreateFile(path, IfExists.Overwrite, ""))
            {
                ostream.Write(textByte1, 0, textByte1.Length);
            }
            string expectedOp = "";
            string actualOp = "";
            using (var istream = _adlsClient.GetReadStream(path))
            {
                byte[] buff = new byte[strLength];
                int totalBytes = 0, startBytes = 0;
                int noBytes;
                int totalLengthToRead = readTill - startBytes;
                do
                {
                    noBytes = istream.Read(buff, 0, totalLengthToRead);
                    actualOp += Encoding.UTF8.GetString(buff, 0, noBytes);
                    totalBytes += noBytes;
                    totalLengthToRead -= noBytes;
                } while (noBytes > 0 && totalLengthToRead > 0);
                expectedOp += text1.Substring(startBytes, totalBytes - startBytes);
                istream.Seek(strLength / 4, SeekOrigin.Begin);
                startBytes = totalBytes = strLength / 4;
                totalLengthToRead = readTill - startBytes;
                do
                {
                    noBytes = istream.Read(buff, 0, totalLengthToRead);
                    actualOp += Encoding.UTF8.GetString(buff, 0, noBytes);
                    totalBytes += noBytes;
                    totalLengthToRead -= noBytes;
                } while (noBytes > 0 && totalLengthToRead > 0);
                expectedOp += text1.Substring(startBytes, totalBytes - startBytes);
                istream.Seek(3 * strLength / 4, SeekOrigin.Begin);
                try
                {
                    istream.Seek(3 * strLength / 4, SeekOrigin.Current);
                    Assert.Fail("Trying to seek beyond the end of file so it should throw an exception");
                }
                catch (IOException)
                {
                }
                startBytes = totalBytes = 3 * strLength / 4;
                totalLengthToRead = 2 * readTill - startBytes;
                do
                {
                    noBytes = istream.Read(buff, 0, totalLengthToRead);
                    actualOp += Encoding.UTF8.GetString(buff, 0, noBytes);
                    totalBytes += noBytes;
                    totalLengthToRead -= noBytes;
                } while (noBytes > 0 && totalLengthToRead > 0);
                expectedOp += text1.Substring(startBytes, totalBytes - startBytes);
                try
                {
                    istream.Seek(-1, SeekOrigin.Begin);
                    Assert.Fail("Trying to seek beyond the begining of file so it should throw an exception");
                }
                catch (IOException)
                {
                }
            }
            Assert.IsTrue(actualOp.Equals(expectedOp));
        }
        /// Unit test for seek a file from current in reverse direction
        ///   Read to middle
        ///   Seek behind to 1/4th of total size from curent
        ///   Read till 3/4th of total size
        ///   Seek behind to middle from current
        ///   Read till end
        /// Verfiy expected and actual reads
        [TestMethod]
        public void TestSeekCurrentBack()
        {
            string path = $"{UnitTestDir}/testSeekCurrentBack.txt";
            int strLength = 12 * 1024 * 1024;
            string text1 = RandomString(strLength);
            int readTill = strLength / 2;
            byte[] textByte1 = Encoding.UTF8.GetBytes(text1);
            using (var ostream = _adlsClient.CreateFile(path, IfExists.Overwrite, ""))
            {
                ostream.Write(textByte1, 0, textByte1.Length);
            }
            string expectedOp = "";
            string actualOp = "";
            using (var istream = _adlsClient.GetReadStream(path))
            {
                byte[] buff = new byte[strLength];
                int totalBytes = 0, startBytes = 0;
                int noBytes;
                int totalLengthToRead = readTill - startBytes;
                do
                {
                    noBytes = istream.Read(buff, 0, totalLengthToRead);
                    actualOp += Encoding.UTF8.GetString(buff, 0, noBytes);
                    totalBytes += noBytes;
                    totalLengthToRead -= noBytes;
                } while (noBytes > 0 && totalLengthToRead > 0);
                expectedOp += text1.Substring(startBytes, totalBytes - startBytes);
                istream.Seek(-1 * strLength / 4, SeekOrigin.Current);
                startBytes = totalBytes = strLength / 4;
                totalLengthToRead = 3 * readTill / 2 - startBytes;
                do
                {
                    noBytes = istream.Read(buff, 0, totalLengthToRead);
                    actualOp += Encoding.UTF8.GetString(buff, 0, noBytes);
                    totalBytes += noBytes;
                    totalLengthToRead -= noBytes;
                } while (noBytes > 0 && totalLengthToRead > 0);
                expectedOp += text1.Substring(startBytes, totalBytes - startBytes);
                istream.Seek(-1 * strLength / 4, SeekOrigin.Current);
                startBytes = totalBytes = strLength / 2;
                totalLengthToRead = 2 * readTill - startBytes;
                do
                {
                    noBytes = istream.Read(buff, 0, totalLengthToRead);
                    actualOp += Encoding.UTF8.GetString(buff, 0, noBytes);
                    totalBytes += noBytes;
                    totalLengthToRead -= noBytes;
                } while (noBytes > 0 && totalLengthToRead > 0);
                expectedOp += text1.Substring(startBytes, totalBytes - startBytes);
            }
            Assert.IsTrue(actualOp.Equals(expectedOp));
        }
        /// Unit test for seek a file from end of file
        ///   Read to middle
        ///   Seek to 3/4th of total size from end (1/4th from begining)
        ///   Read till middle
        ///   Seek to 1/4th from end (3/4th from begining)
        ///   Read till end
        /// Verfiy expected and actual reads
        [TestMethod]
        public void TestSeekEnd()
        {
            string path = $"{UnitTestDir}/testSeekEnd.txt";
            int strLength = 12 * 1024 * 1024;
            string text1 = RandomString(strLength);
            int readTill = strLength / 2;
            byte[] textByte1 = Encoding.UTF8.GetBytes(text1);
            using (var ostream = _adlsClient.CreateFile(path, IfExists.Overwrite, ""))
            {
                ostream.Write(textByte1, 0, textByte1.Length);
            }
            string expectedOp = "";
            string actualOp = "";
            using (var istream = _adlsClient.GetReadStream(path))
            {
                byte[] buff = new byte[strLength];
                int totalBytes = 0, startBytes = 0;
                int noBytes;
                int totalLengthToRead = readTill - startBytes;
                do
                {
                    noBytes = istream.Read(buff, 0, totalLengthToRead);
                    actualOp += Encoding.UTF8.GetString(buff, 0, noBytes);
                    totalBytes += noBytes;
                    totalLengthToRead -= noBytes;
                } while (noBytes > 0 && totalLengthToRead > 0);
                expectedOp += text1.Substring(startBytes, totalBytes - startBytes);
                istream.Seek(-3 * strLength / 4, SeekOrigin.End);
                startBytes = totalBytes = strLength / 4;
                totalLengthToRead = readTill - startBytes;
                do
                {
                    noBytes = istream.Read(buff, 0, totalLengthToRead);
                    actualOp += Encoding.UTF8.GetString(buff, 0, noBytes);
                    totalBytes += noBytes;
                    totalLengthToRead -= noBytes;
                } while (noBytes > 0 && totalLengthToRead > 0);
                expectedOp += text1.Substring(startBytes, totalBytes - startBytes);
                istream.Seek(-strLength / 4, SeekOrigin.End);
                try
                {
                    istream.Seek(3 * strLength / 4, SeekOrigin.Current);
                    Assert.Fail("Trying to seek beyond the end of file so it should throw an exception");
                }
                catch (IOException)
                {
                }
                startBytes = totalBytes = 3 * strLength / 4;
                totalLengthToRead = 2 * readTill - startBytes;
                do
                {
                    noBytes = istream.Read(buff, 0, totalLengthToRead);
                    actualOp += Encoding.UTF8.GetString(buff, 0, noBytes);
                    totalBytes += noBytes;
                    totalLengthToRead -= noBytes;
                } while (noBytes > 0 && totalLengthToRead > 0);
                expectedOp += text1.Substring(startBytes, totalBytes - startBytes);
                try
                {
                    istream.Seek(1, SeekOrigin.End);
                    Assert.Fail("Trying to seek beyond the end of file so it should throw an exception");
                }
                catch (IOException)
                {
                }
            }
            Assert.IsTrue(actualOp.Equals(expectedOp));
        }
        #endregion

        #region ConcurrentAppend
        [TestMethod]
        [DataRow(15)]
        [DataRow(4*1024*1024)]
        [DataRow(6 * 1024 * 1024)]
        public void TestConcurrentAppendSerial(int size)
        {
            string path =$"{UnitTestDir}/testConcurrentAppend_"+size;
            int count = 5;
            string line = RandomString(size);
            byte[] textByte1 = Encoding.UTF8.GetBytes(line);
            string expectedOutput = "";
            for(int i = 0; i < count; i++)
            {
                _adlsClient.ConcurrentAppend(path, true, textByte1, 0, textByte1.Length);
                expectedOutput += line;
            }
            string actualOutput;
            using (var reader = new StreamReader(_adlsClient.GetReadStream(path)))
            {
                actualOutput = reader.ReadToEnd();
            }
            Assert.IsTrue(actualOutput.Equals(expectedOutput));
        }
        [TestMethod]
        [DataRow(15)]
        [DataRow(1024 * 1024)]
        public void TestConcurrentAppendParallel(int size)
        {
            string path = $"{UnitTestDir}/testConcurrentAppendParallel_" + size;
            int count = 10;
            string line = RandomString(size);
            byte[] textByte1 = Encoding.UTF8.GetBytes(line);
            string expectedOutput = "";
            Parallel.For(0, count,
                index => { _adlsClient.ConcurrentAppend(path, true, textByte1, 0, textByte1.Length); });
            for (int i = 0; i < count; i++)
            {
                expectedOutput += line;
            }
            string actualOutput;
            using (var reader = new StreamReader(_adlsClient.GetReadStream(path)))
            {
                actualOutput = reader.ReadToEnd();
            }
            Assert.IsTrue(actualOutput.Equals(expectedOutput));
        }
        #endregion

        #region GetFileStatus
        [TestMethod]
        [DataRow(15)]
        [DataRow(1 * 1024 * 1024)]
        public void TestConcurrentAppendGetFileStatus(int size)
        {
            string path = $"{UnitTestDir}/testConcurrentAppendParallelGetFile_" + size;
            int count = 10;
            string line = RandomString(size);
            byte[] textByte1 = Encoding.UTF8.GetBytes(line);
            int expectedLength = count * size;
            Parallel.For(0, count,
                index => { _adlsClient.ConcurrentAppend(path, true, textByte1, 0, textByte1.Length); });
            var resp = new OperationResponse();
            var diren = Core.GetFileStatusAsync(path, UserGroupRepresentation.ObjectID, _adlsClient, new RequestOptions(), resp, default(CancellationToken), true).GetAwaiter().GetResult();
            Assert.IsTrue(diren.Length == expectedLength);
        }

        /// <summary>
        /// Unit test for verifying whether UserRepresentation correctly retrieves object Id or the user principal name in getfilestatus
        /// </summary>
        [TestMethod]
        public void TestUserRepresentation()
        {
            string filename = $"{UnitTestDir}/UserRepresentation.txt";
            using (var ostream = new StreamWriter(_adlsClient.CreateFile(filename, IfExists.Overwrite)))
            {
                ostream.Write("Hello This is a user representation test");
            }
            var entry = _adlsClient.GetDirectoryEntry(filename);
            Assert.IsTrue(VerifyGuid(entry.User));
            entry = _adlsClient.GetDirectoryEntry(filename, UserGroupRepresentation.UserPrincipalName);
            Assert.IsFalse(VerifyGuid(entry.User));
        }

        #endregion

        #region Rename
        /// <summary>
        /// Unit test to rename a directory where the source directory exists as a subdirectory in the destination path
        /// </summary>
        /// <param name="overwrite">Whether to overwrite the existing destination if it exists</param>
        [TestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public void TestRenameDirectoryDestinationExistsSubDirec(bool overwrite)
        {
            string srcpath = $"{UnitTestDir}/testRenameSource1" + overwrite;
            string destPath = $"{UnitTestDir}/testRenameDestination1" + overwrite;
            string subDestpath = destPath + "/testRenameSource1" + overwrite;
            bool result = _adlsClient.CreateDirectory(srcpath, "");
            Assert.IsTrue(result);
            result = _adlsClient.CreateDirectory(subDestpath, "");
            Assert.IsTrue(result);
            result = _adlsClient.Rename(srcpath, destPath);
            Assert.IsFalse(result);
        }
        /// <summary>
        /// Unit test to rename a directory where the destination does not exist
        /// </summary>
        /// <param name="overwrite">Whether to overwrite the existing destination if it exists</param>
        [TestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public void TestRenameDirectoryDestinationNotExist(bool overwrite)
        {
            string srcDirPath = $"{UnitTestDir}/testRenameSource2" + overwrite;
            string destDirPath = $"{UnitTestDir}/testRenameDest2" + overwrite;
            bool result = _adlsClient.CreateDirectory(srcDirPath, "");
            Assert.IsTrue(result);
            result = _adlsClient.Rename(srcDirPath, destDirPath, overwrite);
            Assert.IsTrue(result);
            try
            {
                _adlsClient.GetDirectoryEntry(srcDirPath);
                Assert.Fail("Src directory should not exist so GetFileStatus should throw an exception");
            }
            catch (IOException)
            { }
            DirectoryEntry diren = _adlsClient.GetDirectoryEntry(destDirPath);
            Assert.IsNotNull(diren);
        }
        /// <summary>
        /// Unit test to rename a directory where the destination directory is empty
        /// </summary>
        /// <param name="overwrite">Whether to overwrite the existing destination if it exists</param>
        [TestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public void TestRenameDirectoryDestinationExist(bool overwrite)
        {
            string srcDirPath = $"{UnitTestDir}/testRenameSource3" + overwrite;
            string destDirPath = $"{UnitTestDir}/testRenameDest3" + overwrite;
            string expectedDestPath = destDirPath + "/testRenameSource3" + overwrite;
            Assert.IsTrue(_adlsClient.CreateDirectory(srcDirPath, ""));
            Assert.IsTrue(_adlsClient.CreateDirectory(destDirPath, ""));
            Assert.IsTrue(_adlsClient.Rename(srcDirPath, destDirPath, overwrite));
            try
            {
                _adlsClient.GetDirectoryEntry(srcDirPath);
                Assert.Fail("Src directory should not exist so GetFileStatus should throw an exception");
            }
            catch (IOException)
            { }
            DirectoryEntry diren = _adlsClient.GetDirectoryEntry(expectedDestPath);
            Assert.IsNotNull(diren);
        }
        /// <summary>
        /// Unit test to rename a directory where the destination directory has a file
        /// </summary>
        /// <param name="overwrite">Whether to overwrite the existing destination if it exists</param>
        [TestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public void TestRenameDirectoryDestinationExistNonEmpty(bool overwrite)
        {
            string srcDirPath = $"{UnitTestDir}/testRenameSource4" + overwrite;
            string destDirPath = $"{UnitTestDir}/testRenameDest4" + overwrite;
            string destDirFilePath = destDirPath + "/File.txt";
            string expectedDestPath = destDirPath + "/testRenameSource4" + overwrite;
            Assert.IsTrue(_adlsClient.CreateDirectory(srcDirPath, ""));
            Assert.IsTrue(_adlsClient.CreateDirectory(destDirPath, ""));
            using (_adlsClient.CreateFile(destDirFilePath, IfExists.Overwrite, ""))
            { }
            Assert.IsTrue(_adlsClient.Rename(srcDirPath, destDirPath, overwrite));
            try
            {
                _adlsClient.GetDirectoryEntry(srcDirPath);
                Assert.Fail("Src Directory should not exist so GetFileStatus should throw an exception");
            }
            catch (IOException)
            { }
            DirectoryEntry diren = _adlsClient.GetDirectoryEntry(expectedDestPath);
            Assert.IsNotNull(diren);
        }
        /// <summary>
        /// Unit test to rename a file where the destination does not exist
        /// </summary>
        /// <param name="overwrite">Whether to overwrite the existing destination if it exists</param>
        [TestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public void TestRenameFileDestinationNotExist(bool overwrite)
        {
            string srcFilePath = $"{UnitTestDir}/testRenameSource5" + overwrite + ".txt";
            string destFilePath = $"{UnitTestDir}/testRenameDest5" + overwrite + ".txt";
            int strLength = 300;
            string srcFileText = RandomString(strLength);
            byte[] srcByte = Encoding.UTF8.GetBytes(srcFileText);
            using (var ostream = _adlsClient.CreateFile(srcFilePath, IfExists.Overwrite, ""))
            {
                ostream.Write(srcByte, 0, srcByte.Length);
            }
            bool result = _adlsClient.Rename(srcFilePath, destFilePath, overwrite);
            Assert.IsTrue(result);
            try
            {
                _adlsClient.GetDirectoryEntry(srcFilePath);
                Assert.Fail("Src file shouldnt exist so GetFileStatus should throw an exception");
            }
            catch (IOException)
            { }
            string output = "";
            try
            {
                using (var istream = _adlsClient.GetReadStream(destFilePath))
                {
                    int noOfBytes;
                    byte[] buffer = new byte[2 * 1024 * 1024];
                    do
                    {
                        noOfBytes = istream.Read(buffer, 0, buffer.Length);
                        output += Encoding.UTF8.GetString(buffer, 0, noOfBytes);
                    } while (noOfBytes > 0);
                }
            }
            catch (IOException) { Assert.Fail("The destination file should exist so ReadStream should throw an exception"); }
            Assert.IsTrue(output.Equals(srcFileText));
        }
        /// <summary>
        /// Unit test to rename a file where the destination file exists
        /// </summary>
        /// <param name="overwrite">Whether to overwrite the existing destination if it exists</param>
        [TestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public void TestRenameFileDestinationExist(bool overwrite)
        {
            string srcFilePath = $"{UnitTestDir}/testRenameSource6" + overwrite + ".txt";
            string destFilePath = $"{UnitTestDir}/testRenameDest6" + overwrite + ".txt";
            int strLength = 300;
            string srcFileText = RandomString(strLength);
            byte[] srcByte = Encoding.UTF8.GetBytes(srcFileText);
            using (var ostream = _adlsClient.CreateFile(srcFilePath, IfExists.Overwrite, ""))
            {
                ostream.Write(srcByte, 0, srcByte.Length);
            }
            string destFileText = RandomString(strLength);
            byte[] destByte = Encoding.UTF8.GetBytes(destFileText);
            using (var ostream = _adlsClient.CreateFile(destFilePath, IfExists.Overwrite, ""))
            {
                ostream.Write(destByte, 0, destByte.Length);
            }
            bool result = _adlsClient.Rename(srcFilePath, destFilePath, overwrite);
            if (overwrite)
            {
                Assert.IsTrue(result);
            }
            else
            {
                Assert.IsFalse(result);
            }
            try
            {
                _adlsClient.GetDirectoryEntry(srcFilePath);
                if (overwrite)
                {
                    Assert.Fail("Src file should not exist so Getfilestatus should throw an exception");
                }
            }
            catch (IOException)
            {
                if (!overwrite)
                {
                    Assert.Fail("Src file should exist so getfilestatus should not throw an exception");
                }
            }
            string output = "";
            using (var istream = _adlsClient.GetReadStream(destFilePath))
            {
                int noOfBytes;
                byte[] buffer = new byte[2 * 1024 * 1024];
                do
                {
                    noOfBytes = istream.Read(buffer, 0, buffer.Length);
                    output += Encoding.UTF8.GetString(buffer, 0, noOfBytes);
                } while (noOfBytes > 0);
            }
            Assert.IsTrue(overwrite ? output.Equals(srcFileText) : output.Equals(destFileText));
        }

        [TestMethod]
        public void TestRenameFileEncoding()
        {
            string srcFilePath = $"{UnitTestDir}/testSourceRenameFileEncoding.txt";
            string destFilePath = $"{UnitTestDir}/testDestRenameFileEncoding+,#?.txt";
            using (_adlsClient.CreateFile(srcFilePath, IfExists.Overwrite, ""))
            {
            }
            bool result = _adlsClient.Rename(srcFilePath, destFilePath);
            Assert.IsTrue(result);
            Assert.IsTrue(_adlsClient.CheckExists(destFilePath));
        }

        #endregion

        #region Delete
        /// <summary>
        /// Unit test to try deleting an non empty directory without specifying the flag
        /// </summary>
        [TestMethod]
        [ExpectedException(typeof(AdlsException))]
        public void TestDeleteException()
        {
            string deletePath = $"{UnitTestDir}/testDelete/testDelete1";
            _adlsClient.CreateDirectory(deletePath, "");
            _adlsClient.Delete($"{UnitTestDir}/testDelete");
            Assert.Fail("Trying to delete an non-empty directory so it should throw an exception");
        }
        /// <summary>
        /// Unit test to delete non empty directory
        /// </summary>
        [TestMethod]
        public void TestDeleteNonRecursive()
        {
            string deletePath = $"{UnitTestDir}/testDelete1";
            _adlsClient.CreateDirectory(deletePath, "");
            bool result = _adlsClient.Delete(deletePath);
            Assert.IsTrue(result);
            try
            {
                _adlsClient.GetDirectoryEntry(deletePath);
                Assert.Fail("The directory should have been deleted so GetFileStatus should throw an exception");
            }
            catch (IOException)
            {
            }
        }
        /// <summary>
        /// Unit test to delete a directory recursively
        /// </summary>
        [TestMethod]
        public void TestDeleteRecursive()
        {
            string deletePath = $"{UnitTestDir}/testDelete2/testDelete3/testDeleteInternal";
            _adlsClient.CreateDirectory(deletePath, "");
            bool result = _adlsClient.DeleteRecursive($"{UnitTestDir}/testDelete2");
            Assert.IsTrue(result);
            try
            {
                _adlsClient.GetDirectoryEntry($"{UnitTestDir}/testDelete2");
                Assert.Fail("The directory should have been deleted so GetFileStatus should throw an exception");
            }
            catch (IOException) { }

        }
        #endregion

        #region Concat
        /// <summary>
        /// Unit test to try concat same file
        /// </summary>
        [TestMethod]
        [ExpectedException(typeof(AdlsException))]
        public void TestConcatException1()
        {
            string destPath = $"{UnitTestDir}/destPath.txt";
            List<string> srcList = new List<string>();
            string srcFile1 = $"{UnitTestDir}/Source/srcPathEx1.txt";
            srcList.Add(srcFile1);
            srcList.Add(srcFile1);
            _adlsClient.ConcatenateFiles(destPath, srcList);
            Assert.Fail("Trying to concat same file should fail");
        }
        /// <summary>
        /// Unit test to try concat the destination path also
        /// </summary>
        [TestMethod]
        [ExpectedException(typeof(AdlsException))]
        public void TestConcatException2()
        {
            string destPath = $"{UnitTestDir}/destPath.txt";
            List<string> srcList = new List<string>();
            string srcFile1 = $"{UnitTestDir}/Source/srcPathEx2.txt";
            srcList.Add(destPath);
            srcList.Add(srcFile1);
            _adlsClient.ConcatenateFiles(destPath, srcList);
            Assert.Fail("Concating the destination path should fail");
        }
        /// <summary>
        /// Unit test to try concat one file
        /// </summary>
        [TestMethod]
        public void TestConcatOneFile()
        {
            string destPath = $"{UnitTestDir}/destPathOneFile.txt";
            string srcFile1 = $"{UnitTestDir}/Source/srcPathOneFile.txt";
            string text1 = RandomString(2 * 1024);
            byte[] textByte1 = Encoding.UTF8.GetBytes(text1);
            using (var ostream = _adlsClient.CreateFile(srcFile1, IfExists.Overwrite, ""))
            {
                ostream.Write(textByte1, 0, textByte1.Length);
            }

            List<string> srcList = new List<string>
            {
                srcFile1
            };
            _adlsClient.ConcatenateFiles(destPath, srcList);
            string output = "";
            using (var istream = _adlsClient.GetReadStream(destPath))
            {
                int noOfBytes;
                byte[] buffer = new byte[2 * 1024];
                do
                {
                    noOfBytes = istream.Read(buffer, 0, buffer.Length);
                    output += Encoding.UTF8.GetString(buffer, 0, noOfBytes);
                } while (noOfBytes > 0);
            }
            Assert.IsTrue(output.Equals(text1));

        }
        /// <summary>
        /// Unit test to test failure when the destination is folder
        /// </summary>
        [TestMethod]
        [ExpectedException((typeof(AdlsException)))]
        public void TestConcatException4()
        {
            string destPath = $"{UnitTestDir}/destPath1";
            List<string> srcList = new List<string>()
            {
                $"{UnitTestDir}/Source4/SrcFileEx4.txt",
                $"{UnitTestDir}/Source4/SrcFileEx5.txt"
            };
            byte[] textByte1 = Encoding.UTF8.GetBytes("Hello World");
            _adlsClient.CreateDirectory(destPath);
            using (var ostream = _adlsClient.CreateFile(srcList[0], IfExists.Overwrite, ""))
            {
                ostream.Write(textByte1, 0, textByte1.Length);
            }
            using (var ostream = _adlsClient.CreateFile(srcList[1], IfExists.Overwrite, ""))
            {
                ostream.Write(textByte1, 0, textByte1.Length);
            }
            _adlsClient.ConcatenateFiles(destPath, srcList);
        }

        /// <summary>
        /// This test is to test stream is sealed after concat. This is valid behavior only in tieredstore.
        /// </summary>
        [TestMethod]
        [ExpectedException(typeof(AdlsException))]
        public void TestConcatException5()
        {
            if (_isAccountTieredStore)
            {
                throw new AdlsException("Ignore this test, since stream is not sealed after concat in tiered store");
            }
            string destPath = $"{UnitTestDir}/destPathEx2.txt";
            List<string> srcList = new List<string>()
            {
                $"{UnitTestDir}/Source4/SrcFileEx1.txt",
                $"{UnitTestDir}/Source4/SrcFileEx2.txt"
            };
            byte[] textByte1 = Encoding.UTF8.GetBytes("Hello World");
            using (var ostream = _adlsClient.CreateFile(srcList[0], IfExists.Overwrite, ""))
            {
                ostream.Write(textByte1, 0, textByte1.Length);
            }
            using (var ostream = _adlsClient.CreateFile(srcList[1], IfExists.Overwrite, ""))
            {
                ostream.Write(textByte1, 0, textByte1.Length);
            }
            _adlsClient.ConcatenateFiles(destPath, srcList);
            using (var ostream = _adlsClient.GetAppendStream(destPath))
            {
                ostream.Write(textByte1, 0, textByte1.Length);
            }
        }

        /// <summary>
        /// Unit test to concat two file with and without deleting the source directory
        /// </summary>
        [TestMethod]
        public void TestConcatTwoFile()
        {
            TestConcatTwoFile(false, UnitTestDir + "/destPath2.txt", UnitTestDir + "/Source");
            TestConcatTwoFile(true, UnitTestDir + "/destPath3.txt", UnitTestDir + "/Source1");
            TestConcatTwoFile(false, UnitTestDir + "/destPath6.txt", UnitTestDir + "/Source1", "prefix+with,signs");
            TestConcatTwoFile(true, UnitTestDir + "/destPath7.txt", UnitTestDir + "/Source1", "prefix+with,signs");
        }

        /// <summary>
        /// Unit test to concat two file
        /// </summary>
        /// <param name="deleteSource">Whether to delete source directory</param>
        /// <param name="destPath">Destination filename</param>
        /// <param name="sourcePath">Source directory</param>
        public void TestConcatTwoFile(bool deleteSource, string destPath, string sourcePath, string sourceFileNamePrefix = "")
        {
            List<string> srcList = new List<string>();
            string srcFile1 = sourcePath + "/" + sourceFileNamePrefix + "srcPath1.txt";
            string text1 = RandomString(2 * 1024 * 1024);
            byte[] textByte1 = Encoding.UTF8.GetBytes(text1);
            using (var ostream = _adlsClient.CreateFile(srcFile1, IfExists.Overwrite, ""))
            {
                ostream.Write(textByte1, 0, textByte1.Length);
            }
            string srcFile2 = sourcePath + "/" + sourceFileNamePrefix + "srcPath2.txt";
            string text2 = RandomString(3 * 1024 * 1024);
            byte[] textByte2 = Encoding.UTF8.GetBytes(text2);
            using (var ostream = _adlsClient.CreateFile(srcFile2, IfExists.Overwrite, ""))
            {
                ostream.Write(textByte2, 0, textByte2.Length);
            }
            srcList.Add(srcFile1);
            srcList.Add(srcFile2);
            _adlsClient.ConcatenateFiles(destPath, srcList, deleteSource);
            string output = "";
            using (var istream = _adlsClient.GetReadStream(destPath))
            {
                int noOfBytes;
                byte[] buffer = new byte[5 * 1024 * 1024];
                do
                {
                    noOfBytes = istream.Read(buffer, 0, buffer.Length);
                    output += Encoding.UTF8.GetString(buffer, 0, noOfBytes);
                } while (noOfBytes > 0);
            }
            Assert.IsTrue(output.Equals(text1 + text2));
            try
            {
                _adlsClient.GetDirectoryEntry(srcFile1);
                Assert.Fail("The file should have been deleted so Getfilestatus should throw an exception");
            }
            catch (IOException) { }
            try
            {
                _adlsClient.GetDirectoryEntry(srcFile2);
                Assert.Fail("The file should have been deleted so Getfilestatus should throw an exception");
            }
            catch (IOException) { }
            if (deleteSource)
            {
                try
                {
                    _adlsClient.GetDirectoryEntry(sourcePath);
                    Assert.Fail("The directory should have been deleted so Getfilestatus should throw an exception");
                }
                catch (IOException) { }
            }
        }

        /// <summary>
        /// Unit test to concat three files with and without deleting the source directory
        /// </summary>
        [TestMethod]
        public void TestConcatThreeFile()
        {
            TestConcatThreeFile(false, UnitTestDir + "/destPath4.txt", UnitTestDir + "/Source2");
            TestConcatThreeFile(true, UnitTestDir + "/destPath5.txt", UnitTestDir + "/Source3");
        }

        /// <summary>
        /// Unit test to concat three files
        /// </summary>
        /// <param name="deleteSource">Whether to delete source directory</param>
        /// <param name="destPath">Destination filename</param>
        /// <param name="sourcePath">Source directory</param>
        public void TestConcatThreeFile(bool deleteSource, string destPath, string sourcePath)
        {
            List<string> srcList = new List<string>();
            string srcFile1 = sourcePath + "/srcPath3.txt";
            string text1 = RandomString(1024);
            byte[] textByte1 = Encoding.UTF8.GetBytes(text1);
            using (var ostream = _adlsClient.CreateFile(srcFile1, IfExists.Overwrite, ""))
            {
                ostream.Write(textByte1, 0, textByte1.Length);
            }
            string srcFile2 = sourcePath + "/srcPath4.txt";
            string text2 = RandomString(5 * 1024 * 1024);
            byte[] textByte2 = Encoding.UTF8.GetBytes(text2);
            using (var ostream = _adlsClient.CreateFile(srcFile2, IfExists.Overwrite, ""))
            {
                ostream.Write(textByte2, 0, textByte2.Length);
            }
            string srcFile3 = sourcePath + "/srcPath5.txt";
            string text3 = RandomString(300 * 1024);
            byte[] textByte3 = Encoding.UTF8.GetBytes(text3);
            using (var ostream = _adlsClient.CreateFile(srcFile3, IfExists.Overwrite, ""))
            {
                ostream.Write(textByte3, 0, textByte3.Length);
            }
            srcList.Add(srcFile1);
            srcList.Add(srcFile2);
            srcList.Add(srcFile3);
            _adlsClient.ConcatenateFiles(destPath, srcList, deleteSource);
            string output = "";
            using (var istream = _adlsClient.GetReadStream(destPath))
            {
                int noOfBytes;
                byte[] buffer = new byte[8 * 1024 * 1024];
                do
                {
                    noOfBytes = istream.Read(buffer, 0, buffer.Length);
                    output += Encoding.UTF8.GetString(buffer, 0, noOfBytes);
                } while (noOfBytes > 0);


            }
            Assert.IsTrue(output.Equals(text1 + text2 + text3));
            try
            {
                _adlsClient.GetDirectoryEntry(srcFile1);
                Assert.Fail("The file should have been deleted so GetFileStatus should throw an exception");
            }
            catch (IOException) { }
            try
            {
                _adlsClient.GetDirectoryEntry(srcFile2);
                Assert.Fail("The file should have been deleted so GetFileStatus should throw an exception");
            }
            catch (IOException) { }
            try
            {
                _adlsClient.GetDirectoryEntry(srcFile3);
                Assert.Fail("The file should have been deleted so GetFileStatus should throw an exception");
            }
            catch (IOException) { }
            if (deleteSource)
            {
                try
                {
                    _adlsClient.GetDirectoryEntry($"{UnitTestDir}/Source");
                    Assert.Fail("The directory should have been deleted so GetFileStatus should throw an exception");
                }
                catch (IOException) { }
            }
        }

        [TestMethod]
        public void TestConcatExisting()
        {
            string sourcePath = UnitTestDir + "/TestConcatExisting";
            List<string> srcList = new List<string>();
            string destPath = sourcePath + "/destPath.txt";
            string text1 = RandomString(1024);
            using (var writer = new StreamWriter(_adlsClient.CreateFile(destPath, IfExists.Overwrite, "")))
            {
                writer.Write(text1);
            }
            string srcFile1 = sourcePath + "/srcPath1.txt";
            string text2 = RandomString( 1024);
            using (var ostream = new StreamWriter(_adlsClient.CreateFile(srcFile1, IfExists.Overwrite, "")))
            {
                ostream.Write(text2);
            }
            string srcFile2 = sourcePath + "/srcPath2.txt";
            string text3 = RandomString(1024);
            using (var ostream = new StreamWriter(_adlsClient.CreateFile(srcFile2, IfExists.Overwrite, "")))
            {
                ostream.Write(text3);
            }
            srcList.Add(srcFile1);
            srcList.Add(srcFile2);
            _adlsClient.ConcatenateFiles(destPath, srcList);
            using (var istream = new StreamReader(_adlsClient.GetReadStream(destPath)))
            {
                Assert.IsTrue(istream.ReadToEnd().Equals(text1+text2+text3));
            }
        }
        #endregion

        #region Expiry
        /// <summary>
        /// Unit test to try set expiry time for a directory 
        /// </summary>
        [TestMethod]
        [ExpectedException(typeof(AdlsException))]
        public void TestExpiryFailDirectory()
        {
            string path = $"{UnitTestDir}/ExpiryFolder";
            _adlsClient.CreateDirectory(path, "");
            _adlsClient.SetExpiryTime(path, ExpiryOption.NeverExpire, 0);
            Assert.Fail("SetExpiry should have raised an exception as expiry cannot be set for folders");
        }
        /// <summary>
        /// Unit test to set expiry time as never
        /// </summary>
        [TestMethod]
        public void TestExpiryTimeNever()
        {
            string path = $"{UnitTestDir}/ExpiryFolder/ExpiryFile1.txt";
            string text1 = RandomString(100);
            byte[] textByte1 = Encoding.UTF8.GetBytes(text1);
            using (var ostream = _adlsClient.CreateFile(path, IfExists.Overwrite, ""))
            {
                ostream.Write(textByte1, 0, textByte1.Length);
            }
            _adlsClient.SetExpiryTime(path, ExpiryOption.NeverExpire, 0);
            DirectoryEntry diren = _adlsClient.GetDirectoryEntry(path);
            Assert.IsNull(diren.ExpiryTime);
        }
        private static bool DateTimeEquals(DateTime d1, DateTime d2)
        {
            long diff = Math.Abs(d1.Ticks - d2.Ticks) / 10000;
            return diff < 100;//Error of 100 milliseconds
        }
        /// <summary>
        /// Unit test to set expiry time as absolute
        /// </summary>
        [TestMethod]
        public void TestExpiryTimeAbsolute()
        {
            string path = $"{UnitTestDir}/ExpiryFolder/ExpiryFileAbsolute.txt";
            string text1 = RandomString(100);
            byte[] textByte1 = Encoding.UTF8.GetBytes(text1);
            using (var ostream = _adlsClient.CreateFile(path, IfExists.Overwrite, ""))
            {
                ostream.Write(textByte1, 0, textByte1.Length);
            }
            DateTime dt = DateTime.UtcNow.AddDays(1);
            long milliseconds = (long)(dt - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;
            _adlsClient.SetExpiryTime(path, ExpiryOption.Absolute, milliseconds);
            DirectoryEntry diren = _adlsClient.GetDirectoryEntry(path);
            Assert.IsNotNull(diren.ExpiryTime);
            Assert.IsTrue(DateTimeEquals(diren.ExpiryTime.Value, dt));
        }
        /// <summary>
        /// Unit test to set expiry time as relative to creation time
        /// </summary>
        [TestMethod]
        public void TestExpiryTimeRelativeCreation()
        {
            string path = $"{UnitTestDir}/ExpiryFolder/ExpiryFileRelative.txt";
            var ostream = _adlsClient.CreateFile(path, IfExists.Overwrite, "");
            DirectoryEntry diren = _adlsClient.GetDirectoryEntry(path);
            Assert.IsNotNull(diren.LastModifiedTime);
            DateTime create = diren.LastModifiedTime.Value;
            DateTime endTime = create.Add(new TimeSpan(0, 0, 0, 10));
            _adlsClient.SetExpiryTime(path, ExpiryOption.RelativeToCreationDate, 10000);
            diren = _adlsClient.GetDirectoryEntry(path);
            Assert.IsNotNull(diren.ExpiryTime);
            Assert.IsTrue(DateTimeEquals(diren.ExpiryTime.Value, endTime));
            ostream.Dispose();
        }
        /// <summary>
        /// Unit test to set expiry time as relative to current time
        /// </summary>
        [TestMethod]
        public void TestExpiryTimeRelativeNow()
        {
            string path = $"{UnitTestDir}/ExpiryFolder/ExpiryFile3.txt";
            long time = 5000;//In milliseconds: 5 seconds
            using (_adlsClient.CreateFile(path, IfExists.Overwrite, ""))
            { }
            _adlsClient.SetExpiryTime(path, ExpiryOption.RelativeToNow, time);
            Thread.Sleep((int)time + 1000);
            try
            {
                using (_adlsClient.GetAppendStream(path))
                {
                }
                Assert.Fail("File is expired and it cannot be edited so Appendstream should throw an exception");
            }
            catch (IOException)
            { }
        }
        #endregion

        #region PermissionAndAcls
        /// <summary>
        /// Unit test to try setting invalid permission
        /// </summary>
        [TestMethod]
        [ExpectedException(typeof(AdlsException))]
        public void TestSetPermissionException()
        {
            string path = $"{UnitTestDir}/SetPermission";
            string permission = "77777";
            _adlsClient.CreateDirectory(path, "");
            _adlsClient.SetPermission(path, permission);
            Assert.Fail("SetPermission should have raised an exception because permission is not valid");
        }
        /// <summary>
        /// Unit test to set permission of a directory
        /// </summary>
        [TestMethod]
        public void TestSetPermissionFolder()
        {
            string path = $"{UnitTestDir}/SetPermissionFolder";
            string originalPermission = "771";
            string permission = "772";
            _adlsClient.CreateDirectory(path, originalPermission);
            string testFile = path + "/testFile.txt";
            try
            {
                using (_adlsClient.CreateFile(testFile, IfExists.Overwrite, "776"))
                { }
            }
            catch (IOException)
            {
                Assert.Fail("Owner should have write permission so CreateFile should not raise an exception");
            }

            AdlsClient nonOwner1 = SetupNonOwnerClient1();

            try
            {
                using (nonOwner1.GetReadStream(testFile))
                {
                }

            }
            catch (IOException)
            {
                Assert.Fail("Nonowner1 should have execute permission so ReadStream should not raise an exception");
            }
            _adlsClient.SetPermission(path, permission);
            DirectoryEntry diren = _adlsClient.GetDirectoryEntry(path);
            Assert.IsTrue(diren.Permission == permission);
            try
            {
                using (nonOwner1.GetReadStream(testFile))
                {
                }
                Assert.Fail("Nonowner1 should not have execute permission so ReadStream should raise an exception");
            }
            catch (IOException)
            { }
            try
            {
                using (nonOwner1.GetAppendStream(testFile))
                { }
                Assert.Fail("Nonowner1 should not have execute permission so AppendStream should raise an exception");
            }
            catch (IOException)
            {
            }
        }
        /// <summary>
        /// Unit test to set permission of a file
        /// </summary>
        [TestMethod]
        public void TestSetPermissionFile()
        {
            string path = $"{UnitTestDir}/SetPermission.txt";
            string originalPermission = "770";
            string permission = "772";
            using (_adlsClient.CreateFile(path, IfExists.Overwrite, originalPermission))
            {

            }
            AdlsClient nonOwner1 = SetupNonOwnerClient1();
            try
            {
                using (nonOwner1.GetAppendStream(path))
                {
                }
                Assert.Fail("Nonowner1 should not have write permission so AppendStream should raise an exception");
            }
            catch (IOException)
            {

            }
            _adlsClient.SetPermission(path, permission);
            DirectoryEntry diren = _adlsClient.GetDirectoryEntry(path);
            Assert.IsTrue(diren.Permission == permission);
            try
            {
                using (nonOwner1.GetAppendStream(path))
                {
                }

            }
            catch (IOException)
            {
                Assert.Fail("Nonowner1 should have write permission so AppendStream should not raise an exception");
            }


        }
        /// <summary>
        /// Unit test to verify check access when user has no access
        /// </summary>
        [TestMethod]
        public void TestCheckNoAccess()
        {
            string path = $"{UnitTestDir}/CheckNoAccess";
            _adlsClient.CreateDirectory(path, "");
            _adlsClient.SetPermission(path, "775");
            AdlsClient nonOwner1 = SetupNonOwnerClient1();
            Assert.IsFalse(nonOwner1.CheckAccess(path, "-w-"));
            string testFile = path + "/CheckNoAccessFile.txt";
            try
            {
                using (nonOwner1.CreateFile(testFile, IfExists.Overwrite, ""))
                {

                }
                Assert.Fail("Nonowner1 should not have write access so CreateFile should raise an exception");
            }
            catch (IOException)
            { }
        }
        /// <summary>
        /// Unit test to verify check access when user has access
        /// </summary>
        [TestMethod]
        public void TestCheckAccess()
        {
            string path = $"{UnitTestDir}/CheckAccess";
            string originalPermission = "774";
            string changedPermission = "775";
            _adlsClient.CreateDirectory(path, "");
            _adlsClient.SetPermission(path, originalPermission);
            string testFile = path + "/CheckAccessFile.txt";
            string text1 = RandomString(100);
            byte[] textByte1 = Encoding.UTF8.GetBytes(text1);
            using (var ostream = _adlsClient.CreateFile(testFile, IfExists.Overwrite, originalPermission))
            {
                ostream.Write(textByte1, 0, textByte1.Length);
            }
            AdlsClient nonOwner1 = SetupNonOwnerClient1();
            Assert.IsTrue(nonOwner1.CheckAccess(path, "r--"));
            try
            {
                using (var istream = nonOwner1.GetReadStream(testFile))
                {
                    byte[] buff = new byte[25];
                    istream.Read(buff, 0, buff.Length);
                }
                Assert.Fail("nonowner1 should not have access to traverse the directory so ReadStream should not raise an exception");
            }
            catch (IOException)
            {
            }
            _adlsClient.SetPermission(path, changedPermission);
            Assert.IsTrue(nonOwner1.CheckAccess(path, "r-x"));
            try
            {
                using (nonOwner1.GetReadStream(testFile))
                {

                }
            }
            catch (IOException)
            {
                Assert.Fail("Nonowner1 should have read permission so ReadStream should not raise an exception");
            }
        }
        /// <summary>
        /// Unit test to try set wrong Acl
        /// </summary>
        [TestMethod]
        [ExpectedException(typeof(AdlsException))]
        public void TestSetAclException()
        {
            string path = $"{UnitTestDir}/SetAclEntriesException";
            _adlsClient.CreateDirectory(path, "");
            List<AclEntry> aclList = new List<AclEntry>() { new AclEntry(AclType.user, NonOwner1ObjectId, AclScope.Access, AclAction.ReadWrite) };//Non owner client 1
            _adlsClient.SetAcl(path, aclList);
            Assert.Fail("Acl List has no default permission acls so SetAcl should raise an exception");
        }
        /// <summary>
        /// Unit test to set ACL successfully
        /// </summary>
        [TestMethod]
        public void TestSetAcl()
        {
            string path = $"{UnitTestDir}/SetAclEntries";
            _adlsClient.CreateDirectory(path, "");
            _adlsClient.SetPermission(path, "770");
            string testFile = path + "/SetAcl.txt";
            using (var ostream = _adlsClient.CreateFile(testFile, IfExists.Overwrite, "775"))
            {
                byte[] buff = Encoding.UTF8.GetBytes("Hello test i am");
                ostream.Write(buff, 0, buff.Length);
            }
            AdlsClient nonOwner1 = SetupNonOwnerClient1();
            Assert.IsFalse(nonOwner1.CheckAccess(path, "r--"));
            AdlsClient nonOwner2 = SetupNonOwnerClient2();
            Assert.IsFalse(nonOwner2.CheckAccess(path, "r--"));
            List<AclEntry> aclList = new List<AclEntry>() {
            new AclEntry(AclType.user, NonOwner1ObjectId, AclScope.Access, AclAction.ReadWrite),
            //Add the default permission ACLs
            new AclEntry(AclType.user, "", AclScope.Access, AclAction.All),
            new AclEntry(AclType.group, "", AclScope.Access, AclAction.All),
            new AclEntry(AclType.other, "", AclScope.Access, AclAction.None)
            };
            _adlsClient.SetAcl(path, aclList);
            //Non owenr 1
            Assert.IsTrue(nonOwner1.CheckAccess(path, "rw-"));
            Assert.IsFalse(nonOwner1.CheckAccess(path, "--x"));
            try
            {
                byte[] buff = new byte[25];
                using (var istream = nonOwner1.GetReadStream(testFile))
                {
                    istream.Read(buff, 0, buff.Length);
                }
                Assert.Fail("nonowner1 should not have execute permission on the directory so ReadStream should raise an exception");
            }
            catch (IOException)
            {
            }
            //Non owner 2
            Assert.IsFalse(nonOwner2.CheckAccess(path, "--x"));
            Assert.IsFalse(nonOwner2.CheckAccess(path, "rw-"));
            aclList.Clear();
            aclList.Add(new AclEntry(AclType.user, NonOwner2ObjectId, AclScope.Access, AclAction.ExecuteOnly));
            //Add the default permission ACLs
            aclList.Add(new AclEntry(AclType.user, "", AclScope.Access, AclAction.All));
            aclList.Add(new AclEntry(AclType.group, "", AclScope.Access, AclAction.All));
            aclList.Add(new AclEntry(AclType.other, "", AclScope.Access, AclAction.None));
            _adlsClient.SetAcl(path, aclList);

            Assert.IsFalse(nonOwner1.CheckAccess(path, "rw-"));
            Assert.IsFalse(nonOwner1.CheckAccess(path, "--x"));

            Assert.IsTrue(nonOwner2.CheckAccess(path, "--x"));
            Assert.IsFalse(nonOwner2.CheckAccess(path, "rw-"));
            try
            {
                byte[] buff = new byte[25];
                using (var istream = nonOwner2.GetReadStream(testFile))
                {
                    istream.Read(buff, 0, buff.Length);
                }

            }
            catch (IOException)
            {
                Assert.Fail("The nonowner2 should have execute permission on the directory");
            }
        }

        /// <summary>
        /// Unit test to modify ACL entries
        /// </summary>
        [TestMethod]
        public void TestModifyAcl()
        {
            TestModifyAcl(UnitTestDir + "/ModifyAclEntries.txt");
        }

        /// <summary>
        /// Unit test to modify ACL entries
        /// </summary>
        /// <param name="path">Destination path</param>
        public void TestModifyAcl(string path)
        {
            using (_adlsClient.CreateFile(path, IfExists.Overwrite, "770"))
            { }
            AdlsClient nonOwner1 = SetupNonOwnerClient1();
            Assert.IsFalse(nonOwner1.CheckAccess(path, "rw-"));
            AdlsClient nonOwner2 = SetupNonOwnerClient2();
            Assert.IsFalse(nonOwner2.CheckAccess(path, "r-x"));

            //Setup Acl Entries
            List<AclEntry> aclList = new List<AclEntry>()
            {
                new AclEntry(AclType.user, NonOwner1ObjectId, AclScope.Access,
                    AclAction.ReadWrite), //Non owner client 1
                new AclEntry(AclType.user, NonOwner2ObjectId, AclScope.Access, AclAction.ReadExecute)
            };//NonOwnerClient 2
            _adlsClient.ModifyAclEntries(path, aclList);

            Assert.IsTrue(nonOwner1.CheckAccess(path, "rw-"));
            try
            {
                using (var istream = nonOwner1.GetReadStream(path))
                {
                    byte[] buff = new byte[25];
                    istream.Read(buff, 0, buff.Length);
                }
            }
            catch (IOException) { Assert.Fail("nonowner1 should have read permission so ReadStream should not raise an exception"); }
            try
            {
                using (var istream = nonOwner1.GetReadStream(path))
                {
                    byte[] buff = new byte[25];
                    istream.Read(buff, 0, buff.Length);
                }
            }
            catch (IOException) { Assert.Fail("nonowner1 should have read permission so ReadStream should not raise an exception"); }
            Assert.IsFalse(nonOwner1.CheckAccess(path, "--x"));

            Assert.IsTrue(nonOwner2.CheckAccess(path, "--x"));
            Assert.IsFalse(nonOwner2.CheckAccess(path, "rw-"));

        }
        /// <summary>
        /// Unit test to modify Acl entries for a group
        /// </summary>
        [TestMethod]
        public void TestModifyAclGroup()
        {
            string path = $"{UnitTestDir}/ModifyAclEntryGroup.txt";
            using (_adlsClient.CreateFile(path, IfExists.Overwrite, "700"))
            {

            }
            AdlsClient nonOwner2 = SetupNonOwnerClient2();
            try
            {
                using (nonOwner2.GetAppendStream(path))
                {

                }
                Assert.Fail("Nonowner2 should not have write permission so AppendStream should raise an exception");
            }
            catch (IOException)
            {
            }
            List<AclEntry> aclList = new List<AclEntry>() {
            new AclEntry(AclType.group, Group1Id, AclScope.Access, AclAction.ReadWrite)};//Non owner client 2
            _adlsClient.ModifyAclEntries(path, aclList);
            try
            {
                using (nonOwner2.GetAppendStream(path))
                {

                }

            }
            catch (IOException)
            {
                Assert.Fail("Nonowner2 should have write permission so AppendStream should not raise an exception");
            }
            try
            {
                using (var istream = nonOwner2.GetReadStream(path))
                {
                    byte[] buffer = new byte[25];
                    istream.Read(buffer, 0, buffer.Length);
                }

            }
            catch (IOException)
            {
                Assert.Fail("Nonowner2 should not have read permission so ReadStream should not raise an exception");
            }
        }
        /// <summary>
        /// Unit test to modify Acl for Other
        /// </summary>
        [TestMethod]
        public void TestModifyAclOther()
        {
            string path = $"{UnitTestDir}/ModifyAclEntryOther.txt";
            using (_adlsClient.CreateFile(path, IfExists.Overwrite, "700"))
            { }
            AdlsClient nonOwner2 = SetupNonOwnerClient2();
            try
            {
                using (nonOwner2.GetAppendStream(path))
                { }
                Assert.Fail("Nonowner2 should not have write permission so AppendStream should raise an exception");
            }
            catch (IOException)
            {
            }
            List<AclEntry> aclList = new List<AclEntry>() {
            new AclEntry(AclType.other, "", AclScope.Access, AclAction.ReadWrite)};//Non owner client 1
            _adlsClient.ModifyAclEntries(path, aclList);
            try
            {
                using (nonOwner2.GetAppendStream(path))
                { }
            }
            catch (IOException)
            {
                Assert.Fail("Nonowner2 should have write permission so AppendStream should not raise an exception");
            }
            try
            {
                using (var istream = nonOwner2.GetReadStream(path))
                {
                    byte[] buffer = new byte[25];
                    istream.Read(buffer, 0, buffer.Length);
                }

            }
            catch (IOException)
            {
                Assert.Fail("Nonowner2 should have read permission so ReadStream should not raise an exception");
            }
        }
        /// <summary>
        /// Unit test to modify Acl Mask
        /// </summary>
        [TestMethod]
        public void TestModifyAclMask()
        {
            string path = $"{UnitTestDir}/ModifyAclEntryMask.txt";
            using (var ostream = _adlsClient.CreateFile(path, IfExists.Overwrite, "700"))
            {
                byte[] buff = Encoding.UTF8.GetBytes("Hello");
                ostream.Write(buff, 0, buff.Length);
            }
            AdlsClient nonOwner1 = SetupNonOwnerClient1();
            try
            {
                using (nonOwner1.GetAppendStream(path))
                { }
                Assert.Fail("Nonowner1 should not have write permission so AppendStream should raise an exception");
            }
            catch (IOException)
            {
            }
            List<AclEntry> aclList = new List<AclEntry>() {
            new AclEntry(AclType.mask, "", AclScope.Access, AclAction.ReadOnly),
            new AclEntry(AclType.user, NonOwner1ObjectId, AclScope.Access, AclAction.ReadWrite)};//Non owner client 1
            _adlsClient.ModifyAclEntries(path, aclList);
            try
            {
                using (nonOwner1.GetAppendStream(path))
                { }
                Assert.Fail("Nonowner1 still should not have effective write permission so AppendStream should raise an exception");
            }
            catch (IOException)
            { }
            try
            {
                using (var istream = nonOwner1.GetReadStream(path))
                {
                    byte[] buffer = new byte[25];
                    istream.Read(buffer, 0, buffer.Length);
                }

            }
            catch (IOException)
            {
                Assert.Fail("Nonowner1 should have read permission so ReadStream should not raise an exception");
            }
        }
        /// <summary>
        /// Unit test to modfy Acl Mask for group
        /// </summary>
        [TestMethod]
        public void TestAclMaskGroup()
        {
            string path = $"{UnitTestDir}/TestAclMaskGroup";
            _adlsClient.CreateDirectory(path, "");
            List<AclEntry> aclList = new List<AclEntry>() {
                new AclEntry(AclType.user, NonOwner2ObjectId, AclScope.Access, AclAction.All),
                new AclEntry(AclType.user, _nonOwner3ObjectId,AclScope.Access, AclAction.ExecuteOnly)
            };//Non owner client 2
            _adlsClient.ModifyAclEntries(path, aclList);
            string subDirec = path + "/SubDir";
            AdlsClient nonOwner2 = SetupNonOwnerClient2();
            nonOwner2.CreateDirectory(subDirec, "770");
            aclList = new List<AclEntry>()
            {
                new AclEntry(AclType.group, Group1Id, AclScope.Access,
                    AclAction.WriteExecute), //Non owner client 2 and 3
            };
            nonOwner2.ModifyAclEntries(subDirec, aclList);
            AdlsClient nonOwner3 = SetupNonOwnerClient3();
            Assert.IsTrue(nonOwner3.CheckAccess(subDirec, "-wx"));
            aclList = new List<AclEntry>()
            {
                new AclEntry(AclType.mask, "", AclScope.Access,
                    AclAction.ReadExecute), //Non owner client 2 and 3
            };
            nonOwner2.ModifyAclEntries(subDirec, aclList);
            Assert.IsFalse(nonOwner3.CheckAccess(subDirec, "-w-"));
        }
        /// <summary>
        /// Unit test to set owner group and verify the mask changed
        /// </summary>
        [TestMethod]
        public void TestSetOwnerGroupMask()
        {
            string path = $"{UnitTestDir}/TestGroupMask";
            _adlsClient.CreateDirectory(path, "");
            List<AclEntry> aclList = new List<AclEntry>() {
                new AclEntry(AclType.user, NonOwner2ObjectId, AclScope.Access, AclAction.All),
                new AclEntry(AclType.user, _nonOwner3ObjectId,AclScope.Access, AclAction.ExecuteOnly),
                new AclEntry(AclType.user, NonOwner1ObjectId,AclScope.Access, AclAction.ExecuteOnly)
            };//Non owner client 2
            _adlsClient.ModifyAclEntries(path, aclList);
            string subDirec = path + "/SubDir";
            AdlsClient nonOwner2 = SetupNonOwnerClient2();
            AdlsClient nonOwner3 = SetupNonOwnerClient3();
            AdlsClient nonOwner1 = SetupNonOwnerClient1();
            nonOwner2.CreateDirectory(subDirec, "770");
            _adlsClient.SetOwner(subDirec, "", Group1Id);
            Assert.IsTrue(nonOwner2.CheckAccess(subDirec, "rwx"));
            Assert.IsTrue(nonOwner3.CheckAccess(subDirec, "rwx"));//Because group has access
            aclList = new List<AclEntry>()
            {
                new AclEntry(AclType.mask, "", AclScope.Access,AclAction.ReadExecute),
                new AclEntry(AclType.user,NonOwner1ObjectId, AclScope.Access,AclAction.WriteExecute)
            };
            _adlsClient.ModifyAclEntries(subDirec, aclList);
            Assert.IsFalse(nonOwner3.CheckAccess(subDirec, "-w-"));//Due to mask the group permission changed
            Assert.IsTrue(nonOwner3.CheckAccess(subDirec, "r-x"));
            Assert.IsTrue(nonOwner1.CheckAccess(subDirec, "--x"));
            Assert.IsFalse(nonOwner1.CheckAccess(subDirec, "-w-"));
            Assert.IsFalse(nonOwner1.CheckAccess(subDirec, "r--"));
            Assert.IsTrue(nonOwner2.CheckAccess(subDirec, "rwx"));
        }
        /// <summary>
        /// Unit test to set Owner
        /// </summary>
        [TestMethod]
        public void TestSetOwner()
        {
            string path = $"{UnitTestDir}/TestOwner";
            _adlsClient.CreateDirectory(path, "");
            List<AclEntry> aclList = new List<AclEntry>() {
                new AclEntry(AclType.user, NonOwner2ObjectId, AclScope.Access, AclAction.All),
                new AclEntry(AclType.user, _nonOwner3ObjectId, AclScope.Access, AclAction.ExecuteOnly),
            };//Non owner client 2
            _adlsClient.ModifyAclEntries(path, aclList);
            string subDirec = path + "/SubDir";
            AdlsClient nonOwner2 = SetupNonOwnerClient2();
            nonOwner2.CreateDirectory(subDirec, "770");
            Assert.IsTrue(nonOwner2.CheckAccess(subDirec, "rwx"));
            _adlsClient.SetOwner(subDirec, _nonOwner3ObjectId, "");
            Assert.IsFalse(nonOwner2.CheckAccess(subDirec, "rwx"));
            AdlsClient nonOwner3 = SetupNonOwnerClient3();
            Assert.IsTrue(nonOwner3.CheckAccess(subDirec, "rwx"));
            string subSubFile = subDirec + "/subFile.txt";
            try
            {
                using (nonOwner3.CreateFile(subSubFile, IfExists.Overwrite, ""))
                {
                }
                using (nonOwner3.GetReadStream(subSubFile))
                {
                }
            }
            catch (Exception) { Assert.Fail("NonOwner 3 should have read write access to the subdirec so Create file and read stream should not raise an exception"); }
            DirectoryEntry dir = nonOwner3.GetDirectoryEntry(subDirec);
            Assert.IsTrue(dir.User.Equals(_nonOwner3ObjectId));
        }
        /// <summary>
        /// Unit test to set group owner
        /// </summary>
        [TestMethod]
        public void TestSetGroup()
        {
            string path = $"{UnitTestDir}/TestGroup";
            _adlsClient.CreateDirectory(path, "");
            List<AclEntry> aclList = new List<AclEntry>()
            {
                new AclEntry(AclType.user, NonOwner2ObjectId, AclScope.Access, AclAction.All),
                new AclEntry(AclType.user, _nonOwner3ObjectId, AclScope.Access, AclAction.ExecuteOnly),
            }; //Non owner client 2
            _adlsClient.ModifyAclEntries(path, aclList);
            string subDirec = path + "/SubDir";
            AdlsClient nonOwner2 = SetupNonOwnerClient2();
            nonOwner2.CreateDirectory(subDirec, "770");
            AdlsClient nonOwner3 = SetupNonOwnerClient3();
            //Nonowner3 should not have any access
            Assert.IsFalse(nonOwner3.CheckAccess(subDirec, "r--"));
            Assert.IsFalse(nonOwner3.CheckAccess(subDirec, "-w-"));
            //Nonowner3 is a part of group1
            _adlsClient.SetOwner(subDirec, "", Group1Id);
            Assert.IsTrue(nonOwner3.CheckAccess(subDirec, "rwx"));
            _adlsClient.SetPermission(subDirec, "750");
            //As we changed permission of group the permission of nonowner3 changed
            Assert.IsTrue(nonOwner3.CheckAccess(subDirec, "r-x"));
            Assert.IsFalse(nonOwner3.CheckAccess(subDirec, "-w-"));
            DirectoryEntry dir = nonOwner2.GetDirectoryEntry(subDirec);
            Assert.IsTrue(dir.Group.Equals(Group1Id));
        }
        /// <summary>
        /// Unit test for extended Acl
        /// </summary>
        [TestMethod]
        public void TestAclExtended()
        {
            string path = $"{UnitTestDir}/TestAclExtended";
            _adlsClient.CreateDirectory(path, "750");
            List<AclEntry> aclList = new List<AclEntry>()
            {
                new AclEntry(AclType.user, NonOwner2ObjectId, AclScope.Access,
                    AclAction.All), //Non owner client 2
            };
            _adlsClient.ModifyAclEntries(path, aclList);
            AdlsClient nonOwner2 = SetupNonOwnerClient2();
            string subFile1 = path + "/subFile1.txt";
            try
            {
                using (nonOwner2.CreateFile(subFile1, IfExists.Overwrite, ""))
                {
                }
            }
            catch (IOException)
            {
                Assert.Fail("This should pass because mask is rwx so it still shouldnt have effective write");
            }
            _adlsClient.SetPermission(path, "750");
            //When Acl is set and you change the group the mask is changed
            string subFile2 = path + "/subFile2.txt";
            try
            {
                using (nonOwner2.CreateFile(subFile2, IfExists.Overwrite, ""))
                {
                }
                Assert.Fail("This shouldnt pass because mask is r-x so it still shouldnt have effective write");
            }
            catch (IOException)
            { }
            List<AclEntry> aclList1 = new List<AclEntry>()
            {
                new AclEntry(AclType.user, NonOwner1ObjectId, AclScope.Access,
                    AclAction.WriteExecute), //Non owner client 2
            };
            _adlsClient.ModifyAclEntries(path, aclList1);//Mask is recalculated
            try
            {
                using (nonOwner2.CreateFile(subFile2, IfExists.Overwrite, ""))
                {
                }
            }
            catch (IOException)
            {
                Assert.Fail("Create file should not raise an exception because recalculated mask is rwx and nonowner2 still shouldnt have effective write");
            }
        }

        /// <summary>
        /// Unit test to set default Acl
        /// </summary>
        [TestMethod]
        public void TestAclDefault()
        {
            TestAclDefault(UnitTestDir + "/DefaultAclEntries");
        }

        /// <summary>
        /// Unit test to set default Acl
        /// </summary>
        /// <param name="path">Path for setting default Acl</param>
        public void TestAclDefault(string path)
        {
            _adlsClient.CreateDirectory(path, "700");
            AdlsClient nonOwner1 = SetupNonOwnerClient1();
            Assert.IsFalse(nonOwner1.CheckAccess(path, "rwx"));

            //Setup Acl Entries
            List<AclEntry> aclList = new List<AclEntry>() {
            new AclEntry(AclType.user, NonOwner1ObjectId, AclScope.Access, AclAction.WriteExecute),//Non owner client 1
            new AclEntry(AclType.user, NonOwner1ObjectId, AclScope.Default, AclAction.WriteExecute)};//Non owner client 1
            _adlsClient.ModifyAclEntries(path, aclList);
            Assert.IsTrue(nonOwner1.CheckAccess(path, "--x"));
            string subDirec = path + "/subdirec";
            string subFile = path + "/subFile";
            _adlsClient.CreateDirectory(subDirec, "");
            using (_adlsClient.CreateFile(subFile, IfExists.Overwrite, ""))
            {
            }
            Assert.IsTrue(nonOwner1.CheckAccess(subFile, "-wx"));
            Assert.IsTrue(nonOwner1.CheckAccess(subDirec, "-wx"));
            string subDirecsubFile = subDirec + "/subSubFile.txt";
            try
            {
                using (nonOwner1.CreateFile(subDirecsubFile, IfExists.Overwrite, "700"))
                {
                }
            }
            catch (IOException)
            {
                Assert.Fail("Nonowner1 should have write and execute permission on the directory so create file in the directory should not raise an exception ");
            }

        }
        /// <summary>
        /// Unit test to verify the effective access after setting default Acl for user
        /// </summary>
        [TestMethod]
        public void TestAclDefaultMode()
        {
            string path = $"{UnitTestDir}/TestAclDefaultMode";
            _adlsClient.CreateDirectory(path, "730");
            List<AclEntry> aclList = new List<AclEntry>() {
                new AclEntry(AclType.user, NonOwner1ObjectId, AclScope.Access, AclAction.WriteExecute),
                new AclEntry(AclType.user, NonOwner1ObjectId, AclScope.Default, AclAction.WriteExecute)};//Non owner client 2
            _adlsClient.ModifyAclEntries(path, aclList);
            AdlsClient nonOwner1 = SetupNonOwnerClient1();
            //Mask is -wx
            string subFile1 = path + "/subFile1.txt";
            string subFile2 = path + "/subFile2.txt";

            using (_adlsClient.CreateFile(subFile1, IfExists.Overwrite, "750"))//Mask of subFile1 is r-x
            {
            }
            using (_adlsClient.CreateFile(subFile2, IfExists.Overwrite, "730"))//Mask of subFile1 is -wx
            {
            }
            try
            {
                using (nonOwner1.GetAppendStream(subFile1))
                {
                }
                Assert.Fail("Nonowner1 shouldnt have effective write accecss so AppendStream should raise an exception");
            }
            catch (IOException)
            {
            }
            try
            {
                using (nonOwner1.GetAppendStream(subFile2))
                {
                }

            }
            catch (IOException)
            {
                Assert.Fail("Nonowner1 should have effective write accecss so AppendStream should not raise an exception");
            }
        }
        /// <summary>
        /// Unit test to set default Acl for groups
        /// </summary>
        [TestMethod]
        public void TestAclDefaultGroup()
        {
            string path = $"{UnitTestDir}/TestAclDefaultGroup";
            _adlsClient.CreateDirectory(path, "740");
            AdlsClient nonOwner2 = SetupNonOwnerClient2();
            string subDirec = path + "/subdirec";
            string subFile = path + "/subFile";

            List<AclEntry> aclList = new List<AclEntry>() {
                new AclEntry(AclType.group, Group1Id, AclScope.Access, AclAction.WriteExecute),
                new AclEntry(AclType.group, Group1Id, AclScope.Default, AclAction.All)};//Non owner client 2
            _adlsClient.ModifyAclEntries(path, aclList);
            //Mask of path is rwx
            _adlsClient.CreateDirectory(subDirec, "730");//Mask of directory becomes -wx
            using (_adlsClient.CreateFile(subFile, IfExists.Overwrite, "740"))//Mask of file becomes r--
            {
            }

            try
            {
                using (nonOwner2.GetAppendStream(subFile))
                {
                }
                Assert.Fail("Nonowner2 should have no effective access so AppendStream should raise an exception");
            }
            catch (IOException)
            {

            }
            Assert.IsTrue(nonOwner2.CheckAccess(subFile, "r--"));
            Assert.IsTrue(nonOwner2.CheckAccess(subDirec, "-wx"));
            string subDirecsubFile = subDirec + "/subSubFile.txt";
            try
            {
                using (nonOwner2.CreateFile(subDirecsubFile, IfExists.Overwrite, ""))
                {
                }
            }
            catch (IOException)
            {
                Assert.Fail("Nonowner2 should have write and default access so CreateFile should not raise an exception");
            }
        }
        /// <summary>
        /// Unit test for removing Acl
        /// </summary>
        [TestMethod]
        public void TestRemoveAcl()
        {
            string path = $"{UnitTestDir}/TestRemoveAcl.txt";
            TestModifyAcl(path);
            List<AclEntry> aclList = new List<AclEntry>() {
                new AclEntry(AclType.user, NonOwner1ObjectId, AclScope.Access, AclAction.ReadWrite)
            };//Non owner client 1
            _adlsClient.RemoveAclEntries(path, aclList);
            AdlsClient nonOwner1 = SetupNonOwnerClient1();
            Assert.IsFalse(nonOwner1.CheckAccess(path, "rw-"));
            Assert.IsFalse(nonOwner1.CheckAccess(path, "--x"));
            AdlsClient nonOwner2 = SetupNonOwnerClient2();
            Assert.IsTrue(nonOwner2.CheckAccess(path, "--x"));
            Assert.IsFalse(nonOwner2.CheckAccess(path, "rw-"));

        }
        /// <summary>
        /// Unit test for removing Default Acl
        /// </summary>
        [TestMethod]
        public void TestRemoveDefaultAcl()
        {
            string path = $"{UnitTestDir}/TestRemoveDefault";
            TestAclDefault(path);
            _adlsClient.RemoveDefaultAcls(path);
            string subDirec = path + "/subdirecNew";
            string subFile = path + "/subFileNew";
            _adlsClient.CreateDirectory(subDirec, "");
            using (_adlsClient.CreateFile(subFile, IfExists.Overwrite, ""))
            { }
            AdlsClient nonOwner1 = SetupNonOwnerClient1();
            Assert.IsFalse(nonOwner1.CheckAccess(subDirec, "-wx"));
            Assert.IsFalse(nonOwner1.CheckAccess(subFile, "-wx"));
        }
        /// <summary>
        /// Unit test for removing all acl
        /// </summary>
        [TestMethod]
        public void TestRemoveAllAcl()
        {
            string path = $"{UnitTestDir}/TesRemoveAcl";
            TestModifyAcl(path);
            _adlsClient.RemoveAllAcls(path);
            AdlsClient nonOwnerCLient1 = SetupNonOwnerClient1();
            AdlsClient nonOwnerCLient2 = SetupNonOwnerClient2();
            Assert.IsFalse(nonOwnerCLient1.CheckAccess(path, "rw-"));
            Assert.IsFalse(nonOwnerCLient2.CheckAccess(path, "--x"));

        }
        /// <summary>
        /// Unit test for getting Acl status
        /// </summary>
        [TestMethod]
        public void TestGetAclStatus()
        {
            string path = $"{UnitTestDir}/TestGetAclStatus";
            TestModifyAcl(path);
            AclStatus status = _adlsClient.GetAclStatus(path);
            Assert.IsTrue(status.Owner.Equals(OwnerObjectId));
            Assert.IsTrue(status.Permission.Equals("770"));
            Assert.IsTrue(status.Entries.Contains(new AclEntry(AclType.user, NonOwner1ObjectId, AclScope.Access, AclAction.ReadWrite)));
            Assert.IsTrue(status.Entries.Contains(new AclEntry(AclType.user, NonOwner2ObjectId, AclScope.Access, AclAction.ReadExecute)));
        }
        #endregion

        #region TrashEnumerateRestore
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
            return Tuple.Create<EnumerateDeletedItemsProgress, Progress<EnumerateDeletedItemsProgress>>(progress,progressTracker);
        }
        [TestMethod]
        public void TestRestoreDeletedItemsToOriginalDestination()
        {
            // Restore file
            string streamName = GetFileOrFolderName("file");
            SetupTrashFile(streamName, "file");

            // Enumerate goes to Alki secondaries, so sleep to let them catch up.
            Thread.Sleep(3000);

            IEnumerable<TrashEntry> trashEntries = _adlsClient.EnumerateDeletedItems(streamName, null, 1, null);
            Assert.IsTrue(trashEntries.Count() == 1);
            Assert.IsTrue(trashEntries.ElementAt(0).Type == TrashEntryType.FILE);

            string restoreToken = trashEntries.ElementAt(0).TrashDirPath;
            string destPath = trashEntries.ElementAt(0).OriginalPath;
            _adlsClient.RestoreDeletedItems(restoreToken, destPath, "file");

            // Get file status on restored entry
            string path = $"{UnitTestDir}/" + streamName;
            DirectoryEntry diren = _adlsClient.GetDirectoryEntry(path);
            
            // Restore directory
            string dirName = GetFileOrFolderName("directory");
            SetupTrashFile(dirName, "directory");

            // Enumerate goes to Alki secondaries, so sleep to let them catch up.
            Thread.Sleep(3000);
            trashEntries = _adlsClient.EnumerateDeletedItems(dirName, null, 1, null);
            Assert.IsTrue(trashEntries.Count() == 1);
            Assert.IsTrue(trashEntries.ElementAt(0).Type == TrashEntryType.DIRECTORY);

            restoreToken = trashEntries.ElementAt(0).TrashDirPath;
            destPath = trashEntries.ElementAt(0).OriginalPath;
            _adlsClient.RestoreDeletedItems(restoreToken, destPath, "directory");

            // Get file status on restored entry
            path = $"{UnitTestDir}/" + dirName;
            diren = _adlsClient.GetDirectoryEntry(path);
        }

        [TestMethod]
        public void TestRestoreDeletedItemsToNewDestination()
        {
            // Restore file
            string streamName = GetFileOrFolderName("file");
            SetupTrashFile(streamName, "file");

            string path = $"{UnitTestDir}/" + streamName;
            _adlsClient.CreateFile(path, IfExists.Overwrite);

            Thread.Sleep(3000);

            IEnumerable<TrashEntry> trashEntries = _adlsClient.EnumerateDeletedItems(streamName, null, 1, null);
            Assert.IsTrue(trashEntries.Count() == 1);
            Assert.IsTrue(trashEntries.ElementAt(0).Type == TrashEntryType.FILE);

            string restoreToken = trashEntries.ElementAt(0).TrashDirPath;
            string destPath = trashEntries.ElementAt(0).OriginalPath;
            
            try
            {
                _adlsClient.RestoreDeletedItems(restoreToken, destPath, "file");
                Assert.IsTrue(false);
            }
            catch (AdlsException ex)
            {
                Assert.IsTrue(ex.HttpStatus == HttpStatusCode.Conflict);
            }

            destPath.Substring(0, destPath.LastIndexOf('/') + 1);
            String newName = GetFileOrFolderName("file");
            destPath += newName;

            _adlsClient.RestoreDeletedItems(restoreToken, destPath, "file");

            // Get file status on restored entry
            path.Substring(0, path.LastIndexOf('/') + 1);
            path += newName;
            DirectoryEntry diren = _adlsClient.GetDirectoryEntry(path);

            // Restore Directory
            string dirName = GetFileOrFolderName("directory");
            SetupTrashFile(dirName, "directory");

            path = $"{UnitTestDir}/" + dirName;
            bool result = _adlsClient.CreateDirectory(path);
            Assert.IsTrue(result);

            Thread.Sleep(3000);
            trashEntries = _adlsClient.EnumerateDeletedItems(dirName, null, 1, null);
            Assert.IsTrue(trashEntries.Count() == 1);
            Assert.IsTrue(trashEntries.ElementAt(0).Type == TrashEntryType.DIRECTORY);

            restoreToken = trashEntries.ElementAt(0).TrashDirPath;
            destPath = trashEntries.ElementAt(0).OriginalPath;
            
            try
            {
                _adlsClient.RestoreDeletedItems(restoreToken, destPath, "directory");
                Assert.IsTrue(false);
            }
            catch (AdlsException ex)
            {
                Assert.IsTrue(ex.HttpStatus == HttpStatusCode.Conflict);
            }

            destPath.Substring(0, destPath.LastIndexOf('/') + 1);
            newName = GetFileOrFolderName("file");
            destPath += newName;

            _adlsClient.RestoreDeletedItems(restoreToken, destPath, "directory");

            // Get file status on restored entry
            path.Substring(0, path.LastIndexOf('/') + 1);
            path += newName;
            diren = _adlsClient.GetDirectoryEntry(path);
        }

        [TestMethod]
        public void TestRestoreDeletedItemsFileWithOverwriteOrCopy()
        {
            // Test copy
            string streamName = GetFileOrFolderName("file");
            SetupTrashFile(streamName, "file");

            string path = $"{UnitTestDir}/" + streamName;
            _adlsClient.CreateFile(path, IfExists.Overwrite);

            Thread.Sleep(3000);

            IEnumerable<TrashEntry> trashEntries = _adlsClient.EnumerateDeletedItems(streamName, null, 1, null);
            Assert.IsTrue(trashEntries.Count() == 1);
            Assert.IsTrue(trashEntries.ElementAt(0).Type == TrashEntryType.FILE);

            string restoreToken = trashEntries.ElementAt(0).TrashDirPath;
            string destPath = trashEntries.ElementAt(0).OriginalPath;

            try
            {
                _adlsClient.RestoreDeletedItems(restoreToken, destPath, "file");
                Assert.IsTrue(false);
            }
            catch (AdlsException ex)
            {
                Assert.IsTrue(ex.HttpStatus == HttpStatusCode.Conflict);
            }

            _adlsClient.RestoreDeletedItems(restoreToken, destPath, "file", "copy");
            
            // Test overwrite
            streamName = GetFileOrFolderName("file");
            SetupTrashFile(streamName, "file");

            path = $"{UnitTestDir}/" + streamName;
            _adlsClient.CreateFile(path, IfExists.Overwrite, "777");

            Thread.Sleep(3000);
            trashEntries = _adlsClient.EnumerateDeletedItems(streamName, null, 1, null);
            Assert.IsTrue(trashEntries.Count() == 1);
            Assert.IsTrue(trashEntries.ElementAt(0).Type == TrashEntryType.FILE);

            restoreToken = trashEntries.ElementAt(0).TrashDirPath;
            destPath = trashEntries.ElementAt(0).OriginalPath;

            try
            {
                _adlsClient.RestoreDeletedItems(restoreToken, destPath, "file");
                Assert.IsTrue(false);
            }
            catch (AdlsException ex)
            {
                Assert.IsTrue(ex.HttpStatus == HttpStatusCode.Conflict);
            }

            _adlsClient.RestoreDeletedItems(restoreToken, destPath, "file", "overwrite");
        }

        [TestMethod]
        public void TestRestoreDeletedItemsDirectoryWithCopy()
        {
            // Test copy
            string dirName = GetFileOrFolderName("directory");
            SetupTrashFile(dirName, "directory");

            string path = $"{UnitTestDir}/" + dirName;
            bool result = _adlsClient.CreateDirectory(path);
            Assert.IsTrue(result);

            Thread.Sleep(3000);
            
            IEnumerable<TrashEntry> trashEntries = _adlsClient.EnumerateDeletedItems(dirName, null, 1, null);
            Assert.IsTrue(trashEntries.Count() == 1);
            Assert.IsTrue(trashEntries.ElementAt(0).Type == TrashEntryType.DIRECTORY);

            string restoreToken = trashEntries.ElementAt(0).TrashDirPath;
            string destPath = trashEntries.ElementAt(0).OriginalPath;

            try
            {
                _adlsClient.RestoreDeletedItems(restoreToken, destPath, "directory");
                Assert.IsTrue(false);
            }
            catch (AdlsException ex)
            {
                Assert.IsTrue(ex.HttpStatus == HttpStatusCode.Conflict);
            }

            _adlsClient.RestoreDeletedItems(restoreToken, destPath, "directory", "copy");

            // directory restores with overwrites not supported yet
        }

        [TestMethod]
        public void TestTrashEnumerateCancellationToken()
        {
            CancellationTokenSource source = new CancellationTokenSource();
            CancellationToken token = source.Token;

            string streamName = GetFileOrFolderName("file");
            SetupTrashFile(streamName, "file");

            source.Cancel();
            var tuple = GetProgressTracker();
           
            IEnumerable<TrashEntry> trashEntries = _adlsClient.EnumerateDeletedItems(streamName, null, 1, tuple.Item2, token);

            Thread.Sleep(3000);
            Assert.IsTrue(tuple.Item1.NumSearched == 0);
        }

        [TestMethod]
        public void TestTrashEnumerateForMultipleFileSearch()
        {
            string prefix = GetFileOrFolderName("file");

            int N = 10;
            for (int i = 0; i < N; i++)
            {
                string streamName = prefix + "_" + GetFileOrFolderName("file");
                SetupTrashFile(streamName, "file");
            }

            Thread.Sleep(3000);
            var progresstuple = GetProgressTracker();
            IEnumerable<TrashEntry> trashEntries = _adlsClient.EnumerateDeletedItems(prefix, null, N, progresstuple.Item2);

            Thread.Sleep(3000);
            Assert.IsTrue(progresstuple.Item1.NumFound == N);
            for (int i = 0; i < N; i++)
            {
                Assert.IsTrue(trashEntries.ElementAt(i).Type == TrashEntryType.FILE);
            }
        }

        [TestMethod]
        public void TestTrashEnumerateForZeroFileSearch()
        {
            var progresstuple = GetProgressTracker();
            IEnumerable<TrashEntry> trashEntries = _adlsClient.EnumerateDeletedItems("zzzzz", null, 1, progresstuple.Item2);
            Thread.Sleep(3000);
            Assert.IsTrue(progresstuple.Item1.NumFound == 0);
            Assert.IsTrue(string.IsNullOrEmpty(progresstuple.Item1.NextListAfter));
        }

        [TestMethod]
        public void TestTrashEnumerateForEmptyHint()
        {
            try
            {
                IEnumerable<TrashEntry> trashEntries = _adlsClient.EnumerateDeletedItems("", null, 1, null);
                Assert.IsTrue(false);
            }
            catch(Exception ex)
            {
                if(ex is ArgumentException)
                {
                    Assert.IsTrue(true);
                }
                else
                {
                    Assert.IsTrue(false);
                }

                return;
            }

            Assert.IsTrue(false);
        }

        [TestMethod]
        public void TestTrashEnumerateForMultipleDirectorySearch()
        {
            string prefix = GetFileOrFolderName("directory");

            int N = 10;
            for (int i = 0; i < N; i++)
            {
                string dirName = prefix + "_" + GetFileOrFolderName("directory");
                SetupTrashFile(dirName, "directory");
            }

            Thread.Sleep(3000);
            var progresstuple = GetProgressTracker();
            IEnumerable<TrashEntry> trashEntries = _adlsClient.EnumerateDeletedItems(prefix, null, N, progresstuple.Item2);

            Thread.Sleep(3000);
            Assert.IsTrue(progresstuple.Item1.NumFound == N);
            for (int i = 0; i < N; i++)
            {
                Assert.IsTrue(trashEntries.ElementAt(i).Type == TrashEntryType.DIRECTORY);
            }
        }
        
        #endregion

        private bool VerifyGuid(string objectId)
        {
            return Regex.IsMatch(objectId, @"^[{(]?[0-9A-Fa-f]{8}[-]?([0-9A-Fa-f]{4}[-]?){3}[0-9A-Fa-f]{12}[)}]?$");
        }
        
        /// <summary>
        /// Unit test for getting content summary of a directory
        /// </summary>
        [TestMethod]
        public void TestGetContentSummary()
        {
            string path = $"{UnitTestDir}/CntSum";
            int oneLevelDirecCnt = 3;
            int oneLevelFileCnt = 2;
            int recurseLevel = 2;
            int oneFileSize = 100;
            int expectedDirCnt = 0;
            int power = 1;
            for (int i = 1; i <= recurseLevel; i++)
            {
                power *= oneLevelDirecCnt;
                expectedDirCnt += power;
            }
            int expectedFileCnt = (expectedDirCnt + 1) * oneLevelFileCnt;
            int expectedFileSize = expectedFileCnt * oneFileSize;
            TestDataCreator.DataCreator.CreateDirRecursiveRemote(_adlsClient, path, recurseLevel, oneLevelDirecCnt, oneLevelFileCnt, oneLevelFileCnt, oneFileSize, oneFileSize);
            ContentSummary summary = _adlsClient.GetContentSummary(path);
            Assert.IsTrue(summary.DirectoryCount == expectedDirCnt);
            Assert.IsTrue(summary.FileCount == expectedFileCnt);
            Assert.IsTrue(summary.SpaceConsumed == expectedFileSize);
        }

        [TestMethod]
        [ExpectedException(typeof(AdlsException))]
        public void TestGetFileListStatusNotFoundException() {
            string path = $"{UnitTestDir}/TestGetFileListStatusException";
            foreach (var entry in _adlsClient.EnumerateDirectory(path)) { }
        }

        /// <summary>
        /// Unit test to get filelist status of a directory
        /// </summary>
        [TestMethod]
        public void TestGetFileListStatus()
        {
            char prefix = 'F';
            string path = $"{UnitTestDir}/{prefix}";
            int totFiles = 1;
            string filePrefix = "";
            int setListSize = 120;
            HashSet<string> hSet = new HashSet<string>();
            HashSet<string> hFullNameSet = new HashSet<string>();
            // DataCreator creates new folder/file with 
            TestDataCreator.DataCreator.CreateDirRecursiveRemote(_adlsClient, path, 0, 0, 0, 0, 0, 0, false, filePrefix);
            TestGetFileStatus(path, 0, hSet, hFullNameSet, 0, setListSize);
            TestGetFileStatus(path, 1, hSet, hFullNameSet, 0, setListSize);
            TestDataCreator.DataCreator.CreateDirRecursiveRemote(_adlsClient, path, 0, 0, totFiles, totFiles, 0, 0, false, filePrefix);
            for (int i = 0; i < totFiles; i++)
            {
                hSet.Add(prefix + (filePrefix + i + "File.txt"));
                hFullNameSet.Add(path + "/" + prefix + filePrefix + i + "File.txt");
            }
            TestGetFileStatus(path, 1, hSet, hFullNameSet, 1, setListSize);
            TestGetFileStatus(path, 2, hSet, hFullNameSet, 1, setListSize);
            totFiles = 99;
            filePrefix = "A1";
            TestDataCreator.DataCreator.CreateDirRecursiveRemote(_adlsClient, path, 0, 0, totFiles, totFiles, 0, 0, false, filePrefix);
            for (int i = 0; i < totFiles; i++)
            {
                hSet.Add(prefix + (filePrefix + i + "File.txt"));
                hFullNameSet.Add(path + "/" + prefix + filePrefix + i + "File.txt");
            }
            TestGetFileStatus(path, 50, hSet, hFullNameSet, 50, setListSize);
            TestGetFileStatus(path, 100, hSet, hFullNameSet, 100, setListSize);
            TestGetFileStatus(path, setListSize, hSet, hFullNameSet, 100, setListSize);
            totFiles = 20;
            filePrefix = "A2";
            TestDataCreator.DataCreator.CreateDirRecursiveRemote(_adlsClient, path, 0, 0, totFiles, totFiles, 0, 0, false, filePrefix);
            for (int i = 0; i < totFiles; i++)
            {
                hSet.Add(prefix + (filePrefix + i + "File.txt"));
                hFullNameSet.Add(path + "/" + prefix + filePrefix + i + "File.txt");
            }
            TestGetFileStatus(path, setListSize - 1, hSet, hFullNameSet, setListSize - 1, setListSize);
            TestGetFileStatus(path, setListSize, hSet, hFullNameSet, setListSize, setListSize);
            TestGetFileStatus(path, setListSize + 1, hSet, hFullNameSet, setListSize, setListSize);
            totFiles = 80;
            filePrefix = "A3";
            TestDataCreator.DataCreator.CreateDirRecursiveRemote(_adlsClient, path, 0, 0, totFiles, totFiles, 0, 0, false, filePrefix);
            for (int i = 0; i < totFiles; i++)
            {
                hSet.Add(prefix + (filePrefix + i + "File.txt"));
                hFullNameSet.Add(path + "/" + prefix + filePrefix + i + "File.txt");
            }
            TestGetFileStatus(path, 100, hSet, hFullNameSet, 100, setListSize);
            TestGetFileStatus(path, setListSize, hSet, hFullNameSet, setListSize, setListSize);
            TestGetFileStatus(path, 200, hSet, hFullNameSet, 200, setListSize);
            TestGetFileStatus(path, 201, hSet, hFullNameSet, 200, setListSize);
            totFiles = 100;
            filePrefix = "A4";
            TestDataCreator.DataCreator.CreateDirRecursiveRemote(_adlsClient, path, 0, 0, totFiles, totFiles, 0, 0, false, filePrefix);
            for (int i = 0; i < totFiles; i++)
            {
                hSet.Add(prefix + (filePrefix + i + "File.txt"));
                hFullNameSet.Add(path + "/" + prefix + filePrefix + i + "File.txt");
            }
            TestGetFileStatus(path, 100, hSet, hFullNameSet, 100, setListSize);
            TestGetFileStatus(path, setListSize, hSet, hFullNameSet, setListSize, setListSize);
            TestGetFileStatus(path, 200, hSet, hFullNameSet, 200, setListSize);
            TestGetFileStatus(path, 2 * setListSize, hSet, hFullNameSet, 2 * setListSize, setListSize);
            TestGetFileStatus(path, 300, hSet, hFullNameSet, 300, setListSize);
            TestGetFileStatus(path, 400, hSet, hFullNameSet, 300, setListSize);
            TestListStatusUsingCore(path, 400, hSet, hFullNameSet, 300);
        }

        public static void TestGetFileStatus(string path, int maxEntries, HashSet<string> hSet, HashSet<string> fullNamehSet, int expectedEntries, int setListSize = 100)
        {
            TestGetFileStatusStandard(path, maxEntries, hSet, fullNamehSet, expectedEntries, setListSize);
            TestGetFileStatusMinimal(path, maxEntries, hSet, fullNamehSet, expectedEntries, setListSize);
        }


        public static void TestGetFileStatusStandard(string path, int maxEntries, HashSet<string> hSet, HashSet<string> fullNamehSet, int expectedEntries, int setListSize = 100)
        {
            int count = 0;
            var fop = _adlsClient.EnumerateDirectory(path, maxEntries, "", "");
            var en = fop.GetEnumerator();
            ((FileStatusList<DirectoryEntry>)en).ListSize = setListSize;

            while (en.MoveNext())
            {
                var dir = en.Current;
                if (!hSet.Contains(dir.Name))
                {
                    Assert.Fail(dir.Name + ": The file should have been in the hashset");
                }
                if (!fullNamehSet.Contains(dir.FullName))
                {
                    Assert.Fail(dir.FullName + ": The file fullname should have been in the hashset");
                }
                if (dir.Type != DirectoryEntryType.FILE)
                {
                    Assert.Fail(dir.Name + " should be file");
                }
                count++;
            }
            Assert.IsTrue(count == expectedEntries);
        }

        internal static void TestListStatusUsingCore(string path, int maxEntries, HashSet<string> hSet, HashSet<string> fullNamehSet, int expectedEntries)
        {
            int count = 0;
            var resp = new OperationResponse();
            var entries = Core.ListStatusAsync(path, "", "", maxEntries, UserGroupRepresentation.ObjectID, _adlsClient, new RequestOptions(), resp).GetAwaiter().GetResult();
            foreach(var dir in entries)
            {
                if (!hSet.Contains(dir.Name))
                {
                    Assert.Fail(dir.Name + ": The file should have been in the hashset");
                }
                if (!fullNamehSet.Contains(dir.FullName))
                {
                    Assert.Fail(dir.FullName + ": The file fullname should have been in the hashset");
                }
                if (dir.Type != DirectoryEntryType.FILE)
                {
                    Assert.Fail(dir.Name + " should be file");
                }
                count++;
            }
            Assert.IsTrue(count == expectedEntries);
        }

        internal static void TestGetFileStatusMinimal(string path, int maxEntries, HashSet<string> hSet, HashSet<string> fullNamehSet, int expectedEntries, int setListSize = 100)
        {
            int count = 0;
            var fop = _adlsClient.EnumerateDirectory(path, maxEntries, "", "", selection: Selection.Minimal);
            var en = fop.GetEnumerator();
            ((FileStatusList<DirectoryEntry>)en).ListSize = setListSize;

            while (en.MoveNext())
            {
                var dir = en.Current;
                if (!hSet.Contains(dir.Name))
                {
                    Assert.Fail(dir.Name + ": The file should have been in the hashset");
                }
                if (!fullNamehSet.Contains(dir.FullName))
                {
                    Assert.Fail(dir.FullName + ": The file fullname should have been in the hashset");
                }
                if (dir.Type != DirectoryEntryType.FILE)
                {
                    Assert.Fail(dir.Name + " should be file");
                }
                Assert.AreEqual(dir.ExpiryTime, null);
                Assert.AreEqual(dir.Group, null);
                Assert.AreEqual(dir.HasAcl, false);
                Assert.AreEqual(dir.LastAccessTime, null);
                Assert.AreEqual(dir.LastModifiedTime, null);
                Assert.AreEqual(dir.Length, 0);
                Assert.AreEqual(dir.Permission, null);
                Assert.AreEqual(dir.User, null);
                count++;
            }
            Assert.IsTrue(count == expectedEntries);
        }

        [ClassCleanup]
        public static void CleanTests()
        {
            _adlsClient.DeleteRecursive(UnitTestDir);
            var nonOwnerAclSpec = new List<AclEntry>
            {
                new AclEntry(AclType.user, NonOwner1ObjectId, AclScope.Access, AclAction.ExecuteOnly),
                new AclEntry(AclType.user, NonOwner2ObjectId, AclScope.Access, AclAction.ExecuteOnly),
                new AclEntry(AclType.user, _nonOwner3ObjectId, AclScope.Access, AclAction.ExecuteOnly),
                new AclEntry(AclType.group, Group1Id, AclScope.Access, AclAction.ExecuteOnly)
            };
            _adlsClient.RemoveAclEntries("/", nonOwnerAclSpec);
        }
    }
}
