using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Azure.DataLake.Store.UnitTest
{
    [TestClass]
    public class LongRunningUnitTest
    {
        private static AdlsClient _adlsClient;
        private static string BasePath;

        private static string RemotePath;

        [ClassInitialize]
        public static void SetupTest(TestContext context)
        {
            BasePath = context.Properties["BasePath"].ToString();
            RemotePath = "/" + BasePath + "/LongRunningUnitTest" + SdkUnitTest.TestId;
            _adlsClient = SdkUnitTest.SetupSuperClient();
            _adlsClient.DeleteRecursive(RemotePath);
            _adlsClient.CreateDirectory(RemotePath);
            AdlsClient.ConcatenateStreamListThreshold = 10;
        }

        [TestMethod]
        public void ParallelConcatenate()
        {
            string path = $"{RemotePath}/A";
            string destination = $"{RemotePath}/Concatdest";
            int countFile = 400;
            string text = "ParallelConcat";
            TestDataCreator.DataCreator.CreateDirRecursiveRemote(_adlsClient, path, 0, 0, countFile, countFile, 0, 0, false, "", 100, text);
            var list = new List<string>();
            string expectedOutput = "";
            for (int i = 0; i < countFile; i++)
            {
                list.Add($"{path}/A" + i + "File.txt");
                expectedOutput += text;
            }

            _adlsClient.ConcatenateFilesParallelAsync(destination, list, true).GetAwaiter().GetResult();

            string actualOutput;
            using (var reader = new StreamReader(_adlsClient.GetReadStream(destination)))
            {
                actualOutput = reader.ReadToEnd();
            }
            Assert.IsTrue(actualOutput.Equals(expectedOutput)); 
        }

        [ClassCleanup]
        public static void CleanTests()
        {
            _adlsClient.DeleteRecursive(RemotePath);
            AdlsClient.ConcatenateStreamListThreshold = 100;
        }
    }
}
