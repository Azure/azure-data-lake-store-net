using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using Microsoft.Azure.DataLake.Store.Acl;
using Microsoft.Azure.DataLake.Store.RetryPolicies;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MockServer;

namespace Microsoft.Azure.DataLake.Store.UnitTest
{
    /// <summary>
    /// Tests retry mechanisms, token cancellation, Account validation and Acl serializing deserializing
    /// </summary>
    [TestClass]
    public class CoreUnitTest
    {
        private static Process _cmdProcess;
        private const int NumTests = 5;
        [ClassInitialize]
        public static void Setup(TestContext context)
        {
            _cmdProcess = new Process();
            ProcessStartInfo pInfo = new ProcessStartInfo()
            {
                FileName = "cmd.exe",
                Verb = "runas",
                RedirectStandardInput = true,
                UseShellExecute = false
            };
            _cmdProcess.StartInfo = pInfo;
            _cmdProcess.Start();
            string cmdPart1 = "netsh http add urlacl url=http://*:808";
            string cmdPart2 = "user=" + Environment.UserDomainName + "\\" + Environment.UserName + " listen=yes";
            StreamWriter sw = _cmdProcess.StandardInput;

            if (sw.BaseStream.CanWrite)
            {
                for (int i = 0; i < NumTests; i++)
                {
                    sw.WriteLine(cmdPart1 + i + "/ " + cmdPart2);
                }
            }
        }
        /// <summary>
        /// Unit test to try creating an invalid account
        /// </summary>
        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestInvalidAccnt1()
        {
            AdlsClient.CreateClient("contoso.com_yt;", "Test");
        }
        /// <summary>
        /// Unit test to create an invalid account
        /// </summary>
        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestInvalidAccnt2()
        {
            AdlsClient.CreateClient("contoso..com", "Test");
        }
        /// <summary>
        /// Unit test to create an invalid account
        /// </summary>
        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public void TestInvalidAccnt3()
        {
            AdlsClient.CreateClient("contoso_datalakestore.net", "Test");
        }
        [TestMethod]
        public void TestValidAccnt()
        {
            AdlsClient.CreateClient("contoso.cabostore.net", "Test");
            AdlsClient.CreateClient("contoso.dogfood.com.net", "Test");
            AdlsClient.CreateClient("contoso-test.azure-data.net", "test");
        }
        /// <summary>
        /// Unit test to test the exponential retry mechanism - 4 retries
        /// </summary>
        [TestMethod]
        public void TestRetry()
        {
            int port = 8080;
            AdlsClient adlsClient = AdlsClient.CreateClientWithoutAccntValidation(MockWebServer.Host + ":" + port, "Test Token");
            MockWebServer server = new MockWebServer(port);
            server.StartServer();
            server.EnqueMockResponse(new MockResponse(502, "Bad Gateway"));
            server.EnqueMockResponse(new MockResponse(503, "Service Unavailable"));
            server.EnqueMockResponse(new MockResponse(504, "Gateway Timeout"));
            server.EnqueMockResponse(new MockResponse(503, "Service Unavailable"));
            server.EnqueMockResponse(new MockResponse(503, "Service Unavailable"));
            RequestOptions req = new RequestOptions(new ExponentialRetryPolicy());
            OperationResponse resp = new OperationResponse();
            adlsClient.SetInsecureHttp();
            Stopwatch watch = Stopwatch.StartNew();
            Core.AppendAsync("/Test/dir", null, null, SyncFlag.DATA, 0, null, -1, 0, adlsClient, req, resp).GetAwaiter()
                .GetResult();
            watch.Stop();
            long time = watch.ElapsedMilliseconds;
            Assert.IsTrue(time >= 83500 && time <= 86500);
            Assert.IsTrue(resp.HttpStatus == (HttpStatusCode)503);
            Assert.IsTrue(resp.HttpMessage.Equals("Service Unavailable"));
            Assert.IsTrue(resp.Retries == 4);
            server.StopServer();
        }
        /// <summary>
        /// Unit test to test the exponential retry mechanism for 408
        /// </summary>
        [TestMethod]
        public void TestRestry1()
        {
            int port = 8081;
            AdlsClient adlsClient = AdlsClient.CreateClientWithoutAccntValidation(MockWebServer.Host + ":" + port, "Test Token");
            MockWebServer server = new MockWebServer(port);
            server.StartServer();
            server.EnqueMockResponse(new MockResponse(408, "Request Timeout"));
            server.EnqueMockResponse(new MockResponse(408, "Request Timeout"));
            RequestOptions req = new RequestOptions(new ExponentialRetryPolicy(1, 1000));
            OperationResponse resp = new OperationResponse();
            adlsClient.SetInsecureHttp();
            //Core.AppendAsync()
            Core.AppendAsync("/Test/dir", null, null, SyncFlag.DATA, 0, null, -1, 0, adlsClient, req, resp).GetAwaiter()
                .GetResult();
            Assert.IsTrue(resp.HttpStatus == (HttpStatusCode)408);
            Assert.IsTrue(resp.HttpMessage.Equals("Request Timeout"));
            Assert.IsTrue(resp.Retries == 1);
            server.StopServer();
        }
        /// <summary>
        /// Unit test to test the exponential retry mechanism for 429
        /// </summary>
        [TestMethod]
        public void TestRestry2()
        {
            int port = 8082;
            AdlsClient adlsClient = AdlsClient.CreateClientWithoutAccntValidation(MockWebServer.Host + ":" + port, "Test Token");
            MockWebServer server = new MockWebServer(port);
            server.StartServer();
            server.EnqueMockResponse(new MockResponse(429, "Too Many Requests"));
            server.EnqueMockResponse(new MockResponse(429, "Too Many Requests"));
            RequestOptions req = new RequestOptions(new ExponentialRetryPolicy(1, 1000));
            OperationResponse resp = new OperationResponse();
            adlsClient.SetInsecureHttp();
            Core.AppendAsync("/Test/dir", null, null, SyncFlag.DATA, 0, null, -1, 0, adlsClient, req, resp).GetAwaiter()
                .GetResult();
            Assert.IsTrue(resp.HttpStatus == (HttpStatusCode)429);
            Assert.IsTrue(resp.HttpMessage.Equals("Too Many Requests"));
            Assert.IsTrue(resp.Retries == 1);
            server.StopServer();
        }
        /// <summary>
        /// Unit test to test the exponential retry mechanism for 502
        /// </summary>
        [TestMethod]
        public void TestRestry3()
        {
            int port = 8083;
            AdlsClient adlsClient = AdlsClient.CreateClientWithoutAccntValidation(MockWebServer.Host + ":" + port, "Test Token");
            MockWebServer server = new MockWebServer(port);
            server.StartServer();
            server.EnqueMockResponse(new MockResponse(502, "Bad Gateway"));
            server.EnqueMockResponse(new MockResponse(200, "OK"));

            adlsClient.SetInsecureHttp();
            AdlsOutputStream ostream = null;
            try
            {
                ostream = adlsClient.CreateFile("/Test/dir", IfExists.Overwrite, "");
            }
            catch (IOException)
            {
                Assert.Fail("The request should have passed");
            }
            server.EnqueMockResponse(new MockResponse(502, "Bad Gateway"));
            server.EnqueMockResponse(new MockResponse(503, "Service Unavailable"));
            server.EnqueMockResponse(new MockResponse(504, "Gateway Timeout"));
            server.EnqueMockResponse(new MockResponse(503, "Service Unavailable"));
            server.EnqueMockResponse(new MockResponse(503, "Service Unavailable"));
            try
            {
                byte[] buff = Encoding.UTF8.GetBytes(SdkUnitTest.RandomString(5 * 1024 * 1024));
                ostream.Write(buff, 0, buff.Length);
                Assert.Fail("The request should not have passed");
            }
            catch (IOException)
            {

            }
            server.EnqueMockResponse(new MockResponse(200, "OK"));
            try
            {
                ostream.Dispose();
            }
            catch (IOException) { Assert.Fail("This request should have passed"); }
            server.StopServer();
        }
        /// <summary>
        /// Unit test to test cancellation
        /// </summary>
        [TestMethod]
        public void TestCancelation()
        {
            int port = 8084;
            MockWebServer server = new MockWebServer(port);
            server.StartServer();
            server.EnqueMockResponse(new MockResponse(200, "OK"));
            AdlsClient adlsClient = AdlsClient.CreateClientWithoutAccntValidation(MockWebServer.Host + ":" + port, "Test Token");
            CancellationTokenSource source = new CancellationTokenSource();
            RequestState state = new RequestState()
            {
                AdlsClient = adlsClient,
                CancelToken = source.Token
            };
            Thread worker = new Thread(run);
            worker.Start(state);
            Thread.Sleep(60000);
            source.Cancel();
            Stopwatch watch = Stopwatch.StartNew();
            worker.Join();
            watch.Stop();
            Assert.IsTrue(watch.ElapsedMilliseconds < 10000);
            Assert.IsNotNull(state.AdlsClient);
            Assert.IsInstanceOfType(state.Ex, typeof(OperationCanceledException));
        }

        private void run(object data)
        {
            RequestState state = data as RequestState;

            if (state == null)
            {
                return;
            }
            AdlsClient adlsClient = state.AdlsClient;
            RequestOptions req = new RequestOptions(new ExponentialRetryPolicy());
            OperationResponse resp = new OperationResponse();
            adlsClient.SetInsecureHttp();
            byte[] ip = Encoding.UTF8.GetBytes("wait:300");
            Core.AppendAsync("/Test/dir", null, null, SyncFlag.DATA, 0, ip, 0, ip.Length, adlsClient, req, resp, state.CancelToken).GetAwaiter()
                .GetResult();
            state.Ex = resp.Ex;
        }

        private class RequestState
        {
            public AdlsClient AdlsClient { set; get; }
            public Exception Ex { set; get; }
            public CancellationToken CancelToken { get; set; }
        }
        /// <summary>
        /// Unit test serializing and deserializing Acl
        /// </summary>
        [TestMethod]
        public void TestAclSerializeUser()
        {
            Assert.IsTrue(CompareAclEntry("user:userid1  :r-x", "user:userid1:r-x"));
            Assert.IsTrue(CompareAclEntry("user:  userid2  :-wX", "user:userid2:-wx"));
            Assert.IsTrue(CompareAclEntry("user::rwx  ", "user::rwx"));
            Assert.IsTrue(CompareAclEntry("group :groupid1 :r--", "group:groupid1:r--"));
            Assert.IsTrue(CompareAclEntry("group:  :-w-", "group::-w-"));
            Assert.IsTrue(CompareAclEntry("other:  :--x", "other::--x"));
            Assert.IsTrue(CompareAclEntry("mask:  :RW-", "mask::rw-"));
        }

        public static bool CompareAclEntry(string actualString, string expectedString)
        {
            string retResult = AclEntry.ParseAclEntryString(actualString, false).ToString();
            return retResult.Equals(expectedString);
        }

        [ClassCleanup]
        public static void CleanTests()
        {
            string cmdPart1 = "netsh http delete urlacl url=http://*:808";
            using (StreamWriter sw = _cmdProcess.StandardInput)
            {
                if (sw.BaseStream.CanWrite)
                {
                    for (int i = 0; i < NumTests; i++)
                    {
                        sw.WriteLine(cmdPart1 + i + "/");
                    }
                }
            }
            _cmdProcess.Close();
        }
    }
}
