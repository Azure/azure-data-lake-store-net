using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Http;
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
        /// <summary>
        /// Adls Client
        /// </summary>
        private static AdlsClient _adlsClient = SdkUnitTest.SetupSuperClient();
        private static Process _cmdProcess;
        private const int NumTests = 8;
        private static string TestToken = Guid.NewGuid().ToString();
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
            AdlsClient.CreateClient("contoso-test.Azuredatalakestore.eglax.inc.bov", "test");
        }

        [TestMethod]
        public void TestPathMissingRootSeparator()
        {
            string path = "CorePathMissingRootSeparator" + SdkUnitTest.TestId;
            string direcPath = path + "/directory";
            Assert.IsTrue(_adlsClient.CreateDirectory(direcPath));
            string filePath = path + "/file";
            string text = "First Line";
            using (var writer = new StreamWriter(_adlsClient.CreateFile(filePath, IfExists.Overwrite)))
            {
                writer.Write(text);
            }

            using (var reader = new StreamReader(_adlsClient.GetReadStream(filePath)))
            {
                string output = reader.ReadToEnd();
                Assert.IsTrue(text.Equals(output));
            }

            _adlsClient.DeleteRecursive(path);
        }

        /// <summary>
        /// Unit test to test the exponential retry mechanism - 4 retries
        /// </summary>
        [TestMethod]
        public void TestRetry()
        {
            int port = 8080;
            AdlsClient adlsClient = AdlsClient.CreateClientWithoutAccntValidation(MockWebServer.Host + ":" + port, TestToken);
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
            AdlsClient adlsClient = AdlsClient.CreateClientWithoutAccntValidation(MockWebServer.Host + ":" + port, TestToken);
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
            AdlsClient adlsClient = AdlsClient.CreateClientWithoutAccntValidation(MockWebServer.Host + ":" + port, TestToken);
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
            AdlsClient adlsClient = AdlsClient.CreateClientWithoutAccntValidation(MockWebServer.Host + ":" + port, TestToken);
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
        public void TestCancellation()
        {
            int port = 8084;
            MockWebServer server = new MockWebServer(port);
            server.StartServer();
            server.EnqueMockResponse(new MockResponse(200, "OK"));
            AdlsClient adlsClient = AdlsClient.CreateClientWithoutAccntValidation(MockWebServer.Host + ":" + port, TestToken);
            CancellationTokenSource source = new CancellationTokenSource();
            RequestState state = new RequestState()
            {
                AdlsClient = adlsClient,
                CancelToken = source.Token
            };
            Thread worker = new Thread(run);
            worker.Start(state);
            Thread.Sleep(10000);
            Stopwatch watch = Stopwatch.StartNew();
            source.Cancel();
            worker.Join();
            watch.Stop();
            Assert.IsTrue(watch.ElapsedMilliseconds < 1000);
            Assert.IsNotNull(state.AdlsClient);
            Assert.IsInstanceOfType(state.Ex, typeof(OperationCanceledException));
            server.StopAbruptly();
        }

        [TestMethod]
        public void TestTimeout()
        {
            int port = 8085;
            MockWebServer server = new MockWebServer(port);
            server.StartServer();
            server.EnqueMockResponse(new MockResponse(200, "OK"));
            AdlsClient adlsClient = AdlsClient.CreateClientWithoutAccntValidation(MockWebServer.Host + ":" + port, TestToken);
            CancellationTokenSource source = new CancellationTokenSource();
            RequestState state = new RequestState()
            {
                AdlsClient = adlsClient,
                CancelToken = source.Token,
                Timeout = 5,
                IsRetry = false
            };
            Thread worker = new Thread(runClientTimeout);
            worker.Start(state);
            Stopwatch watch = Stopwatch.StartNew();
            worker.Join();
            watch.Stop();
            Assert.IsTrue(watch.ElapsedMilliseconds < 7000);
            Assert.IsNotNull(state.AdlsClient);
            Assert.IsInstanceOfType(state.Ex, typeof(Exception));
            Assert.IsTrue(state.Ex.Message.Contains("Operation timed out"));
            server.StopAbruptly();
        }
#if NET452
        [TestMethod]
        public void TestTimeoutNet452Issue()
        {
            int port = 8086;
            MockWebServer server = new MockWebServer(port, true);
            server.StartServer();
            server.EnqueMockResponse(new MockResponse(200, "OK", "wait:30"));
            AdlsClient adlsClient = AdlsClient.CreateClientWithoutAccntValidation(MockWebServer.Host + ":" + port, TestToken);
            adlsClient.SetInsecureHttp();
            byte[] buff = new byte[4*1024*1024];
            var resp = new OperationResponse();
            Stopwatch watch = Stopwatch.StartNew();
            Core.OpenAsync("/test", null, 0, buff, 0, 4 * 1024 * 1024, adlsClient, new RequestOptions(null, new TimeSpan(0, 0, 10), new NoRetryPolicy()), resp).GetAwaiter().GetResult();
            watch.Stop();
            Assert.IsTrue(watch.ElapsedMilliseconds > 30000);
        }
#endif
        [TestMethod]
        public void TestConnectionBroken()
        {
            int port = 8087;
            MockWebServer server = new MockWebServer(port);
            server.StartServer();
            server.EnqueMockResponse(new MockResponse(200, "OK"));
            AdlsClient adlsClient = AdlsClient.CreateClientWithoutAccntValidation(MockWebServer.Host + ":" + port, TestToken);
            CancellationTokenSource source = new CancellationTokenSource();
            RequestState state = new RequestState()
            {
                AdlsClient = adlsClient,
                CancelToken = source.Token,
                IsRetry = false
            };
            Thread worker = new Thread(run);
            worker.Start(state);
            Thread.Sleep(5000);
            server.StopAbruptly();
            worker.Join();
            Assert.IsNotNull(state.AdlsClient);
            Assert.IsTrue(state.IsConnectionFailure);
        }

        private void run(object data)
        {
            RequestState state = data as RequestState;

            if (state == null)
            {
                return;
            }
            AdlsClient adlsClient = state.AdlsClient;
            OperationResponse resp = new OperationResponse();
            adlsClient.SetInsecureHttp();
            RequestOptions req = null;
            if (state.Timeout != -1)
            {
                req = new RequestOptions(Guid.NewGuid().ToString(), new TimeSpan(0, 0, state.Timeout), state.IsRetry ? new ExponentialRetryPolicy() : (RetryPolicy)new NoRetryPolicy());
            }
            else
            {
                req = new RequestOptions(state.IsRetry ? new ExponentialRetryPolicy() : (RetryPolicy)new NoRetryPolicy());
            }
            byte[] ip = Encoding.UTF8.GetBytes("wait:300");
            Core.AppendAsync("/Test/dir", null, null, SyncFlag.DATA, 0, ip, 0, ip.Length, adlsClient, req, resp, state.CancelToken).GetAwaiter().GetResult();
            state.Ex = resp.Ex;
            state.IsConnectionFailure = resp.ConnectionFailure;
        }

        private void runClientTimeout(object data)
        {
            RequestState state = data as RequestState;

            if (state == null)
            {
                return;
            }
            AdlsClient adlsClient = state.AdlsClient;
            OperationResponse resp = new OperationResponse();
            adlsClient.SetInsecureHttp();
            if (state.Timeout != -1)
            {
                adlsClient.SetPerRequestTimeout(new TimeSpan(0, 0, state.Timeout));
            }
            byte[] ip = Encoding.UTF8.GetBytes("wait:300");
            try
            {
                adlsClient.ConcurrentAppendAsync("/Test/dir", true, ip, 0, ip.Length, state.CancelToken).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                state.Ex = ex;
            }
        }

        private class RequestState
        {
            public AdlsClient AdlsClient { set; get; }
            public Exception Ex { set; get; }
            public CancellationToken CancelToken { get; set; }
            public int Timeout { get; set; } = -1; // In seconds
            public bool IsRetry { get; set; } = true;
            public bool IsConnectionFailure { get; set; } = false;
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

        [TestMethod]
        public void TestListStatusWithArrayInResponse()
        {
            int port = 8087;
            AdlsClient adlsClient = AdlsClient.CreateClientWithoutAccntValidation(MockWebServer.Host + ":" + port, TestToken);
            MockWebServer server = new MockWebServer(port);
            server.StartServer();
            string liststatusOutput = "{\"FileStatuses\":{\"FileStatus\":[{\"length\":0,\"pathSuffix\":\"Test01\",\"type\":\"DIRECTORY\",\"blockSize\":0,\"accessTime\":1528320290048,\"modificationTime\":1528320362596,\"replication\":0,\"permission\":\"770\",\"owner\":\"owner1\",\"group\":\"ownergroup1\",\"aclBit\":true},{\"length\":0,\"pathSuffix\":\"Test02\",\"type\":\"DIRECTORY\",\"blockSize\":0,\"accessTime\":1531515372559,\"modificationTime\":1531523888360,\"replication\":0,\"permission\":\"770\",\"owner\":\"owner2\",\"group\":\"ownergroup2\",\"aclBit\":true,\"attributes\":[\"Link\"]}]}}";
            server.EnqueMockResponse(new MockResponse(200, "Success", liststatusOutput));

            adlsClient.SetInsecureHttp();
            HashSet<string> hset = new HashSet<string>();
            foreach (var entry in adlsClient.EnumerateDirectory("/ShareTest"))
            {
                hset.Add(entry.FullName);
            }
            Assert.IsTrue(hset.Count == 2);
            Assert.IsTrue(hset.Contains("/ShareTest/Test01"));
            Assert.IsTrue(hset.Contains("/ShareTest/Test02"));
            server.StopServer();
        }

        [TestMethod]
        public void testListStatusWithMultipleArrayInResponse()
        {
            int port = 8088;
            AdlsClient adlsClient = AdlsClient.CreateClientWithoutAccntValidation(MockWebServer.Host + ":" + port, TestToken);
            MockWebServer server = new MockWebServer(port);
            server.StartServer();
            string liststatusOutput = "{\"FileStatuses\":{\"FileStatus\":[{\"length\":0,\"pathSuffix\":\"Test01\",\"type\":\"DIRECTORY\",\"blockSize\":0,\"accessTime\":1528320290048,\"modificationTime\":1528320362596,\"replication\":0,\"permission\":\"770\",\"owner\":\"owner1\",\"group\":\"ownergroup1\",\"aclBit\":true},{\"length\":0,\"pathSuffix\":\"Test02\",\"type\":\"DIRECTORY\",\"blockSize\":0,\"accessTime\":1531515372559,\"modificationTime\":1531523888360,\"replication\":0,\"permission\":\"770\",\"owner\":\"owner2\",\"group\":\"ownergroup2\",\"aclBit\":true,\"attributes\":[\"Link\"]}]}}";
            server.EnqueMockResponse(new MockResponse(200, "Success", liststatusOutput));

            adlsClient.SetInsecureHttp();
            HashSet<string> hset = new HashSet<string>();
            foreach (var entry in adlsClient.EnumerateDirectory("/ShareTest"))
            {
                hset.Add(entry.FullName);
            }
            Assert.IsTrue(hset.Count == 2);
            Assert.IsTrue(hset.Contains("/ShareTest/Test01"));
            Assert.IsTrue(hset.Contains("/ShareTest/Test02"));
            server.StopServer();
        }


        private class ConcatTestRetryPolicy : RetryPolicy
        {
            private int _numberOfRetries;
            private readonly int _maxRetries;

            public ConcatTestRetryPolicy()
            {
                _numberOfRetries = 0;
                _maxRetries = 2;
            }

            public override bool ShouldRetry(int httpCode, Exception ex)
            {
                if (_numberOfRetries >= _maxRetries)
                    return false;

                if (httpCode == 400)
                    return false;

                if (httpCode == 404) 
                {
                    _numberOfRetries++;
                    return true;
                }
                throw new Exception("This class is not meant for use other than testing!");
            }

            public int NumberOfRetries()
            {
                return _numberOfRetries;
            }

        }

        /// <summary>
        /// Unit test to ConcatContentTypePopulatedInRetries
        /// </summary>
        [TestMethod]
        public void ConcatContentTypePopulatedInRetries()
        {
            var retrypolicy = new ConcatTestRetryPolicy();
            RequestOptions req = new RequestOptions(retrypolicy);
            OperationResponse resp = new OperationResponse();

            try
            {
                Core.ConcatAsync("WillNotExist.txt", new List<string> { "DoesntExist1.txt", "DoesntExist2.txt" }, _adlsClient, req, resp).GetAwaiter().GetResult();
            }
            catch (AdlsException)
            {
                // do nothing for adls exception as the above call is mean to fail. 
                // We only care about how many times we actually retried
            }
            Assert.AreEqual(retrypolicy.NumberOfRetries(), 2);
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
