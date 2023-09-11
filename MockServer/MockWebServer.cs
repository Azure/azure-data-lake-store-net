using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading;
using System.IO;

namespace MockServer
{
    /// <summary>
    /// Mock web server that can return custom Http responses to a Http request. Mainly used for testing purposes.
    /// </summary>
    public class MockWebServer
    {
        /// <summary>
        /// Locah host name
        /// </summary>
        public const string Host = "localhost";
        /// <summary>
        /// Thread worker where the server runs
        /// </summary>
        private readonly Thread _threadWorker;
        /// <summary>
        /// Internal list which enques custom Http responses user wants to return. For example: If user wants to return a response of 408 user enqueues a MockResponse 
        /// for 408
        /// </summary>
        private readonly Queue<MockResponse> _list;
        /// <summary>
        /// Http listener that immplements a mock server on a localhost and a particular port
        /// </summary>
        private readonly HttpListener _webListener;
        /// <summary>
        /// Initializes the Httplistener to listen to a particular port of localhost
        /// </summary>
        /// <param name="port"></param>
        public MockWebServer(int port)
        {
            _threadWorker = new Thread(Run);
            _list = new Queue<MockResponse>();
            _webListener = new HttpListener();
            _webListener.Prefixes.Add("http://" + Host + ":" + port + "/");
        }
        /// <summary>
        /// Starts the server thread
        /// </summary>
        public void StartServer()
        {
            _threadWorker.Start();
        }
        /// <summary>
        /// Stops the MockWebServer thread
        /// </summary>
        public void StopServer()
        {
            EnqueMockResponse(null);
            _threadWorker.Join();
        }
        /// <summary>
        /// Adds a custom MockResponse to the back of the queue.
        /// </summary>
        /// <param name="item">MockResponse</param>
        public void EnqueMockResponse(MockResponse item)
        {
            lock (_list)
            {
                _list.Enqueue(item);
                Monitor.Pulse(_list);
            }
        }
        /// <summary>
        /// Retrieves the Mockresponse from the queue
        /// </summary>
        /// <returns></returns>
        private MockResponse GetMockResponse()
        {
            lock (_list)
            {

                while (_list.Count == 0)
                {
                    Monitor.Wait(_list);
                }
                return _list.Dequeue();
            }
        }
        /// <summary>
        /// Runs the server. Polls for a mockresponse from the queue, waits for the request to come in and then sets the response of the request.
        /// Keeps doing this till the server is topped.
        /// </summary>
        public void Run()
        {
            _webListener.Start();
            while (true)
            {
                MockResponse resp = GetMockResponse();
                if (resp == null)
                {
                     _webListener.Close();
                    return;
                }

                try
                { 
                
                var context = _webListener.GetContext();
                HttpListenerRequest request = context.Request;
                var response = context.Response;

                Console.WriteLine(request.Headers.AllKeys);
                response.StatusCode = (int)resp.StatusCode.Value;
                response.StatusDescription = resp.StatusDescription;
                if (resp.ResponseBody != null)
                {
                    Wait(context.Request.InputStream);
                    var bytes = Encoding.UTF8.GetBytes(resp.ResponseBody);
                    response.ContentType = "application/json";
                    response.ContentLength64 = bytes.Length;
                    response.OutputStream.WriteAsync(bytes, 0, bytes.Length).Wait();
                }
                response.Close();
                }catch (Exception ex) {
                    Console.WriteLine(ex);
                    return;
                }
            }
        }
        /// <summary>
        /// Waits for a specified amount of time. Used to test cancellation token. 
        /// </summary>
        /// <param name="inputStream"></param>
        private void Wait(Stream inputStream)
        {
            byte[] buff = new byte[250];
            int noOfBytes = inputStream.Read(buff, 0, buff.Length);
            string val = Encoding.UTF8.GetString(buff, 0, noOfBytes);
            string[] arr = val.Split(':');
            if (arr[0].Equals("wait"))
            {
                int waitTime = Int32.Parse(arr[1]);
                Thread.Sleep(waitTime * 1000);
            }
        }

        public void StopAbruptly()
        {
            _webListener.Close();
        }
    }
    /// <summary>
    /// Custom Http response that the mock server uses to set the response of a request
    /// </summary>
    public class MockResponse
    {
        /// <summary>
        /// Http Status code
        /// </summary>
        public HttpStatusCode? StatusCode { get; set; }
        /// <summary>
        /// Http status message
        /// </summary>
        public string StatusDescription { get; set; }
        public string ResponseBody { get; set; }

        public MockResponse(int status, string description, string body=null)
        {
            StatusCode = (HttpStatusCode)status;
            StatusDescription = description;
            ResponseBody = body;
        }
    }




}
