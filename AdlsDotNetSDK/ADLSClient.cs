using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.DataLake.Store.Acl;
using Microsoft.Azure.DataLake.Store.AclTools;
using Microsoft.Azure.DataLake.Store.FileProperties;
using Microsoft.Azure.DataLake.Store.FileTransfer;
using Microsoft.Azure.DataLake.Store.RetryPolicies;
using Azure.Core;
using NLog;
using System.IO;

[assembly: InternalsVisibleTo("Microsoft.Azure.DataLake.InternalStoreSDK, PublicKey=" +
                              "0024000004800000940000000602000000240000525341310004000001000100b5fc90e7027f67" +
                              "871e773a8fde8938c81dd402ba65b9201d60593e96c492651e889cc13f1415ebb53fac1131ae0b" +
                              "d333c5ee6021672d9718ea31a8aebd0da0072f25d87dba6fc90ffd598ed4da35e44c398c454307" +
                              "e8e33b8426143daec9f596836f97c8f74750e5975c64e2189f45def46b2a2b1247adc3652bf5c3" +
                              "08055da9")]
[assembly: InternalsVisibleTo("Microsoft.Azure.DataLake.Store.UnitTest, PublicKey=" +
                              "0024000004800000940000000602000000240000525341310004000001000100b5fc90e7027f67" +
                              "871e773a8fde8938c81dd402ba65b9201d60593e96c492651e889cc13f1415ebb53fac1131ae0b" +
                              "d333c5ee6021672d9718ea31a8aebd0da0072f25d87dba6fc90ffd598ed4da35e44c398c454307" +
                              "e8e33b8426143daec9f596836f97c8f74750e5975c64e2189f45def46b2a2b1247adc3652bf5c3" +
                              "08055da9")]
[assembly: InternalsVisibleTo("TestDataCreator,PublicKey=" +
                              "0024000004800000940000000602000000240000525341310004000001000100b5fc90e7027f67" +
                              "871e773a8fde8938c81dd402ba65b9201d60593e96c492651e889cc13f1415ebb53fac1131ae0b" +
                              "d333c5ee6021672d9718ea31a8aebd0da0072f25d87dba6fc90ffd598ed4da35e44c398c454307" +
                              "e8e33b8426143daec9f596836f97c8f74750e5975c64e2189f45def46b2a2b1247adc3652bf5c3" +
                              "08055da9")]

namespace Microsoft.Azure.DataLake.Store
{
    /// <summary>
    /// Client of Azure data lake store. It contains the public APIs to perform operations of REST API which are easier to call and more usable than Core APIs. Core APIs provide more freedom but ADLSClient provide more commonly used forms.
    /// It encapsulates the Authorization token and token refresh. Contains factory methods that takes a ServiceClientCredential or a string auth token and returns instance of this class. For every operation it provides
    /// a async and sync version. Every sync method is a wait on async method with exception of Create and Concurrent append. 
    /// All APIs are thread safe with some exceptions in CreateFile and GetAppendStream. CreateFile and GetAppendStream cannot be called for the same path from different threads because writing is done with a lease so there will lease conflicts
    /// If an application wants to perform multi-threaded operations using this SDK it is highly recomended to set ServicePointManager.DefaultConnectionLimit to the number of threads application wants the sdk to use before creating any instance of AdlsClient.
    /// By default ServicePointManager.DefaultConnectionLimit is set to 2. Adls use NLog for logging. adls.dotnet.* is the name of the logger to obtain all logs.
    /// </summary>
    public class AdlsClient
    {
        #region Properties

        internal static int ConcatenateStreamListThreshold = 100;
        /// <summary>
        /// Logger to log information (debug/error/trace) regarding client
        /// </summary>
        protected static readonly Logger ClientLogger = LogManager.GetLogger("adls.dotnet");
        /// <summary>
        /// Object synchronise setters/getters for clienttoken and proto
        /// </summary>
        private readonly Object _thisLock = new Object();
        /// <summary>
        /// User agent-SDKVersion/OSName, version, architecture and Dot netversion
        /// </summary>
        private static readonly string UserAgent;
        /// <summary>
        /// UserAgent/UserAgentSuffix
        /// </summary>
        private string _userAgentString = UserAgent;
        /// <summary>
        /// whether request is HTTP or HTTPS. It is mainly HTTPS. It is set as HTTP only for testing purposes
        /// </summary>
        private string _proto = "https";
        /// <summary>
        /// Azure data lake store account name including full domain name
        /// </summary>
        public string AccountFQDN { get; }
        /// <summary>
        /// Field that just tracks the number of Clients running in an application
        /// </summary>
        private static long _atomicClientId;
        /// <summary>
        /// Client object ID
        /// </summary>
        public long ClientId { get; }
        /// <summary>
        /// Authorization token
        /// </summary>
        private string AccessToken { get; set; }
        /// <summary>
        /// Authorization token provider
        /// </summary>
        private TokenCredential AccessProvider { get; }
        /// <summary>
        /// The scopes to request when acquiring a token using AccessProvider
        /// </summary>
        public readonly string[] _tokenScopes = new string[] { "https://datalake.azure.net//.default" };
        /// <summary>
        /// SDK version- AssemblyFileVersion
        /// </summary>
        private static readonly string SdkVersion;

        /// <summary>
        /// Default number of threads used by tools like uploader/downloader, recursive acl change and other multi-threaded tools using the SDK.
        /// Can be used to set ServicePointManager.DefaultConnectionLimit if you want the SDK to decide number of threads it uses for multi-threaded tools.
        /// </summary>
        public static int DefaultNumThreads { get; internal set; }

        internal const int DefaultThreadsCalculationFactor = 8;

        /// <summary>
        /// DIP IP
        /// </summary>
        internal string DipIp { get; set; }


        /// <summary>
        /// Delegate that accepts the writestream and wraps it with the compression stream, Only set by client if we 
        /// </summary>
        internal Func<Stream, Stream> WrapperStream { get; set; } = null;

        /// <summary>
        /// ContentEncoding for the compression, if not set then we use default contentencoding
        /// </summary>
        internal string ContentEncoding { get; set; } = null;

        private TimeSpan _perRequestTimeout = new TimeSpan(0, 0, 60);

        private TimeSpan _perRequestTimeoutForEnumerateTrash = new TimeSpan(0, 0, 135);

        private volatile bool _useConditionalCreateWithOverwrite = false;

        #endregion

        #region Constructors

        /// <summary>
        /// Static constructor that initializes the static fields
        /// </summary>
        static AdlsClient()
        {
            string adlsSdkName = "AdlsDotNetSDK";
            try
            {
                Assembly assembly = typeof(AdlsClient).GetTypeInfo().Assembly;
                adlsSdkName = assembly.GetName().Name;
                AssemblyFileVersionAttribute fvAttribute = assembly.GetCustomAttribute(typeof(AssemblyFileVersionAttribute)) as AssemblyFileVersionAttribute;
                string sdkVersion = fvAttribute?.Version;
                SdkVersion = string.IsNullOrEmpty(sdkVersion) ? "SDKVersionNotKnown" : sdkVersion;
            }
            catch (Exception)
            {
                SdkVersion = "SDKVersionUnknown";
            }
            string osInfo;
            string dotNetVersion = "NETSTANDARD2_0-"+System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription;
            try
            {
                // The below calculation is wrong before net6 since Environment.ProcessorCount gives the count of logical processors not cores.
                // Not sure if we have a better way here.
                DefaultNumThreads = Math.Max(DefaultThreadsCalculationFactor, DefaultThreadsCalculationFactor * Environment.ProcessorCount);
                osInfo = System.Runtime.InteropServices.RuntimeInformation.OSDescription + " " +
                         System.Runtime.InteropServices.RuntimeInformation.OSArchitecture;
            }
            catch (Exception)
            {
                osInfo = "OSNotKnown";
            }
            UserAgent = $"{adlsSdkName};{SdkVersion}/{osInfo}/{dotNetVersion}";
        }

        /// <summary>
        /// Protected constructor for moq tests
        /// </summary>
        protected AdlsClient()
        {

        }

        internal AdlsClient(string accnt, long clientId, bool skipAccntValidation = false)
        {
            AccountFQDN = accnt.Trim();
            if (!skipAccntValidation && !IsValidAccount(AccountFQDN))
            {
                throw new ArgumentException($"Account name {AccountFQDN} is invalid. Specify the full account including the domain name.");
            }
            ClientId = clientId;
            if (ClientLogger.IsTraceEnabled)
            {
                ClientLogger.Trace($"AdlsStoreClient, {ClientId} created for account {AccountFQDN} for SDK version {SdkVersion}");
            }
        }

        internal AdlsClient(string accnt, long clientId, string token, bool skipAccntValidation = false) : this(accnt, clientId, skipAccntValidation)
        {
            AccessToken = token;
        }

        internal AdlsClient(string accnt, long clientId, TokenCredential creds, bool skipAccntValidation = false) : this(accnt, clientId, skipAccntValidation)
        {
            AccessProvider = creds;
        }

        internal AdlsClient(string accnt, long clientId, TokenCredential creds, string[] scopes, bool skipAccntValidation = false) : this(accnt, clientId, creds, skipAccntValidation)
        {
            _tokenScopes = scopes;
        }

        private bool IsValidAccount(string accnt)
        {
            return Regex.IsMatch(accnt, @"^[a-zA-Z0-9\-]+\.[a-zA-Z0-9\-][a-zA-Z0-9.\-]*$");
        }
        #endregion

        #region FactoryMethods
        /// <summary>
        /// Internal factory method that returns a AdlsClient without Account validation. For testing purposes
        /// </summary>
        /// <param name="accnt">Azure data lake store account name including full domain name (e.g. contoso.azuredatalake.net)</param>
        /// <param name="token">Token</param>
        /// <returns>AdlsClient</returns>
        internal static AdlsClient CreateClientWithoutAccntValidation(string accnt, string token)
        {
            return new AdlsClient(accnt, Interlocked.Increment(ref _atomicClientId), token, true);
        }

        /// <summary>
        /// Factory method that creates an instance AdlsClient using the token key. If an application wants to perform multi-threaded operations using this SDK
        /// it is highly recomended to set ServicePointManager.DefaultConnectionLimit to the number of threads application wants the sdk to use before creating any instance of AdlsClient.
        /// By default ServicePointManager.DefaultConnectionLimit is set to 2.
        /// </summary>
        /// <param name="accountFqdn">Azure data lake store account name including full domain name (e.g. contoso.azuredatalakestore.net)</param>
        /// <param name="token">Full authorization Token e.g. Bearer abcddsfere.....</param>
        /// <returns>AdlsClient</returns>
        public static AdlsClient CreateClient(string accountFqdn, string token)
        {
            return new AdlsClient(accountFqdn, Interlocked.Increment(ref _atomicClientId), token);
        }

        /// <summary>
        /// Factory method that creates an instance of AdlsClient using ServiceClientCredential. If an application wants to perform multi-threaded operations using this SDK
        /// it is highly recomended to set ServicePointManager.DefaultConnectionLimit to the number of threads application wants the sdk to use before creating any instance of AdlsClient.
        /// By default ServicePointManager.DefaultConnectionLimit is set to 2.
        /// </summary>
        /// <param name="accountFqdn">Azure data lake store account name including full domain name  (e.g. contoso.azuredatalakestore.net)</param>
        /// <param name="creds">Credentials that retrieves the Auth token</param>
        /// <returns>AdlsClient</returns>
        public static AdlsClient CreateClient(string accountFqdn, TokenCredential creds)
        {
            return new AdlsClient(accountFqdn, Interlocked.Increment(ref _atomicClientId), creds);
        }

        /// <summary>
        /// Factory method that creates an instance of AdlsClient using ServiceClientCredential. If an application wants to perform multi-threaded operations using this SDK
        /// it is highly recomended to set ServicePointManager.DefaultConnectionLimit to the number of threads application wants the sdk to use before creating any instance of AdlsClient.
        /// By default ServicePointManager.DefaultConnectionLimit is set to 2.
        /// </summary>
        /// <param name="accountFqdn">Azure data lake store account name including full domain name  (e.g. contoso.azuredatalakestore.net)</param>
        /// <param name="creds">Credentials that retrieves the Auth token</param>
        /// <returns>AdlsClient</returns>
        public static AdlsClient CreateClient(string accountFqdn, TokenCredential creds, string[] scopes)
        {
            return new AdlsClient(accountFqdn, Interlocked.Increment(ref _atomicClientId), creds, scopes);
        }
        #endregion

        #region Thread Safe Getter Setters
        /// <summary>
        /// Update the DipIp
        /// </summary>
        /// <param name="connectionFailure">True if the request failure is because of connetion failure</param>
        /// <param name="cancelToken"></param>
        /// <returns></returns>
        internal virtual Task UpdateDipIfNeededAsync(bool connectionFailure, CancellationToken cancelToken)
        {
            return Task.FromResult(default(object));
        }

        /// <summary>
        /// Sets the request protocol as http. Only set for testing purposes.
        /// </summary>
        internal void SetInsecureHttp()
        {
            lock (_thisLock)
            {
                _proto = "http";
            }
        }

        /// <summary>
        /// Atomically returns the UserAgent
        /// </summary>
        /// <returns></returns>
        internal string GetUserAgent()
        {
            lock (_thisLock)
            {
                return _userAgentString;
            }
        }

        /// <summary>
        /// Adds the user agent suffix
        /// </summary>
        /// <param name="suffix">User Agent suffix</param>
        public void AddUserAgentSuffix(string suffix)
        {
            lock (_thisLock)
            {
                if (!string.IsNullOrWhiteSpace(suffix))
                {
                    _userAgentString += "/" + suffix;
                }
            }
        }

        /// <summary>
        /// Gets the Http prefix
        /// </summary>
        /// <returns></returns>
        internal string GetHttpPrefix()
        {
            lock (_thisLock)
            {
                return _proto;
            }
        }

        /// <summary>
        /// Gets the authorization token
        /// </summary>
        /// <param name="cancelToken">CAcellation token</param>
        /// <returns></returns>
        internal async Task<string> GetTokenAsync(CancellationToken cancelToken = default(CancellationToken))
        {
            if (AccessProvider != null)
            {
                return "Bearer " + (await AccessProvider.GetTokenAsync(new TokenRequestContext(_tokenScopes), cancelToken).ConfigureAwait(false)).Token;
            }
            lock (_thisLock)
            {
                return AccessToken;
            }
        }

        /// <summary>
        /// Sets the auth token.
        /// </summary>
        /// <param name="accessToken">Access token</param>
        public void SetToken(string accessToken)
        {
            lock (_thisLock)
            {
                AccessToken = "Bearer " + accessToken;
            }
        }

        /// <summary>
        /// Sets the per request timeout. Highly recommended to set it after creating client. 
        /// Not recommended to set it while requests are underway from a different thread.
        /// </summary>
        /// <param name="timeout"></param>
        public void SetPerRequestTimeout(TimeSpan timeout)
        {
            _perRequestTimeout = timeout;
        }

        /// <summary>
        /// Sets whether to perform conditional create with overwrite. Helps resolving atomic issues with retries
        /// </summary>
        /// <param name="useConditionalCreateWithOverwrite">Whether to use conditional create</param>
        public void SetConditionalCreateWithOverwrite(bool useConditionalCreateWithOverwrite)
        {
            _useConditionalCreateWithOverwrite = useConditionalCreateWithOverwrite;
        }

        internal TimeSpan GetPerRequestTimeout()
        {
            return _perRequestTimeout;
        }
        #endregion

        #region REST API 
        /// <summary>
        /// Asynchronous API to create a directory.
        /// </summary>
        /// <param name="dirName">Name of directory</param>
        /// <param name="octalPermission">Octal permission</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>true if it creates the directory else false</returns>
        public virtual async Task<bool> CreateDirectoryAsync(string dirName, string octalPermission = null, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(dirName))
            {
                throw new ArgumentException("Path is null");
            }
            if (dirName.Equals("/"))
            {
                throw new ArgumentException("");
            }
            OperationResponse resp = new OperationResponse();
            bool result = await Core.MkdirsAsync(dirName, octalPermission, this,
                new RequestOptions(GetPerRequestTimeout(), new ExponentialRetryPolicy()), resp, cancelToken).ConfigureAwait(false);
            if (!resp.IsSuccessful)
            {
                throw GetExceptionFromResponse(resp, $"Error in creating Directory {dirName}.");
            }
            return result;
        }
        /// <summary>
        /// Synchronous API to create a directory
        /// </summary>
        /// <param name="dirName">Name of directory</param>
        /// <param name="octalPermission">Octal permission</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>true if it creates the directory else false</returns>
        public virtual bool CreateDirectory(string dirName, string octalPermission = null, CancellationToken cancelToken = default(CancellationToken))
        {
            return CreateDirectoryAsync(dirName, octalPermission, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Asynchronous API that returns the stream to read data from file in ADLS
        /// </summary>
        /// <param name="filename">File name</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>Input stream</returns>
        public virtual async Task<AdlsInputStream> GetReadStreamAsync(string filename, CancellationToken cancelToken = default(CancellationToken))
        {
            return await GetReadStreamAsync(filename, AdlsInputStream.DefaultBufferCapacity, cancelToken).ConfigureAwait(false);
        }
        /// <summary>
        /// Synchronous API that returns the stream to read data from file in ADLS
        /// </summary>
        /// <param name="filename">File name</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>Input stream</returns>
        public virtual AdlsInputStream GetReadStream(string filename, CancellationToken cancelToken = default(CancellationToken))
        {
            return GetReadStreamAsync(filename, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Asynchronous API that returns the stream to read data from file in ADLS
        /// </summary>
        /// <param name="filename">File name</param>
        /// <param name="bufferCapacity"> Buffer Capacity</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>Input stream</returns>
        public virtual async Task<AdlsInputStream> GetReadStreamAsync(string filename, int bufferCapacity, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(filename))
            {
                throw new ArgumentException("Path is null");
            }
            if (filename.Equals("/"))
            {
                throw new ArgumentException("");
            }
            OperationResponse resp = new OperationResponse();
            // Pass getConsistentlength so that we get the updated length of stream
            DirectoryEntry diren = await Core
                .GetFileStatusAsync(filename, UserGroupRepresentation.ObjectID, this,
                    new RequestOptions(GetPerRequestTimeout(), new ExponentialRetryPolicy()), resp, cancelToken, true).ConfigureAwait(false);
            if (!resp.IsSuccessful)
            {
                throw GetExceptionFromResponse(resp, $"Error opening a Read Stream for file {filename}.");
            }
            return new AdlsInputStream(filename, this, diren, bufferCapacity);
        }

        /// <summary>
        /// Synchronous API that returns the stream to read data from file in ADLS
        /// </summary>
        /// <param name="filename">File name</param>
        /// <param name="bufferCapacity"> Buffer Capacity</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>Input stream</returns>
        public virtual AdlsInputStream GetReadStream(string filename, int bufferCapacity, CancellationToken cancelToken = default(CancellationToken))
        {
            return GetReadStreamAsync(filename, bufferCapacity, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Asynchronous API that returns the stream to write data to a file in ADLS. The file is opened with exclusive 
        /// access - any attempt to open the same file for append will fail while this stream is open. 
        /// 
        /// Threading: The returned stream is not thread-safe.
        /// </summary>
        /// <param name="filename">File name</param>
        /// <param name="bufferPool">Buffer Pool</param>
        /// <param name="bufferCapacity">Buffer capacity, Number of bytes written to the server</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>Output stream</returns>
        internal virtual async Task<AdlsOutputStream> GetAppendStreamAsync(string filename, AdlsArrayPool<byte> bufferPool, int bufferCapacity, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(filename))
            {
                throw new ArgumentException("Path is null");
            }
            if (filename.Equals("/"))
            {
                throw new ArgumentException("Cannot append to the root");
            }
            string leaseId = Guid.NewGuid().ToString();
            OperationResponse resp = new OperationResponse();
            //This is necessary to do to obtain the lease on the file
            await Core.AppendAsync(filename, leaseId, leaseId, SyncFlag.DATA, 0, null, -1, 0, this,
                new RequestOptions(GetPerRequestTimeout(), new ExponentialRetryPolicy()), resp, cancelToken).ConfigureAwait(false);
            if (!resp.IsSuccessful)
            {
                throw GetExceptionFromResponse(resp, $"Error trying to append to file {filename}.");
            }
            return await AdlsOutputStream.GetAdlsOutputStreamAsync(filename, this, false, leaseId, bufferPool, bufferCapacity).ConfigureAwait(false);
        }

        /// <summary>
        /// Synchronous API that returns the stream to write data to a file in ADLS. The file is opened with exclusive 
        /// access - any attempt to open the same file for append will fail while this stream is open.  
        /// 
        /// Threading: The returned stream is not thread-safe.
        /// </summary>
        /// <param name="filename">File name</param>
        /// <param name="bufferPool">Buffer Pool</param>
        /// <param name="bufferCapacity">Buffer capacity, Number of bytes written to the server</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>Output stream</returns>
        internal virtual AdlsOutputStream GetAppendStream(string filename, AdlsArrayPool<byte> bufferPool, int bufferCapacity, CancellationToken cancelToken = default(CancellationToken))
        {
            return GetAppendStreamAsync(filename, bufferPool, bufferCapacity, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Asynchronous API that returns the stream to write data to a file in ADLS. The file is opened with exclusive 
        /// access - any attempt to open the same file for append will fail while this stream is open. 
        /// 
        /// Threading: The returned stream is not thread-safe.
        /// </summary>
        /// <param name="filename">File name</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>Output stream</returns>
        public virtual async Task<AdlsOutputStream> GetAppendStreamAsync(string filename, CancellationToken cancelToken = default(CancellationToken))
        {
            return await GetAppendStreamAsync(filename, null, AdlsOutputStream.BufferMaxCapacity, cancelToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Synchronous API that returns the stream to write data to a file in ADLS. The file is opened with exclusive 
        /// access - any attempt to open the same file for append will fail while this stream is open.  
        /// 
        /// Threading: The returned stream is not thread-safe.
        /// </summary>
        /// <param name="filename">File name</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>Output stream</returns>
        public virtual AdlsOutputStream GetAppendStream(string filename, CancellationToken cancelToken = default(CancellationToken))
        {
            return GetAppendStreamAsync(filename, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Asynchronous API that creates a file and returns the stream to write data to that file in ADLS. The file is opened with exclusive 
        /// access - any attempt to open the same file for append will fail while this stream is open. 
        /// 
        /// Threading: The returned stream is not thread-safe.
        /// </summary>
        /// <param name="filename">File name</param>
        /// <param name="mode">Overwrites the existing file if the mode is Overwrite</param>
        /// <param name="bufferPool">Passed buffer pool</param>
        /// <param name="bufferCapacity">Buffer capacity, Min can be 1MB, Max can be 4MB</param>
        /// <param name="octalPermission">Octal permission string, can be null</param>
        /// <param name="createParent">If true creates any non-existing parent directories</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>Output stream</returns>
        internal virtual async Task<AdlsOutputStream> CreateFileAsync(string filename, IfExists mode, AdlsArrayPool<byte> bufferPool, int bufferCapacity, string octalPermission = null, bool createParent = true, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(filename))
            {
                throw new ArgumentException("Path is null");
            }
            if (filename.Equals("/"))
            {
                throw new ArgumentException("Cant create the root");
            }
            if (ClientLogger.IsTraceEnabled)
            {
                ClientLogger.Trace($"AdlsStoreClient, Create File {filename} for client {ClientId}");
            }
            string leaseId = Guid.NewGuid().ToString();
            bool overwrite = mode == IfExists.Overwrite;
            if (overwrite && _useConditionalCreateWithOverwrite)
            {
                await CreateFileAtomicallyWithOverWrite(filename, octalPermission, createParent, leaseId, cancelToken).ConfigureAwait(false);
            }
            else
            {
                //If we are overwriting any existing file by that name then it doesn't matter to try it again even though the last request is in a inconsistent state
                RetryPolicy policy = overwrite ? new ExponentialRetryPolicy() : (RetryPolicy)new NonIdempotentRetryPolicy();

                OperationResponse resp = new OperationResponse();
                await Core.CreateAsync(filename, overwrite, octalPermission, leaseId, leaseId, createParent, SyncFlag.DATA, null, -1, 0, this, new RequestOptions(GetPerRequestTimeout(), policy), resp, cancelToken).ConfigureAwait(false);
                if (!resp.IsSuccessful)
                {
                    throw GetExceptionFromResponse(resp, $"Error in creating file {filename}.");
                }
            }
            return await AdlsOutputStream.GetAdlsOutputStreamAsync(filename, this, true, leaseId, bufferPool, bufferCapacity).ConfigureAwait(false);
        }

        private async Task CreateFileAtomicallyWithOverWrite(string path, string octalPermission, bool createParent, string leaseId, CancellationToken cancelToken = default(CancellationToken))
        {
            bool checkExists = true;
            OperationResponse resp = new OperationResponse();

            var entry = await Core.GetFileStatusAsync(path, UserGroupRepresentation.ObjectID, this, new RequestOptions(GetPerRequestTimeout(), new ExponentialRetryPolicy()), resp).ConfigureAwait(false);
            if (!resp.IsSuccessful)
            {
                if (resp.HttpStatus == HttpStatusCode.NotFound && resp.RemoteExceptionName.Contains("FileNotFoundException"))
                {
                    checkExists = false;
                }
                else
                {
                    throw GetExceptionFromResponse(resp, "Error getting info for file " + path);
                }
            }

            if (checkExists && entry.Type == DirectoryEntryType.DIRECTORY)
            {
                throw new AdlsException("Cannot overwrite directory "+path);
            }

            if(checkExists)
            {

                resp = new OperationResponse();
                await Core.CheckAccessSync(path, "-w-", this, new RequestOptions(GetPerRequestTimeout(), new ExponentialRetryPolicy()), resp).ConfigureAwait(false);
                if (!resp.IsSuccessful)
                {
                    if (resp.HttpStatus == HttpStatusCode.NotFound && resp.RemoteExceptionName.Contains("FileNotFoundException"))
                    {
                        checkExists = false;
                    }
                    else
                    {
                        throw GetExceptionFromResponse(resp, "Error checking access for " + path);
                    }
                }
                if (checkExists)
                {
                    resp = new OperationResponse();
                    await Core.DeleteAsync(path, false, entry.FileContextID, this, new RequestOptions(GetPerRequestTimeout(), new ExponentialRetryPolicy()), resp).ConfigureAwait(false); // conditional delete
                    if (!resp.IsSuccessful)
                    {
                        throw GetExceptionFromResponse(resp, "Error deleting the file for create+overwrite " + path);
                    }
                }
            }

            resp = new OperationResponse();
            await Core.CreateAsync(path, false, octalPermission, leaseId, leaseId, createParent, SyncFlag.DATA, null, -1, 0, this, new RequestOptions(GetPerRequestTimeout(), new ExponentialRetryPolicy()), resp, cancelToken).ConfigureAwait(false);
            if (!resp.IsSuccessful)
            {
                if (resp.HttpStatus == HttpStatusCode.Forbidden && resp.RemoteExceptionName.Contains("FileAlreadyExistsException"))
                {
                    return;
                }
                throw GetExceptionFromResponse(resp, $"Error in creating file {path}.");
            }
        }
/// <summary>
/// Synchronous API that creates a file and returns the stream to write data to that file in ADLS. The file is opened with exclusive 
/// access - any attempt to open the same file for append will fail while this stream is open.  
/// 
/// Threading: The returned stream is not thread-safe.
/// </summary>
/// <param name="filename">File name</param>
/// <param name="mode">Overwrites the existing file if the mode is Overwrite</param>
/// <param name="bufferPool">Passed buffer pool</param>
/// <param name="bufferCapacity"></param>
/// <param name="octalPermission">Octal permission string</param>
/// <param name="createParent">If true creates any non-existing parent directories</param>
/// <returns>Output stream</returns>
internal virtual AdlsOutputStream CreateFile(string filename, IfExists mode, AdlsArrayPool<byte> bufferPool, int bufferCapacity, string octalPermission = null, bool createParent = true)
        {
            return CreateFileAsync(filename, mode, bufferPool, bufferCapacity, octalPermission, createParent).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Asynchronous API that creates a file and returns the stream to write data to that file in ADLS. The file is opened with exclusive 
        /// access - any attempt to open the same file for append will fail while this stream is open. 
        /// 
        /// Threading: The returned stream is not thread-safe.
        /// </summary>
        /// <param name="filename">File name</param>
        /// <param name="mode">Overwrites the existing file if the mode is Overwrite</param>
        /// <param name="octalPermission">Octal permission string, can be null</param>
        /// <param name="createParent">If true creates any non-existing parent directories</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>Output stream</returns>
        public virtual async Task<AdlsOutputStream> CreateFileAsync(string filename, IfExists mode, string octalPermission = null, bool createParent = true, CancellationToken cancelToken = default(CancellationToken))
        {
            return await CreateFileAsync(filename, mode, null, AdlsOutputStream.BufferMaxCapacity, octalPermission, createParent, cancelToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Synchronous API that creates a file and returns the stream to write data to that file in ADLS. The file is opened with exclusive 
        /// access - any attempt to open the same file for append will fail while this stream is open.  
        /// 
        /// Threading: The returned stream is not thread-safe.
        /// </summary>
        /// <param name="filename">File name</param>
        /// <param name="mode">Overwrites the existing file if the mode is Overwrite</param>
        /// <param name="octalPermission">Octal permission string</param>
        /// <param name="createParent">If true creates any non-existing parent directories</param>
        /// <returns>Output stream</returns>
        public virtual AdlsOutputStream CreateFile(string filename, IfExists mode, string octalPermission = null, bool createParent = true)
        {
            return CreateFileAsync(filename, mode, octalPermission, createParent).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Asynchronous api to delete a file or directory. For directory it will only delete if it is empty.
        /// </summary>
        /// <param name="path">Path of file or directory</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>True if the path is deleted successfully else false</returns>
        public virtual async Task<bool> DeleteAsync(string path, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            if (path.Equals("/"))
            {
                throw new ArgumentException("Cant delete the root");
            }
            OperationResponse resp = new OperationResponse();
            bool isSucceded = await Core.DeleteAsync(path, false, this, new RequestOptions(GetPerRequestTimeout(), new ExponentialRetryPolicy()),
                resp, cancelToken).ConfigureAwait(false);
            if (!resp.IsSuccessful)
            {
                throw GetExceptionFromResponse(resp, $"Error in deleting entry {path}.");
            }
            return isSucceded;
        }

        /// <summary>
        /// Synchronous api to delete a file or directory. For directory it will only delete if it is empty.
        /// </summary>
        /// <param name="path">Path of file or directory</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>True if the path is deleted successfully else false</returns>
        public virtual bool Delete(string path, CancellationToken cancelToken = default(CancellationToken))
        {
            return DeleteAsync(path, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Asynchronous api to delete a file or directory recursively
        /// </summary>
        /// <param name="path">Path of file or directory</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>True if the path is deleted successfully else false</returns>
        public virtual async Task<bool> DeleteRecursiveAsync(string path, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            if (path.Equals("/"))
            {
                throw new ArgumentException("Cant delete the root");
            }
            OperationResponse resp = new OperationResponse();
            bool isSucceded = await Core.DeleteAsync(path, true, this, new RequestOptions(GetPerRequestTimeout(), new ExponentialRetryPolicy()),
                resp, cancelToken).ConfigureAwait(false);
            if (!resp.IsSuccessful)
            {
                throw GetExceptionFromResponse(resp, $"Error in deleting recursively for path {path}.");
            }
            return isSucceded;
        }

        /// <summary>
        /// Synchronous api to delete a file or directory recursively. If it is a non-empty directory then it deletes the sub-directories or files.
        /// </summary>
        /// <param name="path">Path of file or directory</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>True if the path is deleted successfully else false</returns>
        public virtual bool DeleteRecursive(string path, CancellationToken cancelToken = default(CancellationToken))
        {
            return DeleteRecursiveAsync(path, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Asynchronous API to rename a file or directory.
        /// For renaming directory: If the destination exists then it puts the source directory one level under the destination.
        /// </summary>
        /// <param name="path">Path of the source file or directory</param>
        /// <param name="destination">Destination path. For directory: If the destination exists then it puts the source directory one level under the destination. If tthere is a subdirectory with same name as source one level under the destination path, rename fails</param>
        /// <param name="overwrite">For file: If true then overwrites the destination file if it exists. Rename of folders cannot result in an overwrite of the target.</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>True if the path is renamed successfully else false</returns>
        public virtual async Task<bool> RenameAsync(string path, string destination, bool overwrite = false, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            if (path.Equals("/"))
            {
                throw new ArgumentException("Cannot rename the root");
            }
            if (string.IsNullOrEmpty(destination))
            {
                throw new ArgumentException("Destination is null");
            }
            if (path.Equals(destination))
            {
                DirectoryEntry diren = await GetDirectoryEntryAsync(path, UserGroupRepresentation.ObjectID, cancelToken).ConfigureAwait(false);
                if (diren.Type != DirectoryEntryType.FILE)
                {
                    throw new ArgumentException("Cannot rename directories same name");
                }
            }
            OperationResponse resp = new OperationResponse();
            bool isSucceeded = await Core.RenameAsync(path, destination, overwrite, this, new RequestOptions(GetPerRequestTimeout(), new ExponentialRetryPolicy()),
                resp, cancelToken).ConfigureAwait(false);
            if (!resp.IsSuccessful)
            {
                throw GetExceptionFromResponse(resp, $"Error in renaming path {path} to {destination}.");
            }
            return isSucceeded;
        }

        /// <summary>
        /// Synchronous API to rename a file or directory.
        /// For renaming directory: If the destination exists then it puts the source directory one level under the destination.
        /// </summary>
        /// <param name="path">Path of the source file or directory</param>
        /// <param name="destination">Destination path</param>
        /// <param name="overwrite">For file: If true then overwrites the destination file if it exists 
        ///                         For directory: If the destination directory exists, then this flag has no use. Because it puts the source one level under destination.
        ///                                        If there is a subdirectory with same name as source one level under the destination path, this flag has no use, rename fails  </param>
        ///                         By default it is false
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>True if the path is renamed successfully else false</returns>
        public virtual bool Rename(string path, string destination, bool overwrite = false, CancellationToken cancelToken = default(CancellationToken))
        {
            return RenameAsync(path, destination, overwrite, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Asynchronously gets meta data like full path, type (file or directory), group, user, permission, length,last Access Time,last Modified Time, expiry time, acl Bit, replication Factor
        /// </summary>
        /// <param name="path">Path of file or directory</param>
        /// <param name="uid">Way the user or group object will be represented</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>Returns the metadata of the file or directory</returns>
        public virtual async Task<DirectoryEntry> GetDirectoryEntryAsync(string path, UserGroupRepresentation uid = UserGroupRepresentation.ObjectID, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            OperationResponse resp = new OperationResponse();
            DirectoryEntry diren = await Core.GetFileStatusAsync(path, uid, this,
                new RequestOptions(GetPerRequestTimeout(), new ExponentialRetryPolicy()), resp, cancelToken).ConfigureAwait(false);
            if (diren == null)
            {
                throw GetExceptionFromResponse(resp, $"Error in getting metadata for path {path}.");
            }
            return diren;
        }

        /// <summary>
        /// Synchronously gets meta data like full path, type (file or directory), group, user, permission, length,last Access Time,last Modified Time, expiry time, acl Bit, replication Factor
        /// </summary>
        /// <param name="path">Path of file or directory</param>
        /// <param name="userIdFormat">Way the user or group object will be represented</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>Returns the metadata of the file or directory</returns>
        public virtual DirectoryEntry GetDirectoryEntry(string path, UserGroupRepresentation userIdFormat = UserGroupRepresentation.ObjectID, CancellationToken cancelToken = default(CancellationToken))
        {
            return GetDirectoryEntryAsync(path, userIdFormat, cancelToken).GetAwaiter().GetResult();
        }
        /// <summary>
        /// Asynchronous API to concatenate source files to a destination file
        /// </summary>
        /// <param name="destination">Path of the destination</param>
        /// <param name="concatFiles">List containing paths of the source files</param>
        /// <param name="deleteSource">If true then deletes the source directory if all the files under it are concatenated</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public virtual async Task ConcatenateFilesAsync(string destination, List<string> concatFiles, bool deleteSource = false, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(destination))
            {
                throw new ArgumentException("Path is null");
            }
            OperationResponse resp = new OperationResponse();
            await Core.ConcatAsync(destination, concatFiles, deleteSource, this,
                new RequestOptions(GetPerRequestTimeout(), new NoRetryPolicy()), resp, cancelToken).ConfigureAwait(false);
            if (!resp.IsSuccessful)
            {
                throw GetExceptionFromResponse(resp, $"Error in concating files {String.Join(",", concatFiles)} to destination {destination}");
            }
        }

        /// <summary>
        /// Internal API to do parallel concatenate
        /// </summary>
        /// <param name="destination">Destintaion</param>
        /// <param name="concatFiles">Concat Files</param>
        /// <param name="deleteSource">Whether to delete the source directory</param>
        /// <param name="cancelToken">Cancellation Token</param>
        internal virtual async Task ConcatenateFilesParallelAsync(string destination, List<string> concatFiles, bool deleteSource = false,
            CancellationToken cancelToken = default(CancellationToken))
        {
            string tempDir = destination.Remove(destination.LastIndexOf('/') + 1) + Guid.NewGuid();
            if (ClientLogger.IsDebugEnabled)
            {
                ClientLogger.Debug($"Temporary Concat Directory: {tempDir}");
            }
            await ConcatenateFilesParallelAsync(destination, concatFiles, 0, tempDir, deleteSource, cancelToken);
            await DeleteRecursiveAsync(tempDir, cancelToken);
        }

        private async Task ConcatenateFilesParallelAsync(string destination, List<string> concatFiles, int recurse, string tempDestination, bool deleteSource = false, CancellationToken cancelToken = default(CancellationToken))
        {
            if (concatFiles.Count < ConcatenateStreamListThreshold)
            {
                await ConcatenateFilesAsync(destination, concatFiles, recurse != 0 || deleteSource, cancelToken);
                return;
            }

            int numberTasks = (int)Math.Ceiling((float)concatFiles.Count / ConcatenateStreamListThreshold);
            var taskList = new Task[numberTasks];
            var destinationList = new List<string>(numberTasks);
            // Parallel concat destination files will be as 0,1,2... under tempDetination\GUID-{recurselevel}. And files under tempDestination\Guid will be the input for 
            // next recursive concat. Adding recurse level tot he temp directory is helpful for debugging purposes
            string tempDir = tempDestination + "/" + Guid.NewGuid() + $"-{recurse}";
            for (int i = 0; i < numberTasks; i++)
            {
                destinationList.Add(tempDir + "/" + i);
                int start = i * ConcatenateStreamListThreshold;
                int count = i < (numberTasks - 1)
                    ? ConcatenateStreamListThreshold
                    : concatFiles.Count - (numberTasks - 1) * ConcatenateStreamListThreshold;
                if (ClientLogger.IsDebugEnabled)
                {
                    ClientLogger.Debug($"Recurse: {recurse}; TaskId: {i}; SourceFiles: {String.Join(",", concatFiles.GetRange(start, count))}; Destination: {destinationList[i]}");
                }
                // Pass false for recurse!=0 also because softdelete will cleanup the folder anyways
                taskList[i] = ConcatenateFilesAsync(destinationList[i], concatFiles.GetRange(start, count),
                    false, cancelToken);
            }

            for (int i = 0; i < numberTasks; i++)
            {
                taskList[i].Wait(cancelToken);
            }

            // Concatenate is always called with deleteSource as false, because parallel concat jobs cannot delete the source folder
            // Now for other recurse levels if you concatenate all the files of a directory softdelete on the SSS will delete the folder since all the files in the tempguid foldler
            if (recurse == 0 && deleteSource)
            {
                string sourcePath = concatFiles[0].Remove(concatFiles[0].LastIndexOf('/'));
                if (string.IsNullOrEmpty(sourcePath))
                {
                    throw new ArgumentException("The root directory cant be deleted");
                }

                await DeleteRecursiveAsync(sourcePath, cancelToken);
            }

            await ConcatenateFilesParallelAsync(destination, destinationList, recurse + 1, tempDestination, deleteSource, cancelToken);
        }

        /// <summary>
        /// Synchronous API to concatenate source files to a destination file
        /// </summary>
        /// <param name="destination">Path of the destination</param>
        /// <param name="concatFiles">List containing paths of the source files</param>
        /// <param name="deleteSource">If true then deletes the source directory if all the files under it are concatenated</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public virtual void ConcatenateFiles(string destination, List<string> concatFiles, bool deleteSource = false,
            CancellationToken cancelToken = default(CancellationToken))
        {
            ConcatenateFilesAsync(destination, concatFiles, deleteSource, cancelToken).GetAwaiter().GetResult();
        }
        /// <summary>
        /// Returns a enumerable that enumerates the sub-directories or files contained in a directory.
        /// By default listAfter and listBefore is empty and we enumerate all the directory entries.
        /// </summary>
        /// <param name="path">Path of the directory</param>
        /// <param name="userIdFormat">Way the user or group object will be represented</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>Enumerable that enumerates over the contents</returns>
        public virtual IEnumerable<DirectoryEntry> EnumerateDirectory(string path, UserGroupRepresentation userIdFormat = UserGroupRepresentation.ObjectID, CancellationToken cancelToken = default(CancellationToken))
        {
            return EnumerateDirectory(path, -1, "", "", Selection.Standard, userIdFormat, cancelToken);
        }

        /// <summary>
        /// Returns a enumerable that enumerates the sub-directories or files contained in a directory.
        /// By default listAfter and listBefore is empty and we enuerate all the directory entries.
        /// </summary>
        /// <param name="path">Path of the directory</param>
        /// <param name="selection">Define data to return for each entry</param>
        /// <param name="userIdFormat">Way the user or group object will be represented. Won't be honored for Selection.Minimal</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>Enumerable that enumerates over the contents</returns>
        internal virtual IEnumerable<DirectoryEntry> EnumerateDirectory(string path, Selection selection, UserGroupRepresentation? userIdFormat,
            CancellationToken cancelToken = default(CancellationToken))
        {
            return EnumerateDirectory(path, -1, "", "", selection, userIdFormat, cancelToken);
        }

        /// <summary>
        /// Returns a enumerable that enumerates the sub-directories or files contained in a directory
        /// </summary>
        /// <param name="path">Path of the directory</param>
        /// <param name="maxEntries">List size to obtain from server</param>
        /// <param name="listAfter">Filename after which list of files should be obtained from server</param>
        /// <param name="listBefore">Filename till which list of files should be obtained from server</param>
        /// <param name="selection">Define data to return for each entry</param>
        /// <param name="userIdFormat">Way the user or group object will be represented. Won't be honored for Selection.Minimal</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>Enumerable that enumerates over the contents</returns>
        internal IEnumerable<DirectoryEntry> EnumerateDirectory(string path, int maxEntries, string listAfter, string listBefore,
            Selection selection = Selection.Standard, UserGroupRepresentation? userIdFormat = UserGroupRepresentation.ObjectID, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            return new FileStatusOutput<DirectoryEntry>(listBefore, listAfter, maxEntries, userIdFormat, this, path, selection, cancelToken);
        }

        #region Access, Acl, Permission

        /// <summary>
        /// Asynchronously sets the expiry time
        /// </summary>
        /// <param name="path">Path of the file</param>
        /// <param name="eopt">Different type of expiry method for example: never expire, relative to now, etc that defines how to evaluate expiryTime</param>
        /// <param name="expiryTime">Expiry time value. It's interpretation depends on what ExpiryOption user passes</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public virtual async Task SetExpiryTimeAsync(string path, ExpiryOption eopt, long expiryTime, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            OperationResponse resp = new OperationResponse();
            await Core.SetExpiryTimeAsync(path, eopt, expiryTime, this, new RequestOptions(GetPerRequestTimeout(), new ExponentialRetryPolicy()),
                resp, cancelToken).ConfigureAwait(false);
            if (!resp.IsSuccessful)
            {
                throw GetExceptionFromResponse(resp, $"Error in setting expiry time for path {path}.");
            }
        }
        /// <summary>
        /// Synchronously sets the expiry time
        /// </summary>
        /// <param name="path">Path of the file</param>
        /// <param name="eopt">Different type of expiry method for example: never expire, relative to now, etc that defines how to evaluate expiryTime</param>
        /// <param name="expiryTime">Expiry time value in milliseconds. It's interpretation depends on what ExpiryOption user passes</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public virtual void SetExpiryTime(string path, ExpiryOption eopt, long expiryTime,
            CancellationToken cancelToken = default(CancellationToken))
        {
            SetExpiryTimeAsync(path, eopt, expiryTime, cancelToken).GetAwaiter().GetResult();
        }
        /// <summary>
        /// Asynchronously checks if the user/group has specified access of the given path
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="rwx">Permission to check in "rwx" string form. For example if the user wants to see if it has read, execute permission, the string would be r-x </param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>True if client has access to the path else false</returns>
        public virtual async Task<bool> CheckAccessAsync(string path, string rwx, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            OperationResponse resp = new OperationResponse();
            await Core.CheckAccessSync(path, rwx, this, new RequestOptions(GetPerRequestTimeout(), new ExponentialRetryPolicy()), resp, cancelToken).ConfigureAwait(false);
            if (!resp.IsSuccessful)
            {
                if (resp.HttpStatus == HttpStatusCode.Forbidden || resp.HttpStatus == HttpStatusCode.Unauthorized)
                {
                    return false;
                }
                throw GetExceptionFromResponse(resp, $"Error in checking access for path {path}.");
            }
            return true;
        }
        /// <summary>
        /// Checks if the user/group has specified access of the given path
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="rwx">Permission to check in "rwx" string form. For example if the user wants to see if it has read, execute permission, the string would be r-x </param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>True if client has access to the path else false</returns>
        public virtual bool CheckAccess(string path, string rwx,
            CancellationToken cancelToken = default(CancellationToken))
        {
            return CheckAccessAsync(path, rwx, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Asynchronously sets the permission of the specified path
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="permission">Permission to check in unix octal form. For example if the user wants to see if owner has read, write execute permission, all groups has read write
        ///                          permission and others has read permission the string would be 741 </param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public virtual async Task SetPermissionAsync(string path, string permission, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            OperationResponse resp = new OperationResponse();
            await Core.SetPermissionAsync(path, permission, this, new RequestOptions(GetPerRequestTimeout(), new ExponentialRetryPolicy()), resp, cancelToken).ConfigureAwait(false);
            if (!resp.IsSuccessful)
            {
                throw GetExceptionFromResponse(resp, $"Error in setting permission for path {path}.");
            }
        }
        /// <summary>
        /// Sets the permission of the specified path
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="permission">Permission to check in unix octal form. For example if the user wants to see if owner has read, write execute permission, all groups has read write
        ///                          permission and others has read permission the string would be 741 </param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public virtual void SetPermission(string path, string permission,
            CancellationToken cancelToken = default(CancellationToken))
        {
            SetPermissionAsync(path, permission, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Asynchronously modifies acl entries of a file or directory with given ACL list. It merges the exisitng ACL list with given list.
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="aclSpec">List of Acl Entries to modify</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public virtual async Task ModifyAclEntriesAsync(string path, List<AclEntry> aclSpec, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            OperationResponse resp = new OperationResponse();
            await Core.ModifyAclEntriesAsync(path, aclSpec, this, new RequestOptions(GetPerRequestTimeout(), new ExponentialRetryPolicy()), resp, cancelToken).ConfigureAwait(false);
            if (!resp.IsSuccessful)
            {
                throw GetExceptionFromResponse(resp, $"Error in modifying ACL entries {AclEntry.SerializeAcl(aclSpec, false)} for path {path}.");
            }
        }

        /// <summary>
        /// Modifies acl entries of a file or directory with given ACL list. It merges the exisitng ACL list with given list.
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="aclSpec">List of Acl Entries to modify</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public virtual void ModifyAclEntries(string path, List<AclEntry> aclSpec,
            CancellationToken cancelToken = default(CancellationToken))
        {
            ModifyAclEntriesAsync(path, aclSpec, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Asynchronously sets Acl Entries for a file or directory. It wipes out the existing Acl entries for the path.
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="aclSpec">List of Acl Entries to set </param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public virtual async Task SetAclAsync(string path, List<AclEntry> aclSpec, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            OperationResponse resp = new OperationResponse();
            await Core.SetAclAsync(path, aclSpec, this, new RequestOptions(GetPerRequestTimeout(), new ExponentialRetryPolicy()), resp, cancelToken).ConfigureAwait(false);
            if (!resp.IsSuccessful)
            {
                throw GetExceptionFromResponse(resp, $"Error in modifying ACL entries {AclEntry.SerializeAcl(aclSpec, false)} for path {path}.");
            }
        }

        /// <summary>
        /// Sets Acl Entries for a file or directory. It wipes out the existing Acl entries for the path.
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="aclSpec">List of Acl Entries to set </param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public virtual void SetAcl(string path, List<AclEntry> aclSpec,
            CancellationToken cancelToken = default(CancellationToken))
        {
            SetAclAsync(path, aclSpec, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Sets the owner or/and group of the path
        /// </summary>
        /// <param name="path">Path of file or directory</param>
        /// <param name="owner">Owner ID</param>
        /// <param name="group">Group ID</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public virtual void SetOwner(string path, string owner, string group, CancellationToken cancelToken = default(CancellationToken))
        {
            SetOwnerAsync(path, owner, group, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Asynchronously sets the owner or/and group of the path
        /// </summary>
        /// <param name="path">Path of file or directory</param>
        /// <param name="owner">Owner ID</param>
        /// <param name="group">Group ID</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public virtual async Task SetOwnerAsync(string path, string owner, string group, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            OperationResponse resp = new OperationResponse();
            await Core.SetOwnerAsync(path, owner, group, this, new RequestOptions(GetPerRequestTimeout(), new ExponentialRetryPolicy()), resp, cancelToken).ConfigureAwait(false);
            if (!resp.IsSuccessful)
            {
                throw GetExceptionFromResponse(resp, $"Error in setting owner or group for path {path}.");
            }
        }

        /// <summary>
        /// Asynchronously removes specified Acl Entries for a file or directory.
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="aclSpec">List of Acl Entries to remove</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public virtual async Task RemoveAclEntriesAsync(string path, List<AclEntry> aclSpec, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            OperationResponse resp = new OperationResponse();
            await Core.RemoveAclEntriesAsync(path, aclSpec, this, new RequestOptions(GetPerRequestTimeout(), new ExponentialRetryPolicy()), resp, cancelToken).ConfigureAwait(false);
            if (!resp.IsSuccessful)
            {
                throw GetExceptionFromResponse(resp, $"Error in modifying ACL entries {AclEntry.SerializeAcl(aclSpec, true)} for path {path}.");
            }
        }

        /// <summary>
        /// Removes specified Acl Entries for a file or directory.
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="aclSpec">List of Acl Entries to remove</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public virtual void RemoveAclEntries(string path, List<AclEntry> aclSpec,
            CancellationToken cancelToken = default(CancellationToken))
        {
            RemoveAclEntriesAsync(path, aclSpec, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Asynchronously removes all Acl Entries for a file or directory.
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public virtual async Task RemoveAllAclsAsync(string path, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            OperationResponse resp = new OperationResponse();
            await Core.RemoveAclAsync(path, this, new RequestOptions(GetPerRequestTimeout(), new ExponentialRetryPolicy()), resp, cancelToken).ConfigureAwait(false);
            if (!resp.IsSuccessful)
            {
                throw GetExceptionFromResponse(resp, $"Error in removing all ACL entries for path {path}.");
            }
        }

        /// <summary>
        /// Removes all Acl Entries for a file or directory.
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public virtual void RemoveAllAcls(string path, CancellationToken cancelToken = default(CancellationToken))
        {
            RemoveAllAclsAsync(path, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Asynchronously removes all Acl Entries of AclScope default for a file or directory.
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public virtual async Task RemoveDefaultAclsAsync(string path, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            OperationResponse resp = new OperationResponse();
            await Core.RemoveDefaultAclAsync(path, this, new RequestOptions(GetPerRequestTimeout(), new ExponentialRetryPolicy()), resp, cancelToken).ConfigureAwait(false);
            if (!resp.IsSuccessful)
            {
                throw GetExceptionFromResponse(resp, $"Error in removing all default ACL entries for path {path}.");
            }
        }

        /// <summary>
        /// Removes all Acl Entries of AclScope default for a file or directory.
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public virtual void RemoveDefaultAcls(string path, CancellationToken cancelToken = default(CancellationToken))
        {
            RemoveDefaultAclsAsync(path, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Asynchronously gets the ACL entry list, owner ID, group ID, octal permission and sticky bit (only for a directory) of the file/directory
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="userIdFormat">way to represent the user/group object</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public virtual async Task<AclStatus> GetAclStatusAsync(string path, UserGroupRepresentation userIdFormat = UserGroupRepresentation.ObjectID, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            OperationResponse resp = new OperationResponse();
            AclStatus status = await Core.GetAclStatusAsync(path, userIdFormat, this, new RequestOptions(GetPerRequestTimeout(), new ExponentialRetryPolicy()), resp, cancelToken).ConfigureAwait(false);
            if (!resp.IsSuccessful)
            {
                throw GetExceptionFromResponse(resp, $"Error in getting ACL entries for path {path}.");
            }
            return status;
        }

        /// <summary>
        /// Gets the ACL entry list, owner ID, group ID, octal permission and sticky bit (only for a directory) of the file/directory
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="userIdFormat">way to represent the user/group object</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public virtual AclStatus GetAclStatus(string path, UserGroupRepresentation userIdFormat = UserGroupRepresentation.ObjectID, CancellationToken cancelToken = default(CancellationToken))
        {
            return GetAclStatusAsync(path, userIdFormat, cancelToken).GetAwaiter().GetResult();
        }

        #endregion

        #region Trash Enumerate, Restore
        /// <summary>
        /// Search trash under a account with hint and a starting point. This is a long running operation,
        /// and user is updated with progress periodically.
        /// Caution: Undeleting files is a best effort operation.  There are no guarantees that a file can be restored once it is deleted. The use of this API is enabled via whitelisting. If your ADL account is not whitelisted, then using this api will throw Not immplemented exception. For further information and assistance please contact Microsoft support.
        /// </summary>
        /// <param name="hint">String to match</param>
        /// <param name="listAfter">Token returned by system in the previous API invocation</param>
        /// <param name="numResults">Search is executed until we find numResults or search completes. Maximum allowed value for this param is 4000. The number of returned entries could be more or less than numResults</param>
        /// <param name="progressTracker">Object to track progress of the task. Can be null</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public virtual IEnumerable<TrashEntry> EnumerateDeletedItems(string hint, string listAfter, int numResults, IProgress<EnumerateDeletedItemsProgress> progressTracker, CancellationToken cancelToken = default(CancellationToken))
        {
            return EnumerateDeletedItemsAsync(hint, listAfter, numResults, progressTracker, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Search trash under a account with hint and a starting point. This is a long running operation,
        /// and user is updated with progress periodically.
        /// Caution: Undeleting files is a best effort operation.  There are no guarantees that a file can be restored once it is deleted. The use of this API is enabled via whitelisting. If your ADL account is not whitelisted, then using this api will throw Not immplemented exception. For further information and assistance please contact Microsoft support.
        /// </summary>
        /// <param name="hint">String to match</param>
        /// <param name="listAfter">Token returned by system in the previous API invocation</param>
        /// <param name="numResults">Search is executed until we find numResults or search completes. Maximum allowed value for this param is 4000. The number of returned entries could be more or less than numResults</param>
        /// <param name="progressTracker">Object to track progress of the task. Can be null</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>A tuple containing the list of trash entries and the continuation token</returns>
        public virtual (IEnumerable<TrashEntry>, string) EnumerateDeletedItemsWithToken(string hint, string listAfter, int numResults, IProgress<EnumerateDeletedItemsProgress> progressTracker, CancellationToken cancelToken = default(CancellationToken))
        {
            return EnumerateDeletedItemsWithTokenAsync(hint, listAfter, numResults, progressTracker, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Asynchronously gets the trash entries
        /// Caution: Undeleting files is a best effort operation.  There are no guarantees that a file can be restored once it is deleted. The use of this API is enabled via whitelisting. If your ADL account is not whitelisted, then using this api will throw Not immplemented exception. For further information and assistance please contact Microsoft support.
        /// </summary>
        /// <param name="hint">String to match. Cannot be empty.</param>
        /// <param name="listAfter">Token returned by system in the previous API invocation</param>
        /// <param name="numResults">Search is executed until we find numResults or search completes. Maximum allowed value for this param is 4000. The number of returned entries could be more or less than numResults</param>
        /// <param name="progressTracker">Object to track progress of the task. Can be null</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public virtual async Task<IEnumerable<TrashEntry>> EnumerateDeletedItemsAsync(string hint, string listAfter, int numResults, IProgress<EnumerateDeletedItemsProgress> progressTracker, CancellationToken cancelToken)
        {
            var result = await EnumerateDeletedItemsInternalAsync(hint, listAfter, numResults, progressTracker, cancelToken).ConfigureAwait(false);
            return result.trashEntries;
        }

        /// <summary>
        /// Asynchronously gets the trash entries along with the next listAfter token.
        /// Caution: Undeleting files is a best effort operation.  There are no guarantees that a file can be restored once it is deleted. The use of this API is enabled via whitelisting. If your ADL account is not whitelisted, then using this api will throw Not immplemented exception. For further information and assistance please contact Microsoft support.
        /// </summary>
        /// <param name="hint">String to match. Cannot be empty.</param>
        /// <param name="listAfter">Token returned by system in the previous API invocation</param>
        /// <param name="numResults">Search is executed until we find numResults or search completes. Maximum allowed value for this param is 4000. The number of returned entries could be more or less than numResults</param>
        /// <param name="progressTracker">Object to track progress of the task. Can be null</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>A tuple containing the list of trash entries and the next listAfter token</returns>
        public virtual async Task<(IEnumerable<TrashEntry>, string)> EnumerateDeletedItemsWithTokenAsync(string hint, string listAfter, int numResults, IProgress<EnumerateDeletedItemsProgress> progressTracker, CancellationToken cancelToken)
        {
            var result = await EnumerateDeletedItemsInternalAsync(hint, listAfter, numResults, progressTracker, cancelToken).ConfigureAwait(false);
            return (result.trashEntries, result.nextListAfter);
        }

        private async Task<(List<TrashEntry> trashEntries, string nextListAfter)> EnumerateDeletedItemsInternalAsync(string hint, string listAfter, int numResults, IProgress<EnumerateDeletedItemsProgress> progressTracker, CancellationToken cancelToken)
        {
            List<TrashEntry> trashEntries = new List<TrashEntry>();
            string nextListAfter = listAfter;
            long numSearched = 0;
            int numFound = 0;

            if (numResults > 4000 || numResults < 0)
            {
                numResults = 4000;
            }

            while (true)
            {
                OperationResponse resp = new OperationResponse();

                var requestTimeout = _perRequestTimeoutForEnumerateTrash < GetPerRequestTimeout() ? GetPerRequestTimeout() : _perRequestTimeoutForEnumerateTrash;
                TrashStatus trashstatus = await Core.EnumerateDeletedItemsAsync(hint, nextListAfter, numResults - numFound, this, new RequestOptions(null, requestTimeout, new ExponentialRetryPolicy()), resp, cancelToken).ConfigureAwait(false);
                if (!resp.IsSuccessful)
                {
                    if (!(resp.Ex is OperationCanceledException))
                    {
                        throw GetExceptionFromResponse(resp, $"Error in EnumerateDeletedItemsAsync hint:{hint}, listAfter:{listAfter}, numResults:{numResults}.");
                    }
                }

                if (trashstatus != null)
                {
                    numSearched += trashstatus.NumSearched;

                    if (trashstatus.TrashEntries != null)
                    {
                        trashEntries.AddRange(trashstatus.TrashEntries);
                        numFound += trashstatus.NumFound;
                    }

                    if (progressTracker != null)
                    {
                        EnumerateDeletedItemsProgress progress = new EnumerateDeletedItemsProgress { NumFound = numFound, NumSearched = numSearched, NextListAfter = trashstatus.NextListAfter };
                        progressTracker.Report(progress);
                    }

                    // empty NextListAfter implies search is complete. Break when search is complete
                    // or when we have found requisite number of entries
                    if (String.IsNullOrEmpty(trashstatus.NextListAfter) || numFound >= numResults)
                    {
                        break;
                    }

                    nextListAfter = trashstatus.NextListAfter;
                }
                else
                {
                    break;
                }
            }

            return (trashEntries, nextListAfter);
        }

        /// <summary>
        /// Synchronously Restores trash entry
        /// Caution: Undeleting files is a best effort operation.  There are no guarantees that a file can be restored once it is deleted. The use of this API is enabled via whitelisting. If your ADL account is not whitelisted, then using this api will throw Not immplemented exception. For further information and assistance please contact Microsoft support.
        /// </summary>
        /// <param name="pathOfFileToRestoreInTrash">Trash Directory path returned by enumeratedeleteditems</param>
        /// <param name="restoreDestination">Destination for restore</param>
        /// <param name="type">type of restore - file or directory</param>
        /// <param name="restoreAction">Action to take in case of destination conflict</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public virtual void RestoreDeletedItems(string pathOfFileToRestoreInTrash, string restoreDestination, string type, string restoreAction = "", CancellationToken cancelToken = default(CancellationToken))
        {
            RestoreDeletedItemsAsync(pathOfFileToRestoreInTrash, restoreDestination, type, restoreAction, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Asynchronously Restores trash entry
        /// Caution: Undeleting files is a best effort operation.  There are no guarantees that a file can be restored once it is deleted. The use of this API is enabled via whitelisting. If your ADL account is not whitelisted, then using this api will throw Not immplemented exception. For further information and assistance please contact Microsoft support.
        /// </summary>
        /// <param name="pathOfFileToRestoreInTrash">Trash Directory path returned by enumeratedeleteditems</param>
        /// <param name="restoreDestination">Destination for restore</param>
        /// <param name="type">type of restore - file or directory</param>
        /// <param name="restoreAction">Action to take in case of destination conflict</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public virtual async Task RestoreDeletedItemsAsync(string pathOfFileToRestoreInTrash, string restoreDestination, string type, string restoreAction = "", CancellationToken cancelToken = default(CancellationToken))
        {
            OperationResponse resp = new OperationResponse();
            await Core.RestoreDeletedItemsAsync(pathOfFileToRestoreInTrash, restoreDestination, type, restoreAction, this, new RequestOptions(GetPerRequestTimeout(), new NoRetryPolicy()), resp, cancelToken).ConfigureAwait(false);
            if (!resp.IsSuccessful)
            {
                throw GetExceptionFromResponse(resp, $"Error in RestoreDeletedItemsAsync pathOfFileToRestoreInTrash:{pathOfFileToRestoreInTrash}, restoreDestination:{restoreDestination}, type:{type}, restoreAction:{restoreAction}");
            }
        }

        #endregion

        /// <summary>
        /// Gets content summary of a file or directory.
        /// It is highly recomended to set ServicePointManager.DefaultConnectionLimit to the number of threads application wants the sdk to use before creating any instance of AdlsClient.
        /// By default ServicePointManager.DefaultConnectionLimit is set to 2.
        /// </summary>
        /// <param name="path">Path of the directory or file</param>
        /// <param name="numThreads">Number of threads</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public virtual ContentSummary GetContentSummary(string path, int numThreads = -1, CancellationToken cancelToken = default(CancellationToken))
        {
            return ContentProcessor.GetContentSummary(this, path, numThreads, cancelToken);
        }

        /// <summary>
        /// Asynchronous API to perform concurrent append at server. The offset at which append will occur is determined by server. Asynchronous operation.
        /// It is highly recomended to call this api with data size less than equals 4MB. Backend gurantees 4MB atomic appends.
        /// </summary>
        /// <param name="path">Path of the file</param>
        /// <param name="autoCreate"></param>
        /// <param name="dataBytes">Array of bytes to write to the file</param>
        /// <param name="offset">Offset in the byte array</param>
        /// <param name="length">Number of bytes to write from the offset</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public virtual async Task ConcurrentAppendAsync(string path, bool autoCreate, byte[] dataBytes, int offset,
            int length, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null or empty");
            }
            OperationResponse resp = new OperationResponse();
            await Core.ConcurrentAppendAsync(path, autoCreate, dataBytes, offset, length, this, new RequestOptions(GetPerRequestTimeout(), new NoRetryPolicy()), resp, cancelToken).ConfigureAwait(false);
            if (!resp.IsSuccessful)
            {
                throw GetExceptionFromResponse(resp, $"Error in concurrent append for file {path}.");
            }
        }

        /// <summary>
        /// Synchronous API to perform concurrent append at server. The offset at which append will occur is determined by server.
        /// It is highly recomended to call this api with data size less than equals 4MB. Backend gurantees 4MB atomic appends.
        /// </summary>
        /// <param name="path">Path of the file</param>
        /// <param name="autoCreate"></param>
        /// <param name="dataBytes">Array of bytes to write to the file</param>
        /// <param name="offset">Offset in the byte array</param>
        /// <param name="length">Number of bytes to write from the offset</param>
        public virtual void ConcurrentAppend(string path, bool autoCreate, byte[] dataBytes, int offset, int length)
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null or empty");
            }
            OperationResponse resp = new OperationResponse();
            Core.ConcurrentAppend(path, autoCreate, dataBytes, offset, length, this, new RequestOptions(GetPerRequestTimeout(), new NoRetryPolicy()), resp);
            if (!resp.IsSuccessful)
            {
                throw GetExceptionFromResponse(resp, $"Error in concurrent append for file {path}.");
            }
        }

        /// <summary>
        /// Checks whether file or directory exists
        /// </summary>
        /// <param name="path">Path name</param>
        /// <returns>True if the path exists else false</returns>
        public virtual bool CheckExists(string path)
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null or empty");
            }
            try
            {
                GetDirectoryEntry(path);
            }
            catch (AdlsException e)
            {
                if (e.HttpStatus == HttpStatusCode.NotFound)
                {
                    return false;
                }
                throw e;
            }
            return true;
        }

        #endregion

        #region SDKTools
        /// <summary>
        /// Upload directory or file from local to remote. Transfers the contents under source directory under 
        /// the destination directory. Transfers the source file and saves it as the destination path.
        /// This method does not throw any exception for any entry's transfer failure. Refer the return value <see cref="TransferStatus"/> to 
        /// get the status/exception of each entry's transfer.
        /// It is highly recomended to set ServicePointManager.DefaultConnectionLimit to the number of threads application wants the sdk to use before creating any instance of AdlsClient.
        /// By default ServicePointManager.DefaultConnectionLimit is set to 2.
        /// By default files are uploaded at new line boundaries. However if files does not have newline within 4MB chunks,
        /// the transfer will fail. In that case it is required to pass true to <paramref name="isBinary"/> to avoid uploads at newline boundaries.
        /// </summary>
        /// <param name="srcPath">Local source path</param>
        /// <param name="destPath">Remote destination path - It should always be a directory.</param>
        /// <param name="numThreads">Number of threads- if not passed will take default number of threads (8 times the number of physical cores)</param>
        /// <param name="shouldOverwrite">Whether to overwrite or skip if the destination exists</param>
        /// <param name="progressTracker">Progresstracker to track progress of file transfer</param>
        /// <param name="notRecurse">If true then does an enumeration till level one else does recursive enumeration</param>
        /// <param name="resume">If true then we want to resume from last transfer</param>
        /// <param name="isBinary">If false then writes files to data lake at newline boundaries, however if the file has no newline within 4MB chunks it will throw exception.
        /// If true, then upload at new line boundaries is not guranteed but the upload will be faster. By default false, if file has no newlines within 4MB chunks true should be apssed</param>
        /// <param name="cancelToken">Cancellation token</param>
        /// <returns>Transfer Status encapsulating the details of upload</returns>
        public virtual TransferStatus BulkUpload(string srcPath, string destPath, int numThreads = -1, IfExists shouldOverwrite = IfExists.Overwrite, IProgress<TransferStatus> progressTracker = null, bool notRecurse = false, bool resume = false, bool isBinary = false, CancellationToken cancelToken = default(CancellationToken))
        {
            return BulkUpload(srcPath, destPath, numThreads, shouldOverwrite, false, progressTracker, notRecurse, resume, isBinary, cancelToken);
        }
        /// <summary>
        /// Upload directory or file from local to remote. Transfers the contents under source directory under 
        /// the destination directory. Transfers the source file and saves it as the destination path.
        /// This method does not throw any exception for any entry's transfer failure. Refer the return value <see cref="TransferStatus"/> to 
        /// get the status/exception of each entry's transfer.
        /// By default logs the transfer progress in system's temp path, so that user can recover using <paramref name="resume"/> if upload has crashed.
        /// This progress logging can be disabled using <paramref name="disableTransferLogging"/>.
        /// It is highly recomended to set ServicePointManager.DefaultConnectionLimit to the number of threads application wants the sdk to use before creating any instance of AdlsClient.
        /// By default ServicePointManager.DefaultConnectionLimit is set to 2.
        /// By default files are uploaded at new line boundaries. However if files does not have newline within 4MB chunks,
        /// the transfer will fail. In that case it is required to pass true to <paramref name="isBinary"/> to avoid uploads at newline boundaries.
        /// </summary>
        /// <param name="srcPath">Local source path</param>
        /// <param name="destPath">Remote destination path - It should always be a directory.</param>
        /// <param name="numThreads">Number of threads- Default -1, if not passed will take default number of threads (8 times the number of physical cores)</param>
        /// <param name="shouldOverwrite">Whether to overwrite or skip if the destination exists, Default IfExists.Overwrite</param>
        /// <param name="disableTransferLogging">If true, logging of transfer progress is disabled. This and <paramref name="resume"/> cannot be true at same time. Default false</param>
        /// <param name="progressTracker">Progresstracker to track progress of file transfer, Default null</param>
        /// <param name="notRecurse">If true then does an enumeration till level one else does recursive enumeration, Default false</param>
        /// <param name="resume">If true then we want to resume from last transfer, Default false</param>
        /// <param name="isBinary">If false then writes files to data lake at newline boundaries, however if the file has no newline within 4MB chunks it will throw exception.
        /// If true, then upload at new line boundaries is not guranteed but the upload will be faster. By default false, if file has no newlines within 4MB chunks true should be apssed</param>
        /// <param name="cancelToken">Cancellation token</param>
        /// <returns>Transfer Status encapsulating the details of upload</returns>
        public virtual TransferStatus BulkUpload(string srcPath, string destPath, int numThreads, IfExists shouldOverwrite, bool disableTransferLogging, IProgress<TransferStatus> progressTracker, bool notRecurse, bool resume, bool isBinary, CancellationToken cancelToken)
        {
            return FileUploader.Upload(srcPath, destPath, this, numThreads, shouldOverwrite, progressTracker, notRecurse, disableTransferLogging, resume, isBinary, cancelToken);
        }
        /// <summary>
        /// Download directory or file from remote server to local. Transfers the contents under source directory under 
        /// the destination directory. Transfers the source file and saves it as the destination path.
        /// This method does not throw any exception for any entry's transfer failure. Refer the return value <see cref="TransferStatus"/> to 
        /// get the status/exception of each entry's transfer.
        /// It is highly recomended to set ServicePointManager.DefaultConnectionLimit to the number of threads application wants the sdk to use before creating any instance of AdlsClient.
        /// By default ServicePointManager.DefaultConnectionLimit is set to 2.
        /// </summary>
        /// <param name="srcPath">Remote source path</param>
        /// <param name="destPath">Local destination path. It should always be a directory.</param>
        /// <param name="numThreads">Number of threads- if not passed will take default number of threads (8 times the number of physical cores)</param>
        /// <param name="shouldOverwrite">Whether to overwrite or skip if the destination exists</param>
        /// <param name="progressTracker">Progresstracker to track progress of file transfer</param>
        /// <param name="notRecurse">If true then does an enumeration till level one else does recursive enumeration</param>
        /// <param name="resume">If true then we want to resume from last transfer</param>
        /// <param name="cancelToken">Cancel token</param>
        /// <returns>Transfer status encapsulating the details of download</returns>
        public virtual TransferStatus BulkDownload(string srcPath, string destPath, int numThreads = -1, IfExists shouldOverwrite = IfExists.Overwrite, IProgress<TransferStatus> progressTracker = null, bool notRecurse = false, bool resume = false, CancellationToken cancelToken = default(CancellationToken))
        {
            return BulkDownload(srcPath, destPath, numThreads, shouldOverwrite, false, progressTracker, notRecurse, resume, cancelToken);
        }

        /// <summary>
        /// Download directory or file from remote server to local. Transfers the contents under source directory under 
        /// the destination directory. Transfers the source file and saves it as the destination path.
        /// This method does not throw any exception for any entry's transfer failure. Refer the return value <see cref="TransferStatus"/> to 
        /// get the status/exception of each entry's transfer.
        /// By default logs the transfer progress in system's temp path, so that user can recover using <paramref name="resume"/> if upload has crashed.
        /// This progress logging can be disabled using <paramref name="disableTransferLogging"/>.
        /// It is highly recomended to set ServicePointManager.DefaultConnectionLimit to the number of threads application wants the sdk to use before creating any instance of AdlsClient.
        /// By default ServicePointManager.DefaultConnectionLimit is set to 2.
        /// </summary>
        /// <param name="srcPath">Remote source path</param>
        /// <param name="destPath">Local destination path. It should always be a directory.</param>
        /// <param name="numThreads">Number of threads- Default -1 if not passed will take default number of threads (8 times the number of physical cores)</param>
        /// <param name="shouldOverwrite">Whether to overwrite or skip if the destination exists, Default IfExists.Overwrite</param>
        /// <param name="disableTransferLogging">If true, logging of transfer progress is disabled. This and <paramref name="resume"/> cannot be true at same time. Default false</param>
        /// <param name="progressTracker">Progresstracker to track progress of file transfer, Default null</param>
        /// <param name="notRecurse">If true then does an enumeration till level one else does recursive enumeration, default false</param>
        /// <param name="resume">If true then we want to resume from last transfer, default false</param>
        /// <param name="cancelToken">Cancel token</param>
        /// <returns>Transfer status encapsulating the details of download</returns>
        public virtual TransferStatus BulkDownload(string srcPath, string destPath, int numThreads, IfExists shouldOverwrite, bool disableTransferLogging, IProgress<TransferStatus> progressTracker, bool notRecurse, bool resume, CancellationToken cancelToken)
        {
            return FileDownloader.Download(srcPath, destPath, this, numThreads, shouldOverwrite, progressTracker, notRecurse, disableTransferLogging, resume, cancelToken);
        }

        /// <summary>
        /// Change Acl (Modify, set and remove) recursively on a directory tree
        /// It is highly recomended to set ServicePointManager.DefaultConnectionLimit to the number of threads application wants the sdk to use before creating any instance of AdlsClient.
        /// By default ServicePointManager.DefaultConnectionLimit is set to 2.
        /// </summary>
        /// <param name="path">The root directory path from where the Acl change will begin</param>
        /// <param name="aclEntries">Acl entries to add or set or remove depending on the input</param>
        /// <param name="type">Type of modification <see cref="RequestedAclType"/></param>
        /// <param name="threadCount">Number of threads to use</param>
        /// <returns>Stats- total number of files and directories processed</returns>
        public virtual AclProcessorStats ChangeAcl(string path, List<AclEntry> aclEntries, RequestedAclType type, int threadCount = -1)
        {
            return AclProcessor.RunAclProcessor(path, this, aclEntries, type, threadCount);
        }

        /// <summary>
        /// Change Acl (Modify, set and remove) recursively on a directory tree
        /// It is highly recomended to set ServicePointManager.DefaultConnectionLimit to the number of threads application wants the sdk to use before creating any instance of AdlsClient.
        /// By default ServicePointManager.DefaultConnectionLimit is set to 2.
        /// </summary>
        /// <param name="path">The root directory path from where the Acl change will begin</param>
        /// <param name="aclEntries">Acl entries to add or set or remove depending on the input</param>
        /// <param name="type">Type of modification <see cref="RequestedAclType"/></param>
        /// <param name="threadCount">Number of threads to use</param>
        /// <param name="statusTracker">Tracker to track progress of acl processor</param>
        /// <param name="cancelToken">CancellationToken</param>
        /// <returns>Stats- total number of files and directories processed</returns>
        public virtual AclProcessorStats ChangeAcl(string path, List<AclEntry> aclEntries, RequestedAclType type, int threadCount, IProgress<AclProcessorStats> statusTracker, CancellationToken cancelToken)
        {
            return AclProcessor.RunAclProcessor(path, this, aclEntries, type, threadCount, statusTracker, cancelToken);
        }

        /// <summary>
        /// Recursively dumps file property of alldirectories or/and files under the given path to a local or adl file. File property can be disk usage or Acl or both.
        /// It is highly recomended to set ServicePointManager.DefaultConnectionLimit to the number of threads application wants the sdk to use before creating any instance of AdlsClient.
        /// By default ServicePointManager.DefaultConnectionLimit is set to 2.
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="getAclUsage">True if we want Acl usage</param>
        /// <param name="dumpFileName">Filename containing the ACL or Disk usage dump</param>
        /// <param name="getDiskUsage">True if we want disk usage</param>
        /// <param name="saveToLocal">True if we want to save to local file else save to ADL</param>
        /// <param name="numThreads">Number of threads</param>
        /// <param name="displayFiles">True if we want to display properties of files. By default we show properties of directories. If this is false we would not retrieve Acls for files.</param>
        /// <param name="hideConsistentAcl">Do not show directory subtree if the ACLs are the same throughout the entire subtree. This 
        ///                                 makes it easier to see only the paths up to which the ACLs differ. For example if all files and folders under /a/b are the same, do not show the subtree under /a/b, and just output /a/b with 'True' in the Consistent ACL column. Cannot be set if IncludeFiles is not set, because consistent Acl cannot be determined without retrieving acls for the files.</param>
        /// <param name="maxDepth">Maximum depth till which we want to view the properties</param>
        /// <param name="cancelToken">CancellationToken to cancel the recursive process</param>
        public virtual void GetFileProperties(string path, bool getAclUsage, string dumpFileName, bool getDiskUsage = true, bool saveToLocal = true, int numThreads = -1, bool displayFiles = false, bool hideConsistentAcl = false, long maxDepth = Int64.MaxValue, CancellationToken cancelToken = default(CancellationToken))
        {
            if (!(getAclUsage || getDiskUsage))
            {
                throw new ArgumentException("At least one option of getAclUsage and getDiskUsage need to be set as true.");
            }

            if (!displayFiles && hideConsistentAcl)
            {
                throw new ArgumentException("hideConsistentAcl cannot be true when displayFiles is false because consistent Acl cannot be determined unless we retrieve acls for the files also.");
            }

            PropertyManager.GetFileProperty(path, this, getAclUsage, getDiskUsage, dumpFileName, saveToLocal, numThreads, displayFiles, hideConsistentAcl, maxDepth, cancelToken);
        }

        #endregion

        /// <summary>
        /// Returns a ADLS Exception based on response from the server
        /// </summary>
        /// <param name="resp">Response encapsulating errors or exceptions</param>
        /// <param name="defaultMessage">Default message</param>
        /// <returns>Adls Exception</returns>
        public AdlsException GetExceptionFromResponse(OperationResponse resp, string defaultMessage)
        {

            string exceptionMessage = defaultMessage;
            // For unauthorized show the authorization header length
            exceptionMessage += $"\nOperation: {resp.OpCode} failed with {(resp.HttpStatus > 0 ? "HttpStatus:" + resp.HttpStatus : "")} {(resp.HttpStatus == HttpStatusCode.Unauthorized ? $" Token Length: {resp.AuthorizationHeaderLength}" : "")} ";

            if (!string.IsNullOrEmpty(resp.Error))
            {
                exceptionMessage += "Error: " + resp.Error;
            }
            else if (!string.IsNullOrEmpty(resp.RemoteExceptionName))
            {
                exceptionMessage += $"RemoteException: {resp.RemoteExceptionName} {resp.RemoteExceptionMessage} JavaClassName: {resp.RemoteExceptionJavaClassName}.";
            }
            else
            {
                // If remote error is nonjson then print the actual error for exception
                exceptionMessage += $"Unknown Error: {resp.Ex.Message} Source: {resp.Ex.Source} StackTrace: {resp.Ex.StackTrace}.\n" + $"{(string.IsNullOrEmpty(resp.RemoteErrorNonJsonResponse) ? "" : $"RemoteJsonErrorResponse: {resp.RemoteErrorNonJsonResponse}")}.";
            }
            exceptionMessage += $"\nLast encountered exception thrown after {(resp.Retries + 1)} tries. ";
            if (resp.ExceptionHistory != null) exceptionMessage += "[" + resp.ExceptionHistory + "]";
            exceptionMessage += $"\n[ServerRequestId:{resp.RequestId}]";
            AdlsException exception = new AdlsException(exceptionMessage)
            {
                Error = resp.Error,
                Ex = resp.Ex,
                ExceptionHistory = resp.ExceptionHistory,
                RemoteExceptionName = resp.RemoteExceptionName,
                RemoteExceptionJavaClassName = resp.RemoteExceptionJavaClassName,
                RemoteExceptionMessage = resp.RemoteExceptionMessage,
                HttpMessage = resp.HttpMessage,
                HttpStatus = resp.HttpStatus,
                TraceId = resp.RequestId,
                LastCallLatency = resp.LastCallLatency
            };
            return exception;
        }
    }
}
