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
using Microsoft.Rest;
using NLog;
#if NET452
using System.Management;
#endif
[assembly: InternalsVisibleTo("Microsoft.Azure.DataLake.Store.UnitTest")]
namespace Microsoft.Azure.DataLake.Store
{
    /// <summary>
    /// Client of Azure data lake store. It contains the public APIs to perform operations of REST API which are easier to call and more usable than Core APIs. Core APIs provide more freedom but ADLSClient provide more commonly used forms.
    /// It encapsulates the Authorization token and token refresh. Contains factory methods that takes a ServiceClientCredential or a string auth token and returns instance of this class. For every operation it provides
    /// a async and sync version. Every sync method is a wait on async method with exception of Create and Concurrent append. Currently this class is not inheritable since it has not exposed constructors.
    /// </summary>
    public class AdlsClient
    {
        #region Properties
        /// <summary>
        /// Logger to log information (debug/error/trace) regarding client
        /// </summary>
        private static readonly Logger ClientLogger = LogManager.GetLogger("adls.dotnet");
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
        private ServiceClientCredentials AccessProvider { get; }
        /// <summary>
        /// SDK version- AssemblyFileVersion
        /// </summary>
        private static readonly string SdkVersion;
        // Default number of threads used by tools using the SDK
        internal static int DefaultNumThreads;
        #endregion

        #region Constructors

        /// <summary>
        /// Static constructor that initializes the static fields
        /// </summary>
        static AdlsClient()
        {
            try
            {
                Assembly assembly = typeof(AdlsClient).GetTypeInfo().Assembly;
                AssemblyFileVersionAttribute fvAttribute = assembly.GetCustomAttribute(typeof(AssemblyFileVersionAttribute)) as AssemblyFileVersionAttribute;
                string sdkVersion = fvAttribute?.Version;
                SdkVersion = string.IsNullOrEmpty(sdkVersion) ? "SDKVersionNotKnown" : sdkVersion;
            }
            catch (Exception)
            {
                SdkVersion = "SDKVersionUnknown";
            }
            string osInfo = "";
            string dotNetVersion = "";
            try
            {
#if NET452
                foreach (var os in new ManagementObjectSearcher("SELECT Caption, Version, OSArchitecture FROM Win32_OperatingSystem").Get())
                {
                    var version = os["Version"].ToString();
                    var productName = os["Caption"].ToString();
                    var architecture = os["OSArchitecture"].ToString();
                    osInfo = productName + " " + version + " " + architecture;
                }
                dotNetVersion = "NET452";
                int coreCount = 0;
                foreach (var item in new ManagementObjectSearcher("Select NumberOfCores from Win32_Processor").Get())
                {
                    coreCount += int.Parse(item["NumberOfCores"].ToString());
                }
                DefaultNumThreads = 8 * coreCount;
                ServicePointManager.DefaultConnectionLimit = DefaultNumThreads;
                ServicePointManager.Expect100Continue = false;
                ServicePointManager.UseNagleAlgorithm = false;
#else
                DefaultNumThreads = 128;
                osInfo = System.Runtime.InteropServices.RuntimeInformation.OSDescription + " " +
                         System.Runtime.InteropServices.RuntimeInformation.OSArchitecture;
#if NETSTANDARD1_4
                dotNetVersion = "NETSTANDARD1_4";
#else
                dotNetVersion="NETCOREAPP1_1";
#endif
#endif
            }
            catch (Exception)
            {
                osInfo = "OSNotKnown";
            }
            UserAgent = "AdlsDotNetSDK;" + SdkVersion + "/" + osInfo + ";" + dotNetVersion;
        }

        internal AdlsClient()
        {
        }
        private AdlsClient(string accnt, long clientId, string token, bool skipAccntValidation = false)
        {
            AccountFQDN = accnt.Trim();
            if (!skipAccntValidation && !IsValidAccount(AccountFQDN))
            {
                throw new ArgumentException("Account name is invalid");
            }
            ClientId = clientId;
            AccessToken = token;
            if (ClientLogger.IsTraceEnabled)
            {
                ClientLogger.Trace($"AdlsStoreClient, {ClientId} created for account {AccountFQDN} for SDK version {SdkVersion}");
            }
        }

        private AdlsClient(string accnt, long clientId, ServiceClientCredentials creds, bool skipAccntValidation = false)
        {
            AccountFQDN = accnt.Trim();
            if (!skipAccntValidation && !IsValidAccount(AccountFQDN))
            {
                throw new ArgumentException("Account name is invalid");
            }
            ClientId = clientId;
            AccessProvider = creds;
        }
        private bool IsValidAccount(string accnt)
        {
            return Regex.IsMatch(accnt, @"^[a-zA-Z0-9]+\.azuredatalakestore\.net$");
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
        /// Factory method that returns a AdlsClient
        /// </summary>
        /// <param name="accountFqdn">Azure data lake store account name including full domain name (e.g. contoso.azuredatalakestore.net)</param>
        /// <param name="token">Full authorization Token e.g. Bearing: abcddsfere.....</param>
        /// <returns>AdlsClient</returns>
        public static AdlsClient CreateClient(string accountFqdn, string token)
        {
            return new AdlsClient(accountFqdn, Interlocked.Increment(ref _atomicClientId), token);
        }
        /// <summary>
        /// Factory method that returns a AdlsClient
        /// </summary>
        /// <param name="accountFqdn">Azure data lake store account name including full domain name  (e.g. contoso.azuredatalakestore.net)</param>
        /// <param name="creds">Credentials that retrieves the Auth token</param>
        /// <returns>AdlsClient</returns>
        public static AdlsClient CreateClient(string accountFqdn, ServiceClientCredentials creds)
        {
            return new AdlsClient(accountFqdn, Interlocked.Increment(ref _atomicClientId), creds);
        }
        #endregion

        #region Thread Safe Getter Setters
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
                HttpRequestMessage request = new HttpRequestMessage();
                await AccessProvider.ProcessHttpRequestAsync(request, cancelToken);
                return request.Headers.Authorization.ToString();
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
        #endregion

        #region REST API 
        /// <summary>
        /// Asynchronous API to create a directory
        /// </summary>
        /// <param name="dirName">Name of directory</param>
        /// <param name="octalPermission">Octal permission</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>true if it creates the directory else false</returns>
        public async Task<bool> CreateDirectoryAsync(string dirName, string octalPermission = null, CancellationToken cancelToken = default(CancellationToken))
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
                new RequestOptions(new ExponentialRetryPolicy()), resp, cancelToken);
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
        public async Task<AdlsInputStream> GetReadStreamAsync(string filename, CancellationToken cancelToken = default(CancellationToken))
        {
            return await GetReadStreamAsync(filename, AdlsInputStream.DefaultBufferCapacity, cancelToken);
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
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <param name="bufferCapacity"> Buffer Capacity</param>
        /// <returns>Input stream</returns>
        public async Task<AdlsInputStream> GetReadStreamAsync(string filename, int bufferCapacity, CancellationToken cancelToken = default(CancellationToken))
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
            DirectoryEntry diren = await Core
                .GetFileStatusAsync(filename, UserGroupRepresentation.ObjectID, this,
                    new RequestOptions(new ExponentialRetryPolicy()), resp, cancelToken);
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
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <param name="bufferCapacity"> Buffer Capacity</param>
        /// <returns>Input stream</returns>
        public virtual AdlsInputStream GetReadStream(string filename, int bufferCapacity, CancellationToken cancelToken = default(CancellationToken))
        {
            return GetReadStreamAsync(filename, bufferCapacity, cancelToken).GetAwaiter().GetResult();
        }
        /// <summary>
        /// Asynchronous API that returns the stream to write data to a file in ADLS
        /// </summary>
        /// <param name="filename">File name</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>Output stream</returns>
        public async Task<AdlsOutputStream> GetAppendStreamAsync(string filename, CancellationToken cancelToken = default(CancellationToken))
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
                new RequestOptions(new ExponentialRetryPolicy()), resp, cancelToken);
            if (!resp.IsSuccessful)
            {
                throw GetExceptionFromResponse(resp, $"Error trying to append to file {filename}.");
            }
            return await AdlsOutputStream.GetAdlsOutputStreamAsync(filename, this, false, leaseId);
        }
        /// <summary>
        /// Synchronous API that returns the stream to write data to a file in ADLS
        /// </summary>
        /// <param name="filename">File name</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>Output stream</returns>
        public virtual AdlsOutputStream GetAppendStream(string filename, CancellationToken cancelToken = default(CancellationToken))
        {
            return GetAppendStreamAsync(filename, cancelToken).GetAwaiter().GetResult();
        }
        /// <summary>
        /// Asynchronous API that creates a file and returns the stream to write data to that file in ADLS
        /// </summary>
        /// <param name="filename">File name</param>
        /// <param name="mode">Overwrites the existing file if the mode is Overwrite</param>
        /// <param name="octalPermission">Octal permission string, can be null</param>
        /// <param name="createParent">If true creates any non-existing parent directories</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>Output stream</returns>
        public async Task<AdlsOutputStream> CreateFileAsync(string filename, IfExists mode, string octalPermission = null, bool createParent = true, CancellationToken cancelToken = default(CancellationToken))
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
            RetryPolicy policy;
            bool overwrite = mode == IfExists.Overwrite;
            //If we are overwriting any existing file by that name then it doesn't matter to try it again even though the last request is in a inconsistent state
            if (overwrite)
            {
                policy = new ExponentialRetryPolicy();
            }
            else
            {
                policy = new NoRetryPolicy();
            }
            OperationResponse resp = new OperationResponse();
            await Core.CreateAsync(filename, overwrite, octalPermission, leaseId, leaseId, createParent, SyncFlag.DATA, null, -1, 0, this, new RequestOptions(policy), resp, cancelToken);
            if (!resp.IsSuccessful)
            {
                throw GetExceptionFromResponse(resp, $"Error in creating file {filename}.");
            }
            return await AdlsOutputStream.GetAdlsOutputStreamAsync(filename, this, true, leaseId);
        }

        /// <summary>
        /// Synchronous API that creates a file and returns the stream to write data to that file in ADLS
        /// </summary>
        /// <param name="filename">File name</param>
        /// <param name="mode">Overwrites the existing file if the mode is Overwrite</param>
        /// <param name="octalPermission">Octal permission string</param>
        /// <param name="createParent">If true creates any non-existing parent directories</param>
        /// <returns>Output stream</returns>
        public virtual AdlsOutputStream CreateFile(string filename, IfExists mode, string octalPermission = null, bool createParent = true)
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
            RetryPolicy policy;
            bool overwrite = mode == IfExists.Overwrite;
            //If we are overwriting any existing file by that name then it doesn't matter to try it again even though the last request is in a inconsistent state
            if (overwrite)
            {
                policy = new ExponentialRetryPolicy();
            }
            else
            {
                policy = new NoRetryPolicy();
            }
            OperationResponse resp = new OperationResponse();
            Core.Create(filename, overwrite, octalPermission, leaseId, leaseId, createParent, SyncFlag.DATA, null, -1, 0, this, new RequestOptions(policy), resp);
            if (!resp.IsSuccessful)
            {
                throw GetExceptionFromResponse(resp, $"Error in creating file {filename}.");
            }
            return AdlsOutputStream.GetAdlsOutputStreamAsync(filename, this, true, leaseId).GetAwaiter().GetResult();
        }
        /// <summary>
        /// Asynchronous api to delete a file or directory. For directory it will only delete if it is empty.
        /// </summary>
        /// <param name="path">Path of file or directory</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>True if the path is deleted successfully else false</returns>
        public async Task<bool> DeleteAsync(string path, CancellationToken cancelToken = default(CancellationToken))
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
            bool isSucceded = await Core.DeleteAsync(path, false, this, new RequestOptions(new ExponentialRetryPolicy()),
                resp, cancelToken);
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
        public async Task<bool> DeleteRecursiveAsync(string path, CancellationToken cancelToken = default(CancellationToken))
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
            bool isSucceded = await Core.DeleteAsync(path, true, this, new RequestOptions(new ExponentialRetryPolicy()),
                resp, cancelToken);
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
        public async Task<bool> RenameAsync(string path, string destination, bool overwrite = false, CancellationToken cancelToken = default(CancellationToken))
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
                DirectoryEntry diren = await GetDirectoryEntryAsync(path, UserGroupRepresentation.ObjectID, cancelToken);
                if (diren.Type != DirectoryEntryType.FILE)
                {
                    throw new ArgumentException("Cannot rename directories same name");
                }
            }
            OperationResponse resp = new OperationResponse();
            bool isSucceeded = await Core.RenameAsync(path, destination, overwrite, this, new RequestOptions(new ExponentialRetryPolicy()),
                resp, cancelToken);
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
        public async Task<DirectoryEntry> GetDirectoryEntryAsync(string path, UserGroupRepresentation uid = UserGroupRepresentation.ObjectID, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            OperationResponse resp = new OperationResponse();
            DirectoryEntry diren = await Core.GetFileStatusAsync(path, uid, this,
                new RequestOptions(new ExponentialRetryPolicy()), resp, cancelToken);
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
        public async Task ConcatenateFilesAsync(string destination, List<string> concatFiles, bool deleteSource = false, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(destination))
            {
                throw new ArgumentException("Path is null");
            }
            OperationResponse resp = new OperationResponse();
            await Core.ConcatAsync(destination, concatFiles, deleteSource, this,
                new RequestOptions(new ExponentialRetryPolicy()), resp, cancelToken);
            if (!resp.IsSuccessful)
            {
                throw GetExceptionFromResponse(resp, $"Error in concating files {String.Join(",", concatFiles)} to destination {destination}");
            }
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
        /// By default listAfter and listBefore is empty and we enuerate all the directory entries.
        /// </summary>
        /// <param name="path">Path of the directory</param>
        /// <param name="userIdFormat">Way the user or group object will be represented</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>Enumerable that enumerates over the contents</returns>
        public virtual IEnumerable<DirectoryEntry> EnumerateDirectory(string path, UserGroupRepresentation userIdFormat = UserGroupRepresentation.ObjectID, CancellationToken cancelToken = default(CancellationToken))
        {
            return EnumerateDirectory(path, -1, "", "", userIdFormat, cancelToken);
        }
        /// <summary>
        /// Returns a enumerable that enumerates the sub-directories or files contained in a directory
        /// </summary>
        /// <param name="path">Path of the directory</param>
        /// <param name="maxEntries">List size to obtain from server</param>
        /// <param name="listAfter">Filename after which list of files should be obtained from server</param>
        /// <param name="listBefore">Filename till which list of files should be obtained from server</param>
        /// <param name="userIdFormat">Way the user or group object will be represented</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>Enumerable that enumerates over the contents</returns>
        internal IEnumerable<DirectoryEntry> EnumerateDirectory(string path, int maxEntries, string listAfter, string listBefore, UserGroupRepresentation userIdFormat = UserGroupRepresentation.ObjectID, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            return new FileStatusOutput(listBefore, listAfter, maxEntries, userIdFormat, this, path);
        }
        /// <summary>
        /// Asynchronously sets the expiry time
        /// </summary>
        /// <param name="path">Path of the file</param>
        /// <param name="eopt">Different type of expiry method for example: never expire, relative to now, etc that defines how to evaluate expiryTime</param>
        /// <param name="expiryTime">Expiry time value. It's interpretation depends on what ExpiryOption user passes</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public async Task SetExpiryTimeAsync(string path, ExpiryOption eopt, long expiryTime, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            OperationResponse resp = new OperationResponse();
            await Core.SetExpiryTimeAsync(path, eopt, expiryTime, this, new RequestOptions(new ExponentialRetryPolicy()),
                resp, cancelToken);
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
            SetExpiryTimeAsync(path, eopt, expiryTime,
                 cancelToken).GetAwaiter().GetResult();
        }
        /// <summary>
        /// Asynchronously checks if the user/group has specified access of the given path
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="rwx">Permission to check in "rwx" string form. For example if the user wants to see if it has read, execute permission, the string would be r-x </param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>True if client has access to the path else false</returns>
        public async Task<bool> CheckAccessAsync(string path, string rwx, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            OperationResponse resp = new OperationResponse();
            await Core.CheckAccessSync(path, rwx, this, new RequestOptions(new ExponentialRetryPolicy()), resp, cancelToken);
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
        public bool CheckAccess(string path, string rwx,
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
        public async Task SetPermissionAsync(string path, string permission, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            OperationResponse resp = new OperationResponse();
            await Core.SetPermissionAsync(path, permission, this, new RequestOptions(new ExponentialRetryPolicy()),
                resp, cancelToken);
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
        public async Task ModifyAclEntriesAsync(string path, List<AclEntry> aclSpec, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            OperationResponse resp = new OperationResponse();
            await Core.ModifyAclEntriesAsync(path, aclSpec, this, new RequestOptions(new ExponentialRetryPolicy()),
                resp, cancelToken);
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
        public async Task SetAclAsync(string path, List<AclEntry> aclSpec, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            OperationResponse resp = new OperationResponse();
            await Core.SetAclAsync(path, aclSpec, this, new RequestOptions(new ExponentialRetryPolicy()),
                resp, cancelToken);
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
        public async Task SetOwnerAsync(string path, string owner, string group, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            OperationResponse resp = new OperationResponse();
            await Core.SetOwnerAsync(path, owner, group, this, new RequestOptions(new ExponentialRetryPolicy()), resp, cancelToken);
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
        public async Task RemoveAclEntriesAsync(string path, List<AclEntry> aclSpec, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            OperationResponse resp = new OperationResponse();
            await Core.RemoveAclEntriesAsync(path, aclSpec, this, new RequestOptions(new ExponentialRetryPolicy()),
                resp, cancelToken);
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
        public async Task RemoveAllAclsAsync(string path, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            OperationResponse resp = new OperationResponse();
            await Core.RemoveAclAsync(path, this, new RequestOptions(new ExponentialRetryPolicy()),
                resp, cancelToken);
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
        public async Task RemoveDefaultAclsAsync(string path, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            OperationResponse resp = new OperationResponse();
            await Core.RemoveDefaultAclAsync(path, this, new RequestOptions(new ExponentialRetryPolicy()),
                resp, cancelToken);
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
        public async Task<AclStatus> GetAclStatusAsync(string path, UserGroupRepresentation userIdFormat = UserGroupRepresentation.ObjectID, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            OperationResponse resp = new OperationResponse();
            AclStatus status = await Core.GetAclStatusAsync(path, userIdFormat, this, new RequestOptions(new ExponentialRetryPolicy()),
                resp, cancelToken);
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
        /// <summary>
        /// Gets content summary of a file or directory
        /// </summary>
        /// <param name="path">Path of the directory or file</param>
        /// <param name="numThreads">Number of threads</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public ContentSummary GetContentSummary(string path, int numThreads = -1, CancellationToken cancelToken = default(CancellationToken))
        {
            return ContentProcessor.GetContentSummary(this, path, numThreads, cancelToken);
        }

        /// <summary>
        /// Asynchronous API to perform concurrent append at server. The offset at which append will occur is determined by server. Asynchronous operation.
        /// </summary>
        /// <param name="path">Path of the file</param>
        /// <param name="autoCreate"></param>
        /// <param name="dataBytes">Array of bytes to write to the file</param>
        /// <param name="offset">Offset in the byte array</param>
        /// <param name="length">Number of bytes to write from the offset</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public async Task ConcurrentAppendAsync(string path, bool autoCreate, byte[] dataBytes, int offset,
            int length, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null or empty");
            }
            OperationResponse resp = new OperationResponse();
            await Core.ConcurrentAppendAsync(path, autoCreate, dataBytes, offset, length, this, new RequestOptions(),
                resp, cancelToken);
            if (!resp.IsSuccessful)
            {
                throw GetExceptionFromResponse(resp, $"Error in concurrent append for file {path}.");
            }
        }
        /// <summary>
        /// Synchronous API to perform concurrent append at server. The offset at which append will occur is determined by server.
        /// </summary>
        /// <param name="path">Path of the file</param>
        /// <param name="autoCreate"></param>
        /// <param name="dataBytes">Array of bytes to write to the file</param>
        /// <param name="offset">Offset in the byte array</param>
        /// <param name="length">Number of bytes to write from the offset</param>
        public void ConcurrentAppend(string path, bool autoCreate, byte[] dataBytes, int offset, int length)
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null or empty");
            }
            OperationResponse resp = new OperationResponse();
            Core.ConcurrentAppend(path, autoCreate, dataBytes, offset, length, this, new RequestOptions(), resp);
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
        public bool CheckExists(string path)
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
        /// <summary>
        /// Upload directory or file from local to remote. Transfers the contents under source directory under 
        /// the destination directory. Transfers the source file and saves it as the destination path.
        /// </summary>
        /// <param name="srcPath">Local source path</param>
        /// <param name="destPath">Remote destination path - It should always be a directory.</param>
        /// <param name="numThreads">Number of threads- if not passed will take default number of threads (8 times the number of physical cores)</param>
        /// <param name="shouldOverwrite">Whether to overwrite or skip if the destination exists</param>
        /// <param name="progressTracker">Progresstracker to track progress of file transfer</param>
        /// <param name="notRecurse">If true then does an enumeration till level one else does recursive enumeration</param>
        /// <param name="resume">If true then we want to resume from last transfer</param>
        /// <param name="isBinary">If false then writes files to data lake at newline boundaries. If true, then this is not guranteed but the upload will be faster.</param>
        /// <param name="cancelToken">Cancellation token</param>
        /// <returns>Transfer Status encapsulating the details of upload</returns>
        public virtual TransferStatus BulkUpload(string srcPath, string destPath, int numThreads = -1, IfExists shouldOverwrite = IfExists.Overwrite, IProgress<TransferStatus> progressTracker = null, bool notRecurse = false, bool resume = false, bool isBinary = false, CancellationToken cancelToken = default(CancellationToken))
        {
            return FileUploader.Upload(srcPath, destPath, this, numThreads, shouldOverwrite, progressTracker, notRecurse, resume, isBinary, cancelToken);
        }
        /// <summary>
        /// Download directory or file from remote server to local. Transfers the contents under source directory under 
        /// the destination directory. Transfers the source file and saves it as the destination path.
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
            return FileDownloader.Download(srcPath, destPath, this, numThreads, shouldOverwrite, progressTracker, notRecurse, resume, cancelToken);
        }
        /// <summary>
        /// Change Acl (Modify, set and remove) recursively on a directory tree
        /// </summary>
        /// <param name="path">The root directory path from where the Acl change will begin</param>
        /// <param name="aclEntries">Acl entries to add or set or remove depending on the input</param>
        /// <param name="type">Type of modification <see cref="RequestedAclType"/></param>
        /// <param name="threadCount">Number of threads to use</param>
        /// <returns>Stats- total number of files and directories processed</returns>
        public AclProcessorStats ChangeAcl(string path, List<AclEntry> aclEntries, RequestedAclType type, int threadCount = -1)
        {
            return AclProcessor.RunAclProcessor(path, this, aclEntries, type, threadCount);
        }
        /// <summary>
        /// Recursively dumps file property of alldirectories or/and files under the given path to a local or adl file. File property can be disk usage or Acl or both.
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="getAclUsage">True if we want Acl usage</param>
        /// <param name="dumpFileName">Filename containing the ACL or Disk usage dump</param>
        /// <param name="getDiskUsage">True if we want disk usage</param>
        /// <param name="saveToLocal">True if we want to save to local file else save to ADL</param>
        /// <param name="numThreads">Number of threads</param>
        /// <param name="displayFiles">True if we want to display properties of files. By default we show properties of directories</param>
        /// <param name="displayConsistentAcl">if True then we wont dump the acl property of a directory/file if it's parent directory has same acl for all of its descendants. For ex: If 
        ///                                     the root ("/") has same Acl for all it's descendants, then we will show the Acl for the root only. If this flag is false, then we show the Acl for all directories or files</param>
        /// <param name="maxDepth">Maximum depth till which we want to view the properties</param>
        public void GetFileProperties(string path, bool getAclUsage, string dumpFileName, bool getDiskUsage = true, bool saveToLocal = true, int numThreads = -1, bool displayFiles = false, bool displayConsistentAcl = true, long maxDepth = Int64.MaxValue)
        {
            if (!(getAclUsage || getDiskUsage))
            {
                throw new ArgumentException("At least one option of getAclUsage and getDiskUsage need to be set as true.");
            }
            PropertyManager.GetFileProperty(path, this, getAclUsage, getDiskUsage, dumpFileName, saveToLocal, numThreads, displayFiles, displayConsistentAcl, maxDepth);
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
            exceptionMessage += $"\nOperation: {resp.OpCode} failed with {(resp.HttpStatus > 0 ? "HttpStatus:" + resp.HttpStatus : "")} ";

            if (!string.IsNullOrEmpty(resp.Error))
            {
                exceptionMessage += "Error: " + resp.Error + ".";
            }
            else if (!string.IsNullOrEmpty(resp.RemoteExceptionName))
            {
                exceptionMessage += $"RemoteException: {resp.RemoteExceptionName} {resp.RemoteExceptionMessage} JavaClassName: {resp.RemoteExceptionJavaClassName}.";
            }
            else
            {
                exceptionMessage += $"Unknown Error: {resp.Ex.Message}.";
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
