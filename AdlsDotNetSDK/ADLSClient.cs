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
    /// a async and sync version. Every sync method is a wait on async method with exception of Create and Concurrent append.
    /// </summary>
    public class AdlsClient
    {
        #region Properties
        /// <summary>
        /// Logger to log information (debug/error/trace) regarding client
        /// </summary>
        private static readonly Logger ClientLogger = LogManager.GetLogger("asdl.dotnet");
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
        public string AccountDomain { get; }
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
                ManagementObjectSearcher searcher = new ManagementObjectSearcher("SELECT Caption, Version, OSArchitecture FROM Win32_OperatingSystem");
                foreach (var os in searcher.Get())
                {
                    var version = os["Version"].ToString();
                    var productName = os["Caption"].ToString();
                    var architecture = os["OSArchitecture"].ToString();
                    osInfo = productName + " " + version + " " + architecture;
                }
                dotNetVersion = "NET452";
#else
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
        private AdlsClient(string accnt, long clientId, string token, bool skipAccntValidation = false)
        {
            AccountDomain = accnt.Trim();
            if (!skipAccntValidation && !IsValidAccount(AccountDomain))
            {
                throw new ArgumentException("Account name is invalid");
            }
            ClientId = clientId;
            AccessToken = token;
            if (ClientLogger.IsTraceEnabled)
            {
                ClientLogger.Trace($"AdlsStoreClient {ClientId} created for account {AccountDomain} for SDK version {SdkVersion}");
            }
        }

        private AdlsClient(string accnt, long clientId, ServiceClientCredentials creds, bool skipAccntValidation = false)
        {
            AccountDomain = accnt.Trim();
            if (!skipAccntValidation && !IsValidAccount(AccountDomain))
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
        /// <param name="accnt">Azure data lake store account name including full domain name (e.g. contoso.azuredatalake.net)</param>
        /// <param name="token">Token</param>
        /// <returns>AdlsClient</returns>
        public static AdlsClient CreateClient(string accnt, string token)
        {
            return new AdlsClient(accnt, Interlocked.Increment(ref _atomicClientId), token);
        }
        /// <summary>
        /// Factory method that returns a AdlsClient
        /// </summary>
        /// <param name="accnt">Azure data lake store account name including full domain name  (e.g. contoso.azuredatalake.net)</param>
        /// <param name="creds">Credentials that gets the Auth token</param>
        /// <returns>AdlsClient</returns>
        public static AdlsClient CreateClient(string accnt, ServiceClientCredentials creds)
        {
            return new AdlsClient(accnt, Interlocked.Increment(ref _atomicClientId), creds);
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
        public bool CreateDirectory(string dirName, string octalPermission = null, CancellationToken cancelToken = default(CancellationToken))
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
        public AdlsInputStream GetReadStream(string filename, CancellationToken cancelToken = default(CancellationToken))
        {
            return GetReadStreamAsync(filename, cancelToken).GetAwaiter().GetResult();
        }/// <summary>
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
                .GetFileStatusAsync(filename, UserGroupRepresentation.OID, this,
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
        public AdlsInputStream GetReadStream(string filename, int bufferCapacity, CancellationToken cancelToken = default(CancellationToken))
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
            return await AdlsOutputStream.GetAdlsOutputStream(filename, this, false, leaseId);
        }
        /// <summary>
        /// Synchronous API that returns the stream to write data to a file in ADLS
        /// </summary>
        /// <param name="filename">File name</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>Output stream</returns>
        public AdlsOutputStream GetAppendStream(string filename, CancellationToken cancelToken = default(CancellationToken))
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
                ClientLogger.Trace($"Create File {filename} for client {ClientId}");
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
            return await AdlsOutputStream.GetAdlsOutputStream(filename, this, true, leaseId);
        }

        /// <summary>
        /// Synchronous API that creates a file and returns the stream to write data to that file in ADLS
        /// </summary>
        /// <param name="filename">File name</param>
        /// <param name="mode">Overwrites the existing file if the mode is Overwrite</param>
        /// <param name="octalPermission">Octal permission string</param>
        /// <param name="createParent">If true creates any non-existing parent directories</param>
        /// <returns>Output stream</returns>
        public AdlsOutputStream CreateFile(string filename, IfExists mode, string octalPermission = null, bool createParent = true)
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
                ClientLogger.Trace($"Create File {filename} for client {ClientId}");
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
            return AdlsOutputStream.GetAdlsOutputStream(filename, this, true, leaseId).GetAwaiter().GetResult();
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
        public bool Delete(string path, CancellationToken cancelToken = default(CancellationToken))
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
        /// Synchronous api to delete a file or directory recursively
        /// </summary>
        /// <param name="path">Path of file or directory</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>True if the path is deleted successfully else false</returns>
        public bool DeleteRecursive(string path, CancellationToken cancelToken = default(CancellationToken))
        {
            return DeleteRecursiveAsync(path, cancelToken).GetAwaiter().GetResult();
        }
        /// <summary>
        /// Asynchronous API to renames a path.
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
                throw new ArgumentException("Cant rename the root");
            }
            if (string.IsNullOrEmpty(destination))
            {
                throw new ArgumentException("Destination is null");
            }
            if (path.Equals(destination))
            {
                DirectoryEntry diren = await GetDirectoryEntryAsync(path, UserGroupRepresentation.OID, cancelToken);
                if (diren.Type != DirectoryEntryType.FILE)
                {
                    throw new ArgumentException("Cant rename directories same name");
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
        /// Synchronous API to renames a path.
        /// For renaming directory: If the destination exists then it puts the source directory one level under the destination.
        /// </summary>
        /// <param name="path">Path of the source file or directory</param>
        /// <param name="destination">Destination path</param>
        /// <param name="overwrite">For file: If true then overwrites the destination file if it exists 
        ///                         For directory: If the destination directory exists, then this flag has no use. Because it puts the source one level under destination.
        ///                                        If there is a subdirectory with same name as source one level under the destination path, this flag has no use. Rename fails  </param>
        ///                         By default it is false
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>True if the path is renamed successfully else false</returns>
        public bool Rename(string path, string destination, bool overwrite = false, CancellationToken cancelToken = default(CancellationToken))
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
        public async Task<DirectoryEntry> GetDirectoryEntryAsync(string path, UserGroupRepresentation uid = UserGroupRepresentation.OID, CancellationToken cancelToken = default(CancellationToken))
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
        /// <param name="uid">Way the user or group object will be represented</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>Returns the metadata of the file or directory</returns>
        public DirectoryEntry GetDirectoryEntry(string path, UserGroupRepresentation uid = UserGroupRepresentation.OID, CancellationToken cancelToken = default(CancellationToken))
        {
            return GetDirectoryEntryAsync(path, uid, cancelToken).GetAwaiter().GetResult();
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
        public void ConcatenateFiles(string destination, List<string> concatFiles, bool deleteSource = false,
            CancellationToken cancelToken = default(CancellationToken))
        {
            ConcatenateFilesAsync(destination, concatFiles, deleteSource, cancelToken).GetAwaiter().GetResult();
        }
        /// <summary>
        /// Returns a enumerable that enumerates the sub-directories or files contained in a directory.
        /// By default listAfter and listBefore is empty and we enuerate all the directory entries.
        /// </summary>
        /// <param name="path">Path of the directory</param>
        /// <param name="uid">Way the user or group object will be represented</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>Enumerable that enumerates over the contents</returns>
        public IEnumerable<DirectoryEntry> EnumerateDirectory(string path, UserGroupRepresentation uid = UserGroupRepresentation.OID, CancellationToken cancelToken = default(CancellationToken))
        {
            return EnumerateDirectory(path, -1, "", "", uid, cancelToken);
        }
        /// <summary>
        /// Returns a enumerable that enumerates the sub-directories or files contained in a directory
        /// </summary>
        /// <param name="path">Path of the directory</param>
        /// <param name="maxEntries">List size to obtain from server</param>
        /// <param name="listAfter">Filename after which list of files should be obtained from server</param>
        /// <param name="listBefore">Filename till which list of files should be obtained from server</param>
        /// <param name="uid">Way the user or group object will be represented</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        /// <returns>Enumerable that enumerates over the contents</returns>
        internal IEnumerable<DirectoryEntry> EnumerateDirectory(string path, int maxEntries, string listAfter, string listBefore, UserGroupRepresentation uid = UserGroupRepresentation.OID, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            return new FileStatusOutput(listBefore, listAfter, maxEntries, uid, this, path);
        }
        /// <summary>
        /// Asynchronously sets the expiry time
        /// </summary>
        /// <param name="path">Path of the file</param>
        /// <param name="eopt">Different type of expiry method for example: never expire, relative to now, etc that defines how to evaluate expiryTime</param>
        /// <param name="expiryTime">Expiry time value. It's interepreation depends on what ExpiryOption user passes</param>
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
        /// <param name="expiryTime">Expiry time value. It's interepreation depends on what ExpiryOption user passes</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public void SetExpiryTime(string path, ExpiryOption eopt, long expiryTime,
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
        public void SetPermission(string path, string permission,
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
        public void ModifyAclEntries(string path, List<AclEntry> aclSpec,
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
        public void SetAcl(string path, List<AclEntry> aclSpec,
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
        public void SetOwner(string path, string owner, string group, CancellationToken cancelToken = default(CancellationToken))
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
        public void RemoveAclEntries(string path, List<AclEntry> aclSpec,
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
        public void RemoveAllAcls(string path, CancellationToken cancelToken = default(CancellationToken))
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
        public void RemoveDefaultAcls(string path, CancellationToken cancelToken = default(CancellationToken))
        {
            RemoveDefaultAclsAsync(path, cancelToken).GetAwaiter().GetResult();
        }
        /// <summary>
        /// Asynchronously gets the ACL entry list, owner ID, group ID, octal permission and sticky bit (only for a directory) of the file/directory
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="ugr">way to represent the user/group object</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public async Task<AclStatus> GetAclStatusAsync(string path, UserGroupRepresentation ugr = UserGroupRepresentation.OID, CancellationToken cancelToken = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ArgumentException("Path is null");
            }
            OperationResponse resp = new OperationResponse();
            AclStatus status = await Core.GetAclStatusAsync(path, ugr, this, new RequestOptions(new ExponentialRetryPolicy()),
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
        /// <param name="ugr">way to represent the user/group object</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public AclStatus GetAclStatus(string path, UserGroupRepresentation ugr = UserGroupRepresentation.OID, CancellationToken cancelToken = default(CancellationToken))
        {
            return GetAclStatusAsync(path, ugr, cancelToken).GetAwaiter().GetResult();
        }
        /// <summary>
        /// Gets contentSummary of a file or directory
        /// </summary>
        /// <param name="path">Path of the directory or file</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public ContentSummary GetContentSummary(string path, CancellationToken cancelToken = default(CancellationToken))
        {
            return ContentProcessor.GetContentSummary(this, path, cancelToken);
        }

        /// <summary>
        /// Performs concurrent append at server. The offset at which append will occur is determined by server. Asynchronous operation.
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
        /// Performs concurrent append at server. The offset at which append will occur is determined by server. Synchronous operation
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
        #endregion
        /// <summary>
        /// Returns a ADLS Exception based on response from the server
        /// </summary>
        /// <param name="resp">Response encapsulating errors or exceptions</param>
        /// <param name="defaultMessage">Default message</param>
        /// <returns></returns>
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
