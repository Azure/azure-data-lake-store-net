using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.DataLake.Store.Acl;
using Microsoft.Azure.DataLake.Store.AclTools;
using Microsoft.Azure.DataLake.Store.FileTransfer;

namespace Microsoft.Azure.DataLake.Store.MockAdlsFileSystem
{
    internal struct DirectoryEntryMetaData
    {
        internal MemoryStream DataStream;
        internal DirectoryEntry Entry;
        internal AclStatus AclData;
        internal DateTime CreationTime;
    }

    /// <summary>
    /// Mock Adls Client. All the operations are done in memory. This is not a accurate immplementation of actual adlsclient. The immplementations are best effort only.
    /// </summary>
    public sealed class MockAdlsClient : AdlsClient
    {
        private readonly IDictionary<string, DirectoryEntryMetaData> _directoryEntries;
        private readonly IDictionary<string, DirectoryEntryMetaData> _trashDirectoryEntries;  // contains trashPath->deletedEntry mapping
        
        private static readonly string Owner = Guid.NewGuid().ToString();
        private static readonly string Group = Guid.NewGuid().ToString();

        private static string accountName;
        private static Random random = new Random();

        private MockAdlsClient(string accountNm) : base(accountNm, 1)
        {
            _directoryEntries = new ConcurrentDictionary<string, DirectoryEntryMetaData>();
            _trashDirectoryEntries = new ConcurrentDictionary<string, DirectoryEntryMetaData>();
            // The root directory is there
            CreateDirectory("/");
            ModifyAclEntries("/",new List<AclEntry>(){new AclEntry(AclType.user,Owner,AclScope.Access, AclAction.All)});

        }
        /// <summary>
        /// Factory method that returns an instance of Mock adls client
        /// </summary>
        /// <returns>Mock ADls Client</returns>
        public static MockAdlsClient GetMockClient()
        {
            accountName = "test.azuredatalakestore.net";
            return new MockAdlsClient(accountName);
        }

        /// <summary>
        /// Factory method that returns an instance of Mock adls client
        /// </summary>
        /// <returns>Mock ADls Client</returns>
        public static MockAdlsClient GetMockClient(string account)
        {
            return new MockAdlsClient(account);
        }

        // Creates an entry for a new file or directory. 
        private DirectoryEntryMetaData CreateMetaData(string entryName,DirectoryEntryType type,string octalPermission)
        {
            var entry = new DirectoryEntry(entryName)
            {
                Permission = octalPermission ?? "770",
                LastAccessTime = DateTime.UtcNow,
                LastModifiedTime = DateTime.UtcNow,
                Type = type,
                ExpiryTime = null,
                User = Owner,
                Group= Group
            };
            var aclStatus = new AclStatus(new List<AclEntry>(), Owner, Group, octalPermission ?? "770", false);
            var metaData = new DirectoryEntryMetaData()
            {
                DataStream = type== DirectoryEntryType.FILE?new MemoryStream():null,
                Entry = entry,
                AclData = aclStatus,
                CreationTime = DateTime.UtcNow
            };
            return metaData;
        }

        /// <summary>
        /// Creates a directory- Creates an entry for the directory in the internal dictionary
        /// </summary>
        /// <param name="dirName">Directory name</param>
        /// <param name="octalPermission">Octal permission</param>
        /// <param name="cancelToken">Cacnellation token</param>
        public override async Task<bool> CreateDirectoryAsync(string dirName, string octalPermission = null,
            CancellationToken cancelToken = default(CancellationToken))
        {
            await Task.Run(() =>
                _directoryEntries.Add(dirName, CreateMetaData(dirName, DirectoryEntryType.DIRECTORY, octalPermission)),cancelToken);
            return true;
        }

        /// <summary>
        /// Creates a directory- Creates an entry for the directory in the internal dictionary
        /// </summary>
        /// <param name="dirName">Directory name</param>
        /// <param name="octalPermission">Octal permission</param>
        /// <param name="cancelToken">Cacnellation token</param>
        /// <returns></returns>
        public override bool CreateDirectory(string dirName, string octalPermission = null,
            CancellationToken cancelToken = default(CancellationToken))
        {
            return CreateDirectoryAsync(dirName, octalPermission, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Returns a memory stream for reading data of the file
        /// </summary>
        /// <param name="filename">File name</param>
        /// <param name="cancelToken">Cancellation token</param>
        /// <returns></returns>
        public override async Task<AdlsInputStream> GetReadStreamAsync(string filename,
            CancellationToken cancelToken = default(CancellationToken))
        {
            return await Task.Run(() =>
            {
                if (!_directoryEntries.ContainsKey(filename))
                {
                    throw new AdlsException("The file does not exists");
                }

                return new MockAdlsInputStream(_directoryEntries[filename].DataStream);
            }, cancelToken);
        }

        /// <summary>
        /// Returns a memory stream for reading data of the file
        /// </summary>
        /// <param name="filename">File name</param>
        /// <param name="cancelToken">Cancellation token</param>
        /// <returns></returns>
        public override AdlsInputStream GetReadStream(string filename, CancellationToken cancelToken = default(CancellationToken))
        {
            return GetReadStreamAsync(filename, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Returns the memory stream for appending to the file encapsulated in mock adls output stream.
        /// </summary>
        /// <param name="filename">File name</param>
        /// <param name="cancelToken">Cancellation token</param>
        /// <returns></returns>
        public override async Task<AdlsOutputStream> GetAppendStreamAsync(string filename,
            CancellationToken cancelToken = default(CancellationToken))
        {
            return await Task.Run(() =>
            {
                if (!_directoryEntries.ContainsKey(filename))
                {
                    throw new AdlsException("The file does not exists");
                }

                _directoryEntries[filename].DataStream.Seek(_directoryEntries[filename].DataStream.Length,
                    SeekOrigin.Begin);
                return new MockAdlsOutputStream(_directoryEntries[filename].DataStream,
                    _directoryEntries[filename].Entry);
            }, cancelToken);
        }

        /// <summary>
        /// Returns the memory stream for appending to the file encapsulated in mock adls output stream.
        /// </summary>
        /// <param name="filename">File name</param>
        /// <param name="cancelToken">Cancellation token</param>
        /// <returns></returns>
        public override AdlsOutputStream GetAppendStream(string filename, CancellationToken cancelToken = default(CancellationToken))
        {
            return GetAppendStreamAsync(filename, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Creates an entry to the internal dictionary for the new file. The entry encapsulates AclStatus, DirectoryEntry and a memory stream
        /// </summary>
        /// <param name="filename">File name</param>
        /// <param name="mode">If exists hether to overwrite or fail</param>
        /// <param name="octalPermission">Permission string</param>
        /// <param name="createParent">True if we create parent directories- currently has no effect</param>
        /// <param name="cancelToken">Cancellation token</param>
        /// <returns>Mock ADls output stream</returns>
        public override async Task<AdlsOutputStream> CreateFileAsync(string filename, IfExists mode, string octalPermission = null,
            bool createParent = true, CancellationToken cancelToken = default(CancellationToken))
        {
            return await Task.Run(() =>
            {

                if (mode == IfExists.Fail && _directoryEntries.ContainsKey(filename))
                {
                    throw new AdlsException("The file exists");
                }
                if (mode == IfExists.Overwrite && _directoryEntries.ContainsKey(filename))
                {
                    _directoryEntries.Remove(filename);
                }
                var metaData = CreateMetaData(filename, DirectoryEntryType.FILE, octalPermission);
                _directoryEntries.Add(filename, metaData);
                return new MockAdlsOutputStream(metaData.DataStream, metaData.Entry);
            }, cancelToken);
        }

        /// <summary>
        /// Creates an entry to the internal dictionary for the new file. The entry encapsulates AclStatus, DirectoryEntry and a memory stream
        /// </summary>
        /// <param name="filename">File name</param>
        /// <param name="mode">If exists hether to overwrite or fail</param>
        /// <param name="octalPermission">Permission string</param>
        /// <param name="createParent">True if we create parent directories- currently has no effect</param>
        /// <returns>Mock ADls output stream</returns>
        public override AdlsOutputStream CreateFile(string filename, IfExists mode, string octalPermission = null,
            bool createParent = true)
        {
            return CreateFileAsync(filename, mode, octalPermission, createParent).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Delete an entry from the internal dictionary
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="cancelToken">Cancellation token</param>
        /// <returns></returns>
        public override async Task<bool> DeleteAsync(string path, CancellationToken cancelToken = default(CancellationToken))
        {
            return await Task.Run(() =>
            {
                foreach (var directoryEntriesKey in _directoryEntries.Keys)
                {
                    if (directoryEntriesKey.StartsWith(path + "/"))
                    {
                        return false;
                    }
                }

                bool ret = false;

                if(_directoryEntries.ContainsKey(path))
                {
                    var dirEntry = _directoryEntries[path];
                    ret = _directoryEntries.Remove(path);
                    var originalPath = dirEntry.Entry.FullName;
                    string trashPath = "/deleted/local/" + random.Next(10000000);

                    // Add it to the list of items in trash
                    lock (_trashDirectoryEntries)
                    {
                        _trashDirectoryEntries.Add(trashPath, dirEntry);
                    }
                }

                return ret;
            }, cancelToken);
        }

        /// <summary>
        /// Delete an entry from the internal dictionary
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="cancelToken">Cancellation token</param>
        /// <returns></returns>
        public override bool Delete(string path, CancellationToken cancelToken = default(CancellationToken))
        {
            return DeleteAsync(path, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Deletes all entries within a directory or delete a file
        /// </summary>
        /// <param name="path">Path of directory or file</param>
        /// <param name="cancelToken">Cancellation token</param>
        /// <returns></returns>
        public override async Task<bool> DeleteRecursiveAsync(string path, CancellationToken cancelToken = default(CancellationToken))
        {
            return await Task.Run(() =>
            {
                var keysToRemove = new List<string>();
                foreach (var directoryEntriesKey in _directoryEntries.Keys)
                {
                    if (directoryEntriesKey.StartsWith(path))
                    {
                        keysToRemove.Add(directoryEntriesKey);
                    }
                }

                foreach (var key in keysToRemove)
                {
                    if (!_directoryEntries.Remove(key))
                    {
                        return false;
                    }
                }

                return true;
            }, cancelToken);
        }

        /// <summary>
        /// Deletes all entries within a directory or delete a file
        /// </summary>
        /// <param name="path">Path of directory or file</param>
        /// <param name="cancelToken">Cancellation token</param>
        /// <returns></returns>
        public override bool DeleteRecursive(string path, CancellationToken cancelToken = default(CancellationToken))
        {
            return DeleteRecursiveAsync(path, cancelToken).GetAwaiter().GetResult();
        }
 
        /// <summary>
        /// Asynchronously gets the trash entries
        /// </summary>
        /// <param name="hint">String to match. Cannot be empty.</param>
        /// <param name="listAfter">Token returned by system in the previous API invocation</param>
        /// <param name="numResults">Search is executed until we find numResults or search completes. Maximum allowed value for this param is 4000. The number of returned entries could be more or less than numResults</param>
        /// <param name="progressTracker">Object to track progress of the task. Can be null</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public override async Task<IEnumerable<TrashEntry>> EnumerateDeletedItemsAsync(string hint, string listAfter, int numResults, IProgress<EnumerateDeletedItemsProgress> progressTracker, CancellationToken cancelToken)
        {
            return await Task.Run(() =>
            {
                List<TrashEntry> trashEntries = new List<TrashEntry>();
                var matchingPaths = new List<string>();
                int numSearched = 0;
                int numFound = 0;
                string nextListAfter = "";
                var enumerator = _trashDirectoryEntries.GetEnumerator();

                // Search after "listafter" element
                if (!string.IsNullOrEmpty(listAfter))
                {
                    while (enumerator.MoveNext())
                    {
                        if (enumerator.Current.Key.Equals(listAfter))
                        {
                            break;
                        }
                    }
                }

                // Start hint comparison now
                while (enumerator.MoveNext())
                {
                    numSearched++;
                    if (enumerator.Current.Value.Entry.FullName.Contains(hint))
                    {
                        numFound++;
                        matchingPaths.Add(enumerator.Current.Key);
                    }

                    if(numFound == numResults)
                    {
                        nextListAfter = enumerator.Current.Key;
                        break;
                    }
                }

                foreach(var trashPath in matchingPaths)
                {
                    var dirEntry = _trashDirectoryEntries[trashPath];
                    //convert to fsurls
                    TrashEntry trashEntry = new TrashEntry();
                    trashEntry.OriginalPath = "adl://" + accountName + dirEntry.Entry.FullName;
                    trashEntry.TrashDirPath = "adl://" + accountName + trashPath;
                    trashEntry.CreationTime = dirEntry.CreationTime;
                    trashEntry.Type = (dirEntry.Entry.Type == DirectoryEntryType.DIRECTORY ? TrashEntryType.DIRECTORY : TrashEntryType.FILE);
                    trashEntries.Add(trashEntry);
                }

                if (progressTracker != null)
                {
                    EnumerateDeletedItemsProgress progress = new EnumerateDeletedItemsProgress { NumFound = numFound, NumSearched = numSearched, NextListAfter = nextListAfter };
                    progressTracker.Report(progress);
                }

                return trashEntries;
            }, cancelToken);
        }

        /// <summary>
        /// Search trash under a account with hint and a starting point. This is a long running operation,
        /// and user is updated with progress periodically.
        /// </summary>
        /// <param name="hint">String to match</param>
        /// <param name="listAfter">Token returned by system in the previous API invocation</param>
        /// <param name="numResults">Search is executed until we find numResults or search completes. Maximum allowed value for this param is 4000. The number of returned entries could be more or less than numResults</param>
        /// <param name="progressTracker">Object to track progress of the task. Can be null</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public override IEnumerable<TrashEntry> EnumerateDeletedItems(string hint, string listAfter, int numResults, IProgress<EnumerateDeletedItemsProgress> progressTracker, CancellationToken cancelToken = default(CancellationToken))
        {
            return EnumerateDeletedItemsAsync(hint, listAfter, numResults, progressTracker, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Synchronously Restores trash entry
        /// </summary>
        /// <param name="pathOfFileToRestoreInTrash">Trash Directory path returned by enumeratedeleteditems</param>
        /// <param name="restoreDestination">Destination for restore</param>
        /// <param name="type">type of restore - file or directory</param>
        /// <param name="restoreAction">Action to take in case of destination conflict</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public override void RestoreDeletedItems(string pathOfFileToRestoreInTrash, string restoreDestination, string type, string restoreAction = "", CancellationToken cancelToken = default(CancellationToken))
        {
            RestoreDeletedItemsAsync(pathOfFileToRestoreInTrash, restoreDestination, type, restoreAction, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Asynchronously Restores trash entry
        /// </summary>
        /// <param name="pathOfFileToRestoreInTrash">Trash Directory path returned by enumeratedeleteditems</param>
        /// <param name="restoreDestination">Destination for restore</param>
        /// <param name="type">type of restore - file or directory</param>
        /// <param name="restoreAction">Action to take in case of destination conflict</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public override async Task RestoreDeletedItemsAsync(string pathOfFileToRestoreInTrash, string restoreDestination, string type, string restoreAction = "", CancellationToken cancelToken = default(CancellationToken))
        {
            await Task.Run(() =>
            {
                // convert FsUrls back to relative paths
                pathOfFileToRestoreInTrash = pathOfFileToRestoreInTrash.Substring(("adl://" + accountName).Length);
                restoreDestination = restoreDestination.Substring(("adl://" + accountName).Length);
 
                if (!_trashDirectoryEntries.Keys.Contains(pathOfFileToRestoreInTrash))
                {
                    throw new ArgumentException();
                }

                var enumerator = _trashDirectoryEntries.GetEnumerator();
                while(enumerator.MoveNext())
                {
                    if(enumerator.Current.Key.Equals(pathOfFileToRestoreInTrash))
                    {
                        var deletedEntry = _trashDirectoryEntries[pathOfFileToRestoreInTrash];
                        var entryType = deletedEntry.Entry.Type;

                        if(String.IsNullOrEmpty(type) || !(type.Equals("file") || type.Equals("folder")))
                        {
                            throw new ArgumentException();
                        }

                        if((type.Equals("file") ? DirectoryEntryType.FILE : DirectoryEntryType.DIRECTORY) != entryType)
                        {
                            throw new ArgumentException();
                        }

                        // Make sure there is no conflict
                        if (_directoryEntries.ContainsKey(restoreDestination))
                        {
                            if (String.Equals(restoreAction, "copy", StringComparison.OrdinalIgnoreCase))
                            {
                                restoreDestination += "_" + random.Next(10000000);
                            }
                            else if (String.Equals(restoreAction, "overwrite", StringComparison.OrdinalIgnoreCase)) // TODO: type is stream
                            {
                                if (String.Equals(type, "folder", StringComparison.OrdinalIgnoreCase))
                                {
                                    throw new ArgumentException();
                                }
                            }
                        }

                        // Start the restore;
                        deletedEntry.Entry.FullName = restoreDestination;
                        deletedEntry.Entry.Name = restoreDestination.Substring(restoreDestination.LastIndexOf("/") + 1); // TODO: fix name
                        
                        // Add it back
                        _directoryEntries.Add(restoreDestination, deletedEntry);

                        // Remove it from trash items
                        _trashDirectoryEntries.Remove(pathOfFileToRestoreInTrash);
                        break;
                    }
                }

                return;
            }, cancelToken);
        }

        private void MoveOneEntry(string src, string dest)
        {
            var metaData = _directoryEntries[src];
            metaData.Entry.Name = metaData.Entry.FullName = dest;
            _directoryEntries.Add(dest, metaData);
            _directoryEntries.Remove(src);
        }
        // Moves all files in the directory
        private void MoveDirectory(string srcPath, string destPath)
        {
            var list=new List<string>();
            foreach (var path in _directoryEntries.Keys)
            {
                if (path.StartsWith(srcPath+"/"))
                {
                    list.Add(path);
                }
            }
            foreach (var entry in list)
            {
                string destfileName = destPath + "/" + Path.GetFileName(entry);
                MoveOneEntry(entry,destfileName);
            }
            // If the user ahs created the directory explicitly it will exist in the dictionary
            if (_directoryEntries.ContainsKey(srcPath))
            {
                MoveOneEntry(srcPath,destPath);
            }
        }

        /// <summary>
        /// Removes the source entry and add a new entry in the internal dictionary with the same metadata of the source entry
        /// </summary>
        /// <param name="path">Source path name</param>
        /// <param name="destination">Destination path</param>
        /// <param name="overwrite">True if we want to overwrite the destination file</param>
        /// <param name="cancelToken">Cancellation token</param>
        /// <returns></returns>
        public override async Task<bool> RenameAsync(string path, string destination, bool overwrite = false,
            CancellationToken cancelToken = default(CancellationToken))
        {
            return await Task.Run(() =>
            {
                if (!_directoryEntries.ContainsKey(path))
                {
                    throw new AdlsException("the path does not exist");
                }

                // The destination exist part is still not fully mocked.
                if (_directoryEntries.ContainsKey(destination))
                {
                    if (_directoryEntries[destination].Entry.Type == DirectoryEntryType.FILE)
                    {
                        MoveOneEntry(path, destination);
                        return true;
                    }

                    var destEntryName = path + "/" + Path.GetFileName(path);
                    if (_directoryEntries.ContainsKey(destEntryName))
                    {
                        return false;
                    }

                    MoveDirectory(path, destEntryName);
                }
                else
                {
                    if (_directoryEntries[path].Entry.Type == DirectoryEntryType.FILE)
                    {
                        MoveOneEntry(path, destination);
                    }
                    else
                    {
                        MoveDirectory(path, destination);
                    }
                }

                return true;
            }, cancelToken);
        }

        /// <summary>
        /// Removes the source entry and add a new entry in the internal dictionary with the same metadata of the source entry
        /// </summary>
        /// <param name="path">Source path name</param>
        /// <param name="destination">Destination path</param>
        /// <param name="overwrite">True if we want to overwrite the destination file</param>
        /// <param name="cancelToken">Cancellation token</param>
        /// <returns></returns>
        public override bool Rename(string path, string destination, bool overwrite = false,
            CancellationToken cancelToken = default(CancellationToken))
        {
            return RenameAsync(path, destination, overwrite, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Get Directory or file info
        /// </summary>
        /// <param name="path">Path of file or directory</param>
        /// <param name="userIdFormat">User or group Id format</param>
        /// <param name="cancelToken">Cancellation token</param>
        public override async Task<DirectoryEntry> GetDirectoryEntryAsync(string path,
            UserGroupRepresentation userIdFormat = UserGroupRepresentation.ObjectID,
            CancellationToken cancelToken = default(CancellationToken))
        {
            return await Task.Run(() =>
            {
                // Update the length here
                if (_directoryEntries.ContainsKey(path))
                {
                    if (_directoryEntries[path].Entry.Type == DirectoryEntryType.FILE)
                    {
                        _directoryEntries[path].Entry.Length =
                            _directoryEntries[path].DataStream.Length; // Update the stream length
                    }

                    return new DirectoryEntry(_directoryEntries[path].Entry); // send deep copy
                }

                // The input path path can be a directory which is not in the dictionary
                foreach (var entries in _directoryEntries.Keys)
                {
                    if (entries.StartsWith(path + "/")) // This has to be directory
                    {
                        return new DirectoryEntry(path)
                        {
                            Type = DirectoryEntryType.DIRECTORY,
                            Length = 0
                        };
                    }
                }

                throw new AdlsException("Not exist") {HttpStatus = HttpStatusCode.NotFound};
            }, cancelToken);
        }

        /// <summary>
        /// Get Directory or file info
        /// </summary>
        /// <param name="path">Path of file or directory</param>
        /// <param name="userIdFormat">User or group Id format</param>
        /// <param name="cancelToken">Cancellation token</param>
        /// <returns></returns>
        public override DirectoryEntry GetDirectoryEntry(string path,
            UserGroupRepresentation userIdFormat = UserGroupRepresentation.ObjectID,
            CancellationToken cancelToken = default(CancellationToken))
        {
            return GetDirectoryEntryAsync(path, userIdFormat, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Concats the memory stream of source entries and merges them into a new memory stream
        /// </summary>
        /// <param name="destination">Destination entry</param>
        /// <param name="concatFiles">Concat files</param>
        /// <param name="deleteSource">True if we want to delete source</param>
        /// <param name="cancelToken">Cancellation token</param>
        public override async Task ConcatenateFilesAsync(string destination, List<string> concatFiles, bool deleteSource = false,
            CancellationToken cancelToken = default(CancellationToken))
        {
            await Task.Run(() =>
            {
                var memoryStream = CreateFile(destination, IfExists.Overwrite);
                foreach (var file in concatFiles)
                {
                    if (!_directoryEntries.ContainsKey(file))
                    {
                        throw new AdlsException("The file to concat does not exist");
                    }

                    _directoryEntries[file].DataStream.CopyTo(memoryStream);
                    Delete(file);
                }
            }, cancelToken);
        }

        /// <summary>
        /// Concats the memory stream of source entries and merges them into a new memory stream
        /// </summary>
        /// <param name="destination">Destination entry</param>
        /// <param name="concatFiles">Concat files</param>
        /// <param name="deleteSource">True if we want to delete source</param>
        /// <param name="cancelToken">Cancellation token</param>
        public override void ConcatenateFiles(string destination, List<string> concatFiles, bool deleteSource = false,
            CancellationToken cancelToken = default(CancellationToken))
        {
            ConcatenateFilesAsync(destination, concatFiles, deleteSource, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Returns a list of entries contained under the given directory
        /// </summary>
        /// <param name="path">Path of directory or file</param>
        /// <param name="userIdFormat">User or group Id format</param>
        /// <param name="cancelToken">Cancellation token</param>
        /// <returns></returns>
        public override IEnumerable<DirectoryEntry> EnumerateDirectory(string path, UserGroupRepresentation userIdFormat = UserGroupRepresentation.ObjectID,
            CancellationToken cancelToken = default(CancellationToken))
        {
            if (!path.EndsWith("/"))
            {
                path = path + "/";
            }
            var listToReturn = new List<DirectoryEntry>();
            // This is not fully mocked. Very naive only works for directory that has files and empty directories
            foreach (var directoryEntriesKey in _directoryEntries.Keys)
            {
                if (directoryEntriesKey.StartsWith(path))
                {
                    listToReturn.Add(new DirectoryEntry(_directoryEntries[directoryEntriesKey].Entry));
                }
            }
            return listToReturn;
        }

        /// <summary>
        /// Sets the expiry time for the file. 
        /// </summary>
        /// <param name="path">Path of file or directory</param>
        /// <param name="eopt">Expiry option</param>
        /// <param name="expiryTime">Expiry time</param>
        /// <param name="cancelToken">Cancellation token</param>
        public override async Task SetExpiryTimeAsync(string path, ExpiryOption eopt, long expiryTime,
            CancellationToken cancelToken = default(CancellationToken))
        {
            await Task.Run(() =>
            {
                if (!_directoryEntries.ContainsKey(path))
                {
                    throw new AdlsException("The file does not exist.");
                }

                DateTime? dateTimeToSet;
                if (eopt == ExpiryOption.Absolute)
                {
                    dateTimeToSet = DirectoryEntry.GetDateTimeFromServerTime(expiryTime);
                }
                else if (eopt == ExpiryOption.NeverExpire)
                {
                    dateTimeToSet = null;
                }
                else if (eopt == ExpiryOption.RelativeToNow)
                {
                    dateTimeToSet = DateTime.UtcNow + new TimeSpan(expiryTime * 10000);
                }
                else
                {
                    dateTimeToSet = _directoryEntries[path].CreationTime + new TimeSpan(expiryTime * 10000);
                }

                _directoryEntries[path].Entry.ExpiryTime = dateTimeToSet;
            }, cancelToken);
        }

        /// <summary>
        /// Sets the expiry time for the file. 
        /// </summary>
        /// <param name="path">Path of file or directory</param>
        /// <param name="eopt">Expiry option</param>
        /// <param name="expiryTime">Expiry time</param>
        /// <param name="cancelToken">Cancellation token</param>
        public override void SetExpiryTime(string path, ExpiryOption eopt, long expiryTime,
            CancellationToken cancelToken = default(CancellationToken))
        {
            SetExpiryTimeAsync(path, eopt, expiryTime, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Sets the permission string for the given path
        /// </summary>
        /// <param name="path">Path of file or directory</param>
        /// <param name="permission">Permission string</param>
        /// <param name="cancelToken">Cancellation token</param>
        public override async Task SetPermissionAsync(string path, string permission,
            CancellationToken cancelToken = default(CancellationToken))
        {
            await Task.Run(() =>
            {
                if (!_directoryEntries.ContainsKey(path))
                {
                    throw new AdlsException("The file does not exist.");
                }

                _directoryEntries[path].Entry.Permission = _directoryEntries[path].AclData.Permission = permission;
            }, cancelToken);
        }

        /// <summary>
        /// Sets the permission string for the given path
        /// </summary>
        /// <param name="path">Path of file or directory</param>
        /// <param name="permission">Permission string</param>
        /// <param name="cancelToken">Cancellation token</param>
        public override void SetPermission(string path, string permission,
            CancellationToken cancelToken = default(CancellationToken))
        {
            SetPermissionAsync(path, permission, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Adds acl entries for a given path
        /// </summary>
        /// <param name="path">Path of file or directory</param>
        /// <param name="aclSpec">Acl list to append</param>
        /// <param name="cancelToken">Cancellation token</param>
        public override async Task ModifyAclEntriesAsync(string path, List<AclEntry> aclSpec,
            CancellationToken cancelToken = default(CancellationToken))
        {
            await Task.Run(() =>
            {
                // Very naive, can be made intelligent by checking whether the acls are already present
                _directoryEntries[path].AclData.Entries.AddRange(aclSpec);
                _directoryEntries[path].Entry.HasAcl = true;
            }, cancelToken);
        }

        /// <summary>
        /// Adds acl entries for a given path
        /// </summary>
        /// <param name="path">Path of file or directory</param>
        /// <param name="aclSpec">Acl list to append</param>
        /// <param name="cancelToken">Cancellation token</param>
        public override void ModifyAclEntries(string path, List<AclEntry> aclSpec,
            CancellationToken cancelToken = default(CancellationToken))
        {
            ModifyAclEntriesAsync(path, aclSpec, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Sets new acl entries for the given path.
        /// </summary>
        /// <param name="path">Path of file or directory</param>
        /// <param name="aclSpec">Acl list to set</param>
        /// <param name="cancelToken">Cancellation token</param>
        public override async Task SetAclAsync(string path, List<AclEntry> aclSpec,
            CancellationToken cancelToken = default(CancellationToken))
        {
            await Task.Run(() =>
            {
                var newAclList = new List<AclEntry>();
                foreach (var aclEntry in aclSpec)
                {
                    if (((aclEntry.Type == AclType.user || aclEntry.Type == AclType.group) &&
                         string.IsNullOrEmpty(aclEntry.UserOrGroupId)) || aclEntry.Type == AclType.other)
                    {
                        continue;
                    }

                    newAclList.Add(aclEntry);
                }

                _directoryEntries[path].AclData.Entries = newAclList;
                _directoryEntries[path].Entry.HasAcl = true;
            }, cancelToken);
        }

        /// <summary>
        /// Sets new acl entries for the given path.
        /// </summary>
        /// <param name="path">Path of file or directory</param>
        /// <param name="aclSpec">Acl list to set</param>
        /// <param name="cancelToken">Cancellation token</param>
        public override void SetAcl(string path, List<AclEntry> aclSpec,
            CancellationToken cancelToken = default(CancellationToken))
        {
            SetAclAsync(path, aclSpec, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Sets the owner and group of the path
        /// </summary>
        /// <param name="path">Path of file or directory</param>
        /// <param name="owner">Owner guid</param>
        /// <param name="group">Group guid</param>
        /// <param name="cancelToken">Cancellation token</param>
        public override async Task SetOwnerAsync(string path, string owner, string group,
            CancellationToken cancelToken = default(CancellationToken))
        {
            await Task.Run(() =>
            {
                var metaData = _directoryEntries[path];
                if (!string.IsNullOrEmpty(owner))
                {
                    metaData.Entry.User = metaData.AclData.Owner = owner;
                }

                if (!string.IsNullOrEmpty(group))
                {
                    metaData.Entry.Group = metaData.AclData.Group = group;
                }
            }, cancelToken);
        }

        /// <summary>
        /// Sets the owner and group of the path
        /// </summary>
        /// <param name="path">Path of file or directory</param>
        /// <param name="owner">Owner guid</param>
        /// <param name="group">Group guid</param>
        /// <param name="cancelToken">Cancellation token</param>
        public override void SetOwner(string path, string owner, string group,
            CancellationToken cancelToken = default(CancellationToken))
        {
            SetOwnerAsync(path, owner, group, cancelToken).GetAwaiter().GetResult();
        }
        /// <summary>
        /// Removes specified Acl Entries for a file or directory from the internal AclStatus maintained in memory.
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="aclSpec">List of Acl Entries to remove</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public override async Task RemoveAclEntriesAsync(string path, List<AclEntry> aclSpec, CancellationToken cancelToken = default(CancellationToken))
        {
            await Task.Run(() =>
            {
                var toRemove = new List<AclEntry>();
                foreach (var aclEntryToRemove in aclSpec)
                {
                    foreach (var entry in _directoryEntries[path].AclData.Entries)
                    {
                        if (entry.Type.Equals(aclEntryToRemove.Type) &&
                            entry.UserOrGroupId.Equals(aclEntryToRemove.UserOrGroupId) &&
                            entry.Scope.Equals(aclEntryToRemove.Scope))
                        {
                            toRemove.Add(entry);
                        }
                    }
                }

                foreach (var entry in toRemove)
                {
                    _directoryEntries[path].AclData.Entries.Remove(entry);
                }
            }, cancelToken);
        }

        /// <summary>
        /// Removes specified Acl Entries for a file or directory from the internal AclStatus maintained in memory.
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="aclSpec">List of Acl Entries to remove</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public override void RemoveAclEntries(string path, List<AclEntry> aclSpec,
            CancellationToken cancelToken = default(CancellationToken))
        {
            RemoveAclEntriesAsync(path, aclSpec, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Removes all Acl Entries for a file or directory from the internal AclStatus maintained in memory.
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public override async Task RemoveAllAclsAsync(string path, CancellationToken cancelToken = default(CancellationToken))
        {
            await Task.Run(() =>
            {
                _directoryEntries[path].AclData.Entries = new List<AclEntry>();
                _directoryEntries[path].Entry.HasAcl = false;
            }, cancelToken);
        }

        /// <summary>
        /// Removes all Acl Entries for a file or directory from the internal AclStatus maintained in memory.
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public override void RemoveAllAcls(string path, CancellationToken cancelToken = default(CancellationToken))
        {
            RemoveAllAclsAsync(path, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Removes all Acl Entries of AclScope default for a file or directory from the internal AclStatus maintained in memory.
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public override async Task RemoveDefaultAclsAsync(string path, CancellationToken cancelToken = default(CancellationToken))
        {
            await Task.Run(() =>
            {
                var defaultAclList = new List<AclEntry>();
                foreach (var aclEntry in _directoryEntries[path].AclData.Entries)
                {
                    if (aclEntry.Scope == AclScope.Default)
                    {
                        defaultAclList.Add(aclEntry);
                    }
                }

                foreach (var aclEntry in defaultAclList)
                {
                    _directoryEntries[path].AclData.Entries.Remove(aclEntry);
                }
            }, cancelToken);
        }

        /// <summary>
        /// Removes all Acl Entries of AclScope default for a file or directory from the internal AclStatus maintained in memory.
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public override void RemoveDefaultAcls(string path, CancellationToken cancelToken = default(CancellationToken))
        {
            RemoveDefaultAclsAsync(path, cancelToken).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Gets the ACL entry list, owner ID, group ID, octal permission and sticky bit (only for a directory) of the file/directory
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="userIdFormat">way to represent the user/group object</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public override async Task<AclStatus> GetAclStatusAsync(string path, UserGroupRepresentation userIdFormat = UserGroupRepresentation.ObjectID, CancellationToken cancelToken = default(CancellationToken))
        {
            return await Task.Run(() => new AclStatus(_directoryEntries[path].AclData), cancelToken);
        }

        /// <summary>
        /// Gets the ACL entry list, owner ID, group ID, octal permission and sticky bit (only for a directory) of the file/directory
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="userIdFormat">way to represent the user/group object</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public override AclStatus GetAclStatus(string path, UserGroupRepresentation userIdFormat = UserGroupRepresentation.ObjectID, CancellationToken cancelToken = default(CancellationToken))
        {
            return GetAclStatusAsync(path, userIdFormat, cancelToken).GetAwaiter().GetResult();
        }
        /// <summary>
        /// Bulk uploads file only. Reads a local file and maintains the memory stream for the entry
        /// </summary>
        /// <param name="srcPath">Local source path</param>
        /// <param name="destPath">Remote destination path - It should always be a directory.</param>
        /// <param name="numThreads">Not used</param>
        /// <param name="shouldOverwrite">Whether to overwrite or skip if the destination exists</param>
        /// <param name="progressTracker">Not used</param>
        /// <param name="notRecurse">Not used</param>
        /// <param name="resume">Not used</param>
        /// <param name="isBinary">Not used</param>
        /// <returns>Transfer Status encapsulating the details of upload</returns>
        public override TransferStatus BulkUpload(string srcPath, string destPath, int numThreads = -1, IfExists shouldOverwrite = IfExists.Overwrite, IProgress<TransferStatus> progressTracker = null, bool notRecurse = false, bool resume = false, bool isBinary = false, CancellationToken cancelToken=default(CancellationToken))
        {
            // Currently this is for a single file
            if (!File.Exists(srcPath))
            {
                throw new ArgumentException("Currently not supported for folder");
            }
            using (Stream localStream = new FileStream(srcPath, FileMode.Open, FileAccess.Read),
                adlsStream=CreateFile(destPath,shouldOverwrite))
            {
                byte[] buff=new byte[AdlsOutputStream.BufferCapacity];
                while (true)
                {
                    int bytesRead = localStream.Read(buff, 0, buff.Length);
                    if (bytesRead > 0)
                    {
                        adlsStream.Write(buff, 0, bytesRead);
                    }
                    else
                    {
                        break;
                    }
                }
            }
            return new TransferStatus();
        }
        /// <summary>
        /// Reads data from memory stream and save it to local file
        /// </summary>
        /// <param name="srcPath">Remote source path</param>
        /// <param name="destPath">Local destination path. It should always be a directory.</param>
        /// <param name="numThreads">Not used</param>
        /// <param name="shouldOverwrite">Whether to overwrite or skip if the destination exists</param>
        /// <param name="progressTracker">Not used</param>
        /// <param name="notRecurse">Not used</param>
        /// <param name="resume">Not used</param>
        /// <returns>Transfer status encapsulating the details of download</returns>
        public override TransferStatus BulkDownload(string srcPath, string destPath, int numThreads = -1, IfExists shouldOverwrite = IfExists.Overwrite, IProgress<TransferStatus> progressTracker = null, bool notRecurse = false, bool resume = false, CancellationToken cancelToken=default(CancellationToken))
        {
            var entry = _directoryEntries[srcPath].Entry;
            if (entry.Type != DirectoryEntryType.FILE)
            {
                throw new ArgumentException("Currently not supported for folder");
            }
            using (Stream localStream = new FileStream(destPath, FileMode.Create, FileAccess.ReadWrite),
                            adlsStream = GetReadStream(srcPath))
            {
                byte[] buff = new byte[AdlsOutputStream.BufferCapacity];
                while (true)
                {
                    int bytesRead = adlsStream.Read(buff, 0, buff.Length);
                    if (bytesRead > 0)
                    {
                        localStream.Write(buff, 0, bytesRead);
                    }
                    else
                    {
                        break;
                    }
                }
            }
            return new TransferStatus();
        }

        /// <summary>
        /// Currently the recursive entities need to be created separately for mock testing
        /// </summary>
        /// <param name="path"></param>
        /// <param name="aclEntries"></param>
        /// <param name="type"></param>
        /// <param name="threadCount"></param>
        /// <param name="statusTracker"></param>
        /// <param name="cancelToken"></param>
        /// <returns></returns>
        public override AclProcessorStats ChangeAcl(string path, List<AclEntry> aclEntries, RequestedAclType type, int threadCount, IProgress<AclProcessorStats> statusTracker, CancellationToken cancelToken)
        {
            return ChangeAcl(path, aclEntries, type, threadCount);
        }

        /// <summary>
        /// Currently the recursive entities need to be created separately for mock testing
        /// </summary>
        /// <param name="path"></param>
        /// <param name="aclEntries"></param>
        /// <param name="type"></param>
        /// <param name="threadCount"></param>
        /// <returns></returns>
        public override AclProcessorStats ChangeAcl(string path, List<AclEntry> aclEntries, RequestedAclType type, int threadCount = -1)
        {
            int numDirs = 0, numFiles = 0;
            foreach (var directoryEntriesKey in _directoryEntries.Keys)
            {
                if (directoryEntriesKey.StartsWith(path))
                {
                    switch (type)
                    {
                        case RequestedAclType.ModifyAcl: ModifyAclEntries(_directoryEntries[directoryEntriesKey].Entry.FullName,aclEntries);
                            break;
                        case RequestedAclType.RemoveAcl:
                            RemoveAclEntries(_directoryEntries[directoryEntriesKey].Entry.FullName, aclEntries);
                            break;
                        case RequestedAclType.SetAcl:
                            SetAcl(_directoryEntries[directoryEntriesKey].Entry.FullName, aclEntries);
                            break;
                    }

                    if (_directoryEntries[directoryEntriesKey].Entry.Type == DirectoryEntryType.DIRECTORY)
                    {
                        numDirs++;
                    }
                    else
                    {
                        numFiles++;
                    }
                }
            }
            return new AclProcessorStats(numFiles,numDirs);
        }

        /// <summary>
        /// Currently the recursive entities need to be created separately for mock testing
        /// </summary>
        /// <param name="path"></param>
        /// <param name="numThreads"></param>
        /// <param name="cancelToken"></param>
        /// <returns></returns>
        public override ContentSummary GetContentSummary(string path, int numThreads = -1,
            CancellationToken cancelToken = default(CancellationToken))
        {
            int numDirs = 0, numFiles = 0;
            long numSize = 0;
            foreach (var directoryEntriesKey in _directoryEntries.Keys)
            {
                if (directoryEntriesKey.StartsWith(path) && !directoryEntriesKey.Equals(path))
                {
                    if (_directoryEntries[directoryEntriesKey].Entry.Type == DirectoryEntryType.DIRECTORY)
                    {
                        numDirs++;
                    }
                    else
                    {
                        numFiles++;
                        numSize += _directoryEntries[directoryEntriesKey].Entry.Length;
                    }
                }
            }
            return new ContentSummary(numDirs, numFiles, numSize, numSize);
        }

        /// <summary>
        /// Gets fileproperties, conmsistentacl is always true since this is mock
        /// </summary>
        /// <param name="path"></param>
        /// <param name="getAclUsage"></param>
        /// <param name="dumpFileName"></param>
        /// <param name="getDiskUsage"></param>
        /// <param name="saveToLocal"></param>
        /// <param name="numThreads"></param>
        /// <param name="displayFiles"></param>
        /// <param name="hideConsistentAcl"></param>
        /// <param name="maxDepth"></param>
        public override void GetFileProperties(string path, bool getAclUsage, string dumpFileName, bool getDiskUsage = true,
            bool saveToLocal = true, int numThreads = -1, bool displayFiles = false, bool hideConsistentAcl = false,
            long maxDepth = Int64.MaxValue, CancellationToken cancelToken = default(CancellationToken))
        {
            if (!(getAclUsage || getDiskUsage))
            {
                throw new ArgumentException("At least one option of getAclUsage and getDiskUsage need to be set as true.");
            }

            if (!displayFiles && hideConsistentAcl)
            {
                throw new ArgumentException("hideConsistentAcl cannot be true when displayFiles is false because consistent Acl cannot be determined unless we retrieve acls for the files also.");
            }
            if (saveToLocal)
            {
                Utils.CreateParentDirectory(dumpFileName);
            }
            using (var propertyDumpWriter = new StreamWriter((saveToLocal
                ? new FileStream(dumpFileName, FileMode.Create, FileAccess.ReadWrite)
                : (Stream)CreateFile(dumpFileName, IfExists.Overwrite))))
            {
                WriteHeader(propertyDumpWriter, getDiskUsage, getAclUsage, displayFiles, hideConsistentAcl);
                foreach (var directoryEntriesKey in _directoryEntries.Keys)
                {
                    
                    if (directoryEntriesKey.StartsWith(path))
                    {
                        var summary = _directoryEntries[directoryEntriesKey].Entry.Type == DirectoryEntryType.DIRECTORY
                            ? GetContentSummary(_directoryEntries[directoryEntriesKey].Entry.FullName)
                            : new ContentSummary(0, 1, _directoryEntries[directoryEntriesKey].Entry.Length,
                                _directoryEntries[directoryEntriesKey].Entry.Length);
                        var status = GetAclStatus(_directoryEntries[directoryEntriesKey].Entry.FullName);
                        string output = "";
                        if (getDiskUsage)
                        {
                            output =
                                $"1{FileProperties.PropertyManager.OuputLineSeparator}1{FileProperties.PropertyManager.OuputLineSeparator}1{FileProperties.PropertyManager.OuputLineSeparator}{summary.Length}{FileProperties.PropertyManager.OuputLineSeparator}{summary.FileCount}{FileProperties.PropertyManager.OuputLineSeparator}{summary.DirectoryCount}";
                        }
                        if (getAclUsage)
                        {
                            bool showAclConsistentColumn = displayFiles || hideConsistentAcl;
                            output +=
                                $"{(string.IsNullOrEmpty(output) ? "" : $"{FileProperties.PropertyManager.OuputLineSeparator}")}{string.Join("|", status.Entries)}{(showAclConsistentColumn ? $"{FileProperties.PropertyManager.OuputLineSeparator}true" : "")}";
                        }
                        propertyDumpWriter.WriteLine($"{_directoryEntries[directoryEntriesKey].Entry.FullName}{FileProperties.PropertyManager.OuputLineSeparator}{_directoryEntries[directoryEntriesKey].Entry.Type}{FileProperties.PropertyManager.OuputLineSeparator}{output}");
                    }
                }
            }
        }

        private void WriteHeader(StreamWriter propertyDumpWriter, bool getSizeProperty, bool getAclProperty, bool displayFiles, bool hideConsistentAclTree)
        {
            string output = "";
            if (getSizeProperty)
            {
                output =
                    $"Total size of direct child files and directories{FileProperties.PropertyManager.OuputLineSeparator}Total number of direct files{FileProperties.PropertyManager.OuputLineSeparator}Total number of direct directories{FileProperties.PropertyManager.OuputLineSeparator}Total size{FileProperties.PropertyManager.OuputLineSeparator}Total number of files{FileProperties.PropertyManager.OuputLineSeparator}Total number of directories";
            }
            if (getAclProperty)
            {
                // If DisplayFiles is false that means HideConsistentAcl has to be false (there is a check at entry point in client)/
                // And if DisplayFiles is false consistentAcl information is not correct since acl of files is not known
                bool showAclConsistentColumn = displayFiles || hideConsistentAclTree;
                output +=
                        $"{(string.IsNullOrEmpty(output) ? "" : $"{FileProperties.PropertyManager.OuputLineSeparator}")}Acl Entries{(showAclConsistentColumn ? FileProperties.PropertyManager.OuputLineSeparator + "Whether Acl is same for all descendants" : "")}";
            }
            propertyDumpWriter.WriteLine($"Entry name{FileProperties.PropertyManager.OuputLineSeparator}Entry Type{FileProperties.PropertyManager.OuputLineSeparator}{output}");
        }
       
    }
}

