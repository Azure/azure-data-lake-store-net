using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading;
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
    /// Mock Adls Client. All the operations are done in memory.
    /// </summary>
    public sealed class MockAdlsClient : AdlsClient
    {
        private readonly Dictionary<string, DirectoryEntryMetaData> _directoryEntries;
        private static readonly string Owner = Guid.NewGuid().ToString();
        private static readonly string Group = Guid.NewGuid().ToString();

        private MockAdlsClient()
        {
            _directoryEntries = new Dictionary<string, DirectoryEntryMetaData>();
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
            return new MockAdlsClient();
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
        /// <returns></returns>
        public override bool CreateDirectory(string dirName, string octalPermission = null,
            CancellationToken cancelToken = default(CancellationToken))
        {
            _directoryEntries.Add(dirName, CreateMetaData(dirName,DirectoryEntryType.DIRECTORY, octalPermission));
            return true;
        }
        /// <summary>
        /// Returns a memory stream for reading data of the file
        /// </summary>
        /// <param name="filename">File name</param>
        /// <param name="cancelToken">Cancellation token</param>
        /// <returns></returns>
        public override AdlsInputStream GetReadStream(string filename, CancellationToken cancelToken = default(CancellationToken))
        {
            if (!_directoryEntries.ContainsKey(filename))
            {
                throw new AdlsException("The file does not exists");
            }
            return new MockAdlsInputStream(_directoryEntries[filename].DataStream);
        }
        /// <summary>
        /// Returns the memory stream for appending to the file encapsulated in mock adls output stream.
        /// </summary>
        /// <param name="filename">File name</param>
        /// <param name="cancelToken">Cancellation token</param>
        /// <returns></returns>
        public override AdlsOutputStream GetAppendStream(string filename, CancellationToken cancelToken = default(CancellationToken))
        {
            if (!_directoryEntries.ContainsKey(filename))
            {
                throw new AdlsException("The file does not exists");
            }
            _directoryEntries[filename].DataStream.Seek(_directoryEntries[filename].DataStream.Length,
                SeekOrigin.Begin);
            return new MockAdlsOutputStream(_directoryEntries[filename].DataStream, _directoryEntries[filename].Entry);
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
            if (mode == IfExists.Fail && _directoryEntries.ContainsKey(filename))
            {
                throw new AdlsException("The file exists");
            }
            var metaData = CreateMetaData(filename, DirectoryEntryType.FILE, octalPermission);
            _directoryEntries.Add(filename, metaData);
            return new MockAdlsOutputStream(metaData.DataStream, metaData.Entry);
        }
        /// <summary>
        /// Delete an entry from the internal dictionary
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="cancelToken">Cancellation token</param>
        /// <returns></returns>
        public override bool Delete(string path, CancellationToken cancelToken = default(CancellationToken))
        {
            foreach (var directoryEntriesKey in _directoryEntries.Keys)
            {
                if (directoryEntriesKey.StartsWith(path+"/"))
                {
                    return false;
                }
            }
            return _directoryEntries.Remove(path);
        }
        /// <summary>
        /// Deletes all entries within a directory or delete a file
        /// </summary>
        /// <param name="path">Path of directory or file</param>
        /// <param name="cancelToken">Cancellation token</param>
        /// <returns></returns>
        public override bool DeleteRecursive(string path, CancellationToken cancelToken = default(CancellationToken))
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
        public override bool Rename(string path, string destination, bool overwrite = false,
            CancellationToken cancelToken = default(CancellationToken))
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
                    MoveOneEntry(path,destination);
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
                    MoveDirectory(path,destination);
                }
            }
            return true;
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
            // Update the length here
            if (_directoryEntries.ContainsKey(path))
            {
                if (_directoryEntries[path].Entry.Type == DirectoryEntryType.FILE)
                {
                    _directoryEntries[path].Entry.Length = _directoryEntries[path].DataStream.Length; // Update the stream length
                }
                return new DirectoryEntry(_directoryEntries[path].Entry); // send deep copy
            }
            // The input path path can be a directory which is not in the dictionary
            foreach (var entries in _directoryEntries.Keys)
            {
                if (entries.StartsWith(path+"/")) // This has to be directory
                {
                    return new DirectoryEntry(path)
                    {
                        Type = DirectoryEntryType.DIRECTORY,
                        Length = 0
                    };
                }
            }
            throw new AdlsException("Not exist") { HttpStatus = HttpStatusCode.NotFound };
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
        public override void SetExpiryTime(string path, ExpiryOption eopt, long expiryTime,
            CancellationToken cancelToken = default(CancellationToken))
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
            if (!_directoryEntries.ContainsKey(path))
            {
                throw new AdlsException("The file does not exist.");
            }
            _directoryEntries[path].Entry.Permission = _directoryEntries[path].AclData.Permission = permission;
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
            // Very naive, can be made intelligent by checking whether the acls are already present
            _directoryEntries[path].AclData.Entries.AddRange(aclSpec);
            _directoryEntries[path].Entry.HasAcl = true;
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
            var metaData=_directoryEntries[path];
            if (!string.IsNullOrEmpty(owner))
            {
                metaData.Entry.User = metaData.AclData.Owner = owner;
            }
            if (!string.IsNullOrEmpty(group))
            {
                metaData.Entry.Group = metaData.AclData.Group = group;
            }
        }
        /// <summary>
        /// Removes specified Acl Entries for a file or directory from the internal AclStatus maintained in memory.
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="aclSpec">List of Acl Entries to remove</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public override void RemoveAclEntries(string path, List<AclEntry> aclSpec, CancellationToken cancelToken = default(CancellationToken))
        {
            var toRemove=new List<AclEntry>();
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
        }
        /// <summary>
        /// Removes all Acl Entries for a file or directory from the internal AclStatus maintained in memory.
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public override void RemoveAllAcls(string path, CancellationToken cancelToken = default(CancellationToken))
        {
            _directoryEntries[path].AclData.Entries=new List<AclEntry>();
            _directoryEntries[path].Entry.HasAcl = false;
        }
        /// <summary>
        /// Removes all Acl Entries of AclScope default for a file or directory from the internal AclStatus maintained in memory.
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public override void RemoveDefaultAcls(string path, CancellationToken cancelToken = default(CancellationToken))
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
        }
        /// <summary>
        /// Gets the ACL entry list, owner ID, group ID, octal permission and sticky bit (only for a directory) of the file/directory
        /// </summary>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="userIdFormat">way to represent the user/group object</param>
        /// <param name="cancelToken">CancellationToken to cancel the request</param>
        public override AclStatus GetAclStatus(string path, UserGroupRepresentation userIdFormat = UserGroupRepresentation.ObjectID, CancellationToken cancelToken = default(CancellationToken))
        {
            return new AclStatus(_directoryEntries[path].AclData);
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
            long maxDepth = Int64.MaxValue)
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
                Directory.CreateDirectory(Path.GetDirectoryName(dumpFileName));
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

