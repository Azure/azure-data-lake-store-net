using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Azure.DataLake.Store.RetryPolicies;
using Microsoft.Azure.DataLake.Store.Serialization;

namespace Microsoft.Azure.DataLake.Store
{
    /// <summary>
    /// Enumerable that exposes enumerator:FileStatusList
    /// </summary>
    internal class FileStatusOutput<T> : IEnumerable<T> where T:DirectoryEntry
    {
        /// <summary>
        /// Number of maximum directory entries to be retrieved from server. If -1 then retrieve all entries
        /// </summary>
        private readonly int _maxEntries;

        /// <summary>
        /// Filename after which list of files should be obtained from server
        /// </summary>
        private readonly string _listAfter;

        /// <summary>
        /// Filename till which list of files should be obtained from server
        /// </summary>
        private readonly string _listBefore;

        /// <summary>
        /// ADLS Client
        /// </summary>
        private readonly AdlsClient _client;

        /// <summary>
        /// Way the user or group object will be represented
        /// </summary>
        private readonly UserGroupRepresentation? _ugr;

        /// <summary>
        /// Path of the directory containing the sub-directories or files
        /// </summary>
        private readonly string _path;

        /// <summary>
        /// selection
        /// </summary>
        private readonly Selection _selection;

        private readonly IDictionary<string, string> _extraQueryParamsForListStatus;
        private readonly CancellationToken _cancelToken;
        /// <summary>
        /// Returns the enumerator
        /// </summary>
        /// <returns></returns>
        public IEnumerator<T> GetEnumerator()
        {
            return new FileStatusList<T>(_listBefore, _listAfter, _maxEntries, _ugr, _client, _selection, _path, _cancelToken, _extraQueryParamsForListStatus);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        internal FileStatusOutput(string listBefore, string listAfter, int maxEntries, UserGroupRepresentation? ugr, AdlsClient client, string path, CancellationToken cancelToken, IDictionary<string, string> extraQueryParamsForListStatus = null)
        {
            _listBefore = listBefore;
            _maxEntries = maxEntries;
            _listAfter = listAfter;
            _ugr = ugr;
            _client = client;
            _path = path;
            _cancelToken = cancelToken;
            _extraQueryParamsForListStatus = extraQueryParamsForListStatus;
        }
        internal FileStatusOutput(string listBefore, string listAfter, int maxEntries, UserGroupRepresentation? ugr, AdlsClient client, string path, Selection selection, CancellationToken cancelToken, IDictionary<string, string> extraQueryParamsForListStatus = null)
        {
            _listBefore = listBefore;
            _maxEntries = maxEntries;
            _listAfter = listAfter;
            _ugr = ugr;
            _client = client;
            _path = path;
            _selection = selection;
            _cancelToken = cancelToken;
            _extraQueryParamsForListStatus = extraQueryParamsForListStatus;
        }
    }
    /// <summary>
    /// Encapsulates a collection storing the list of directory entries. Once the collection is traversed, retrieves next set of directory entries from server
    /// This is for internal use only. Made public because we want to cast the enumerator to test enumeration with a smaller page size.
    /// </summary>
    internal class FileStatusList<T> : IEnumerator<T> where T:DirectoryEntry
    {
        /// <summary>
        /// Internal collection storing list of directory entries retrieved from server. This is not the whole list of directory entries.
        /// It's size is less than equal to listSize
        /// </summary>
        private List<T> FileStatus { get; set; }

        /// <summary>
        /// Number of maximum directory entries to retrieve from server at one time
        /// </summary>
        private int _listSize = 4000;
        /// <summary>
        /// Internal property to set the list size
        /// </summary>
        internal int ListSize
        {
            set { _listSize = value > 4000 ? 4000 : value; }
            private get { return _listSize; }
        }

        /// <summary>
        /// Maximum number of entries to be enumerated as entered by user. If it is -1 then enumerate all the directory entries
        /// </summary>
        private readonly int _maxEntries;
        /// <summary>
        /// Number of entries left to enumerate
        /// </summary>
        private int RemainingEntries { get; set; }
        /// <summary>
        /// Flag indicating enumerate all the directory entries
        /// </summary>
        private bool EnumerateAll { get; }

        /// <summary>
        /// Filename after which we should start the enumeration - entered by user
        /// </summary>
        private readonly string _listAfterClient;
        /// <summary>
        /// Filename after which list of files should be obtained from server next time, updated before everytime the 
        /// </summary>
        private string ListAfterNext { get; set; }

        /// <summary>
        /// Filename till which list of files should be obtained from server
        /// </summary>
        private string ListBefore { get; }
        /// <summary>
        /// ADLS Client
        /// </summary>
        private AdlsClient Client { get; }
        
        /// <summary>
        /// selection
        /// </summary>
        private Selection Selection { get; }

        /// <summary>
        /// Way the user or group object will be represented
        /// </summary>
        private UserGroupRepresentation? Ugr { get; }
        /// <summary>
        /// Path of the directory conatianing the sub-directories or files
        /// </summary>
        private string Path { get; }
        private readonly CancellationToken _cancelToken;
        private readonly IDictionary<string, string> _extraQueryParamsForListStatus;
        /// <summary>
        /// Represents the current directory entry in the internal collection: FileStatus
        /// </summary>
        
        public T Current
        {
            get
            {
                try
                {
                    return FileStatus[_position];
                }
                catch (IndexOutOfRangeException)
                {
                    throw new InvalidOperationException("The index is out of range");
                }
            }
        }
        /// <summary>
        /// Index representating the current position in the internal collection: FileStatus 
        /// </summary>
        private int _position = -1;
        /// <summary>
        /// Immplemented interface property
        /// </summary>
        object IEnumerator.Current => Current;

        /// <summary>
        /// Represent the continationToken for ListStatus
        /// </summary>
        private string continuationToken;

        /// <summary>
        /// Advances the enumerator to the next element in the internal collection.
        /// If the end of the internal collection is reached, performs a ListStatus call to the server to see if any more directories/files need to be enumerated. If yes
        /// then the internal collection is populated with the next set of directory entries. The internal index pointing to the current element is updated. If not, then returns false.
        /// </summary>
        /// <returns>True if there is a next element to enumerate else false</returns>
        public bool MoveNext()
        {
            if (_cancelToken.IsCancellationRequested)
            {
                throw new OperationCanceledException();
            }
            //Not called for first time, first time when this is called ListAfterNext will be whatever client has passed
            if (FileStatus != null)
            {
                _position++;
                //FileStatus.Count will be minimum of ListSize and remaining entries asked by user
                if (_position < FileStatus.Count)//Still more data to be enumerated
                {
                    if (!EnumerateAll)
                    {
                        RemainingEntries--;
                    }
                    return true;
                }
                //Number of entries wanted by the user is already enumerated
                //RemainingEntries 0 means no need to look at server since last time we retrieved "RemainingEntries" number of entries from server
                if (!EnumerateAll && RemainingEntries <= 0)
                {
                    return false;
                }

                // Older behavior for Selection == Minimal. Remove this if else when API is updated.
                if (Selection == Selection.Minimal)
                {
                       if (_position < ListSize)
                    {
                        //position has reached end of the internal list. But number of directory entries retrieved from last server call is less than list 
                        //size so no more entries are left on server. So even though RemainingEntries is positive, but there is no data in server. Return false
                        return false;
                    }
                    //Else we have to look in server to see if we still have any more directory entries to enumerate
                    //Obtain the last enumerated entry name so that we can retrieve files after that from server
                    ListAfterNext = FileStatus[_position - 1].Name;
                }
                else
                {
                    if (string.IsNullOrEmpty(continuationToken))
                    {
                        //Continuation token is blank or null. No more entries.
                        return false;
                    }
                    else
                    {
                        //Obtain the last enumerated entry name so that we can retrieve files after that from server
                        ListAfterNext = continuationToken;
                    }
                }
                                
            }
            _position = -1;
            OperationResponse resp = new OperationResponse();
            int getListSize = EnumerateAll ? ListSize : Math.Min(ListSize, RemainingEntries);
            // EnumerateDirectoryChangeAclJob also calls core separately. If you change logic here, consider changing there also
            var fileListResult = Core.ListStatusAsync<DirectoryEntryListResult<T>>(Path, ListAfterNext, ListBefore, getListSize, Ugr, Selection, _extraQueryParamsForListStatus, Client, new RequestOptions(new ExponentialRetryPolicy()), resp, _cancelToken).GetAwaiter().GetResult();
            if (!resp.IsSuccessful)
            {
                throw Client.GetExceptionFromResponse(resp, "Error getting listStatus for path " + Path + " after " + ListAfterNext);
            }
            FileStatus = Core.GetDirectoryEntryListWithFullPath(Path, fileListResult, resp);
            if (!resp.IsSuccessful)
            {
                throw Client.GetExceptionFromResponse(resp, "Unexpected error getting listStatus for path " + Path + " after " + ListAfterNext);
            }
            // Retrieve the continuation token here since above we have checked whether fileListResult. FileStatuses is not null
            continuationToken = fileListResult.FileStatuses.ContinuationToken;

            return MoveNext();
        }

        internal FileStatusList(string listBefore, string listAfter, int maxEntries, UserGroupRepresentation? ugr, AdlsClient client, Selection selection, string path, CancellationToken cancelToken, IDictionary<string, string> extraQueryParamsForListStatus = null)
        {
            ListBefore = listBefore;
            ListAfterNext = _listAfterClient = listAfter;
            RemainingEntries = _maxEntries = maxEntries;
            if (_maxEntries == -1)
            {
                EnumerateAll = true;
            }
            Ugr = ugr;
            Client = client;
            Selection = selection;
            Path = path;
            _cancelToken = cancelToken;
            _extraQueryParamsForListStatus = extraQueryParamsForListStatus;
        }
        /// <summary>
        /// Clears the internal collection and resets the index, ListAfterNext and remaininig entries of collection
        /// </summary>
        public void Reset()
        {
            FileStatus = null;
            _position = -1;
            ListAfterNext = _listAfterClient;
            RemainingEntries = _maxEntries;
        }
        /// <summary>
        /// Disposes the enumerable
        /// </summary>
        public void Dispose()
        {
            FileStatus = null;
        }
    }
}
