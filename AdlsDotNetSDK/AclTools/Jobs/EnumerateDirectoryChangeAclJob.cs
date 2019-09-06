
using Microsoft.Azure.DataLake.Store.RetryPolicies;
using Microsoft.Azure.DataLake.Store.Serialization;

namespace Microsoft.Azure.DataLake.Store.AclTools.Jobs
{
    internal class EnumerateDirectoryChangeAclJob : BaseJob
    {
        private const int listSize = 4000;
        private readonly AclProcessor _aclProcess;
        internal readonly string FullPath;
        private readonly string listAfter;
        internal EnumerateDirectoryChangeAclJob(AclProcessor aclProcess, string fullPath, string ltAfter = "") : base(1)
        {
            _aclProcess = aclProcess;
            FullPath = fullPath;
            listAfter = ltAfter;
        }
        protected override object DoJob()
        {
            OperationResponse resp = new OperationResponse();
            var getListStatusResult = Core.ListStatusAsync<DirectoryEntryListResult<DirectoryEntry>>(FullPath, listAfter, null, listSize, UserGroupRepresentation.ObjectID, Selection.Minimal, null, _aclProcess.Client, new RequestOptions(new ExponentialRetryPolicy()), resp).GetAwaiter().GetResult();
            if (!resp.IsSuccessful)
            {
                throw _aclProcess.Client.GetExceptionFromResponse(resp, "Error getting listStatus for path " + FullPath + " after " + listAfter);
            }

            var directoriyEntries = Core.GetDirectoryEntryListWithFullPath(FullPath, getListStatusResult, resp);
            if (!resp.IsSuccessful)
            {
                throw _aclProcess.Client.GetExceptionFromResponse(resp, "Error getting listStatus for path " + FullPath + " after " + listAfter);
            }

            var continuationToken = getListStatusResult.FileStatuses.ContinuationToken;

            foreach (var dir in directoriyEntries)
            {
                if (dir.Attribute != null && dir.Attribute.Contains(DirectoryEntryAttributeType.Link)){
                    _aclProcess.AddLinkPath(dir.FullName);
                }
                else {
                    _aclProcess.ProcessDirectoryEntry(dir);
                }
            }
            if (!string.IsNullOrEmpty(continuationToken))
            {
                _aclProcess.Queue.Add(new EnumerateDirectoryChangeAclJob(_aclProcess, FullPath, continuationToken));

            }
            return null;
        }

        protected override string JobDetails()
        {
            return $"Directory: {FullPath}";
        }

        protected override string JobType()
        {
            return "AclProcessor.Enumerate";
        }
    }
}
