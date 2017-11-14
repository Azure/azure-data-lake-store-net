using Microsoft.Azure.DataLake.Store.Acl;
using System.Collections.Generic;

namespace Microsoft.Azure.DataLake.Store.AclTools.Jobs
{
    internal class VerifyChangeAclJob : BaseJob
    {
        private readonly AclProcessor _aclProcess;
        private readonly string _fullPath;
        private readonly DirectoryEntryType _entryType;
        private bool _verifyCorrect;
        internal VerifyChangeAclJob(AclProcessor aclProcess, string fullPath, DirectoryEntryType type):base(1)
        {
            _aclProcess = aclProcess;
            _fullPath = fullPath;
            _entryType = type;
        }
        protected override object DoJob()
        {
            var aclEntries = _entryType == DirectoryEntryType.DIRECTORY ? _aclProcess.AclEntries : _aclProcess.FileAclEntries;
            var status = _aclProcess.Client.GetAclStatus(_fullPath);
            var remoteAclEntries = status.Entries;
            _verifyCorrect = CheckAclListContains(remoteAclEntries, aclEntries,_aclProcess.Type == RequestedAclType.RemoveAcl);
            if (_verifyCorrect)
            {
                _aclProcess.IncrementCorrectCount(_entryType);
            }
            return null;
        }
        
        /// <summary>
        /// Verifies whether Acl has been changed corerctly by comparing current Acl entries for the directory entry and the input acl entries
        /// </summary>
        /// <param name="parentList">Acl entries currently retrieved from the file on server</param>
        /// <param name="subList">Acl entries from input that was given as input to Acl Processor</param>
        /// <param name="notContains">Whether to check remove Acl has worked</param>
        /// <returns>True if the verify is correct else false</returns>
        private bool CheckAclListContains(List<AclEntry> parentList, List<AclEntry> subList, bool notContains = false)
        {
            HashSet<string> hSet = new HashSet<string>();
            foreach (var en in parentList)
            {
                hSet.Add(en.ToString());
            }
            foreach (var entry in subList)
            {
                if (!notContains)
                {
                    if (!hSet.Contains(entry.ToString()))
                    {
                        return false;
                    }
                }
                else
                {
                    if (hSet.Contains(entry.ToString()))
                    {
                        return false;
                    }
                }
            }
            return true;
        }
        protected override string JobDetails()
        {
            return $"EntryName: {_fullPath}, EntryType: {_entryType}, Correct: {_verifyCorrect}";
        }

        protected override string JobType()
        {
            return $"AclProcessor.Verify{_aclProcess.Type}";
        }
    }
}
