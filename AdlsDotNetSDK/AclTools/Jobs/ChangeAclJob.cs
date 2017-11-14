

namespace Microsoft.Azure.DataLake.Store.AclTools.Jobs
{
    internal class ChangeAclJob : BaseJob
    {
        private readonly AclProcessor _aclProcess;
        private readonly string _fullPath;
        private readonly DirectoryEntryType _entryType;
        internal ChangeAclJob(AclProcessor aclProcess, string fullPath, DirectoryEntryType type):base(1)
        {
            _aclProcess = aclProcess;
            _fullPath = fullPath;
            _entryType = type;
        }
        protected override object DoJob()
        {
            var aclEntries = _entryType == DirectoryEntryType.DIRECTORY ? _aclProcess.AclEntries : _aclProcess.FileAclEntries;
            if ( _aclProcess.Type== RequestedAclType.SetAcl)
            {
                _aclProcess.Client.SetAcl(_fullPath, aclEntries);
            }
            else if (_aclProcess.Type == RequestedAclType.ModifyAcl)
            {
                _aclProcess.Client.ModifyAclEntries(_fullPath, aclEntries);
            }
            else if (_aclProcess.Type == RequestedAclType.RemoveAcl)
            {
                _aclProcess.Client.RemoveAclEntries(_fullPath, aclEntries);
            }
            return null;
        }

        protected override string JobDetails()
        {
            return $"EntryName: {_fullPath}, EntryType: {_entryType}";
        }

        protected override string JobType()
        {
            return $"AclProcessor.{_aclProcess.Type}";
        }
    }
}
