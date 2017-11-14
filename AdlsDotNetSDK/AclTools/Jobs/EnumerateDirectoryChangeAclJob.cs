
namespace Microsoft.Azure.DataLake.Store.AclTools.Jobs
{
    internal class EnumerateDirectoryChangeAclJob : BaseJob
    {
        private readonly AclProcessor _aclProcess;
        private readonly string _fullPath;
        internal EnumerateDirectoryChangeAclJob(AclProcessor aclProcess,string fullPath):base(2)
        {
            _aclProcess = aclProcess;
            _fullPath = fullPath;
        }
        protected override object DoJob()
        {
            foreach (var dir in _aclProcess.Client.EnumerateDirectory(_fullPath))
            {
                _aclProcess.ProcessDirectoryEntry(dir);
            }
            return null;
        }

        protected override string JobDetails()
        {
            return $"Directory: {_fullPath}";
        }

        protected override string JobType()
        {
            return "AclProcessor.Enumerate";
        }
    }
}
