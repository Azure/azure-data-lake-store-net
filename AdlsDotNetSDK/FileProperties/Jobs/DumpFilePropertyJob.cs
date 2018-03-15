

namespace Microsoft.Azure.DataLake.Store.FileProperties.Jobs
{
    // This JOB IS ALWAYS RUN BY ONE THREAD _threadWriter
    internal class DumpFilePropertyJob : BaseJob
    {
        private readonly PropertyManager _manager;
        private readonly PropertyTreeNode _currentNode;

        internal DumpFilePropertyJob(PropertyManager manager, PropertyTreeNode node) : base(node.DepthLevel)
        {
            _manager = manager;
            _currentNode = node;
        }

        protected override object DoJob()
        {
            _manager.WritePropertyTreeNodeToFile(_currentNode);
            return null;
        }

        protected override string JobDetails()
        {
            return "Success";
        }

        protected override string JobType()
        {
            return "FileProperty.WriteProperty";
        }
    }
}
