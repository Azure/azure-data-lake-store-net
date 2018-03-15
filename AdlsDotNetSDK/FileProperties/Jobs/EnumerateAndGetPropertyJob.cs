namespace Microsoft.Azure.DataLake.Store.FileProperties.Jobs
{
    internal class EnumerateAndGetPropertyJob : BaseJob
    {
        private readonly PropertyTreeNode _currentNode;
        private readonly PropertyManager _manager;
        // For this job if the parent node has all properties updated then we keep going up, this variable stores the last parent that got updated
        protected override object DoJob()
        {
            if (_manager.GetAclProperty)
            {
                _currentNode.Acls = _manager.Client.GetAclStatus(_currentNode.FullPath);
            }
            if (_currentNode.Type == DirectoryEntryType.DIRECTORY)
            {

                var fop = _manager.Client.EnumerateDirectory(_currentNode.FullPath);

                foreach (var dir in fop)
                {
                    if (dir.Type == DirectoryEntryType.DIRECTORY)
                    {
                        _currentNode.ChildDirectoryNodes.Add(new PropertyTreeNode(dir.FullName, dir.Type, dir.Length,
                            _currentNode, _manager.GetAclProperty || _manager.DisplayFiles));
                        _currentNode.DirectChildDirec++;
                    }
                    else
                    {
                        _currentNode.DirectChildSize += dir.Length;
                        _currentNode.DirectChildFiles++;
                        // We need to add files to list only if user has specified DisplayFiles
                        if (_manager.DisplayFiles)
                        {
                            _currentNode.ChildFileNodes.Add(
                                new PropertyTreeNode(dir.FullName, dir.Type, dir.Length, _currentNode));
                        }
                    }
                }
                if (_manager.GetSizeProperty)
                {
                    _currentNode.TotChildSize += _currentNode.DirectChildSize;
                    _currentNode.TotChildDirec += _currentNode.DirectChildDirec;
                    _currentNode.TotChildFiles += _currentNode.DirectChildFiles;
                }
                // Add the jobs for child nodes after we have enumerated all the sub-directories to be threadsafe
                foreach (var childNode in _currentNode.ChildDirectoryNodes)
                {
                    _manager.ConsumerQueue.Add(new EnumerateAndGetPropertyJob(childNode, _manager));
                }
                // REMEMBER if user has not specified DISPLAYFILES we would not compute ACL for the file
                // _currentNode.ChildFileNodes will be empty if user has not passed DisplayFiles
                // Reason is there can be millions of files and there will be unnecessary delay in getting Acls even though user does not want acl of files
                if (_manager.GetAclProperty)
                {
                    foreach (var childNode in _currentNode.ChildFileNodes)
                    {
                        _manager.ConsumerQueue.Add(new EnumerateAndGetPropertyJob(childNode, _manager));
                    }
                }
                // If it is the root node
                if (_currentNode.DepthLevel == 0)
                {
                    // For only disk usage (no getacl) if no subdirectories then end
                    // for getacl if there are no sub directories and sub files then only end
                    if (!_manager.GetAclProperty && _currentNode.ChildDirectoryNodes.Count == 0 || (_manager.GetAclProperty && _currentNode.NoChildren()))
                    {
                        _manager.ConsumerQueue.Add(new PoisonJob());
                    }
                    return null;
                }

            }
            UpdateParentProperty(_currentNode, true);
            return null;
        }

        // Updates the parent properties- size or acl or both. If all required properties have been updated for the parent then recursively move up the tree and keep doing the same 
        private void UpdateParentProperty(PropertyTreeNode currentNode, bool firstTurn)
        {
            if (currentNode.CheckAndUpdateParentProperties(_manager.GetAclProperty, _manager.GetSizeProperty, firstTurn))
            {
                // Everything below currentNode.ParentNode is done- now put job for dumping
                EnqueueWritingJobForAllChilds(currentNode.ParentNode);
                if (currentNode.ParentNode.DepthLevel == 0)
                {
                    _manager.ConsumerQueue.Add(new PoisonJob());
                }
                else
                {
                    UpdateParentProperty(currentNode.ParentNode, false);
                }
            }
        }
        // Once all childs have updated the parent properties, put jobs for dumping the properties of the childs of that parent
        private void EnqueueWritingJobForAllChilds(PropertyTreeNode currentNode)
        {
            try
            {
                if (currentNode.DepthLevel > _manager.MaxDepth - 1)
                {
                    return;
                }
                // Do not show the children's acl information since parent has a consistent acl and user want to see consistent acl only
                bool skipChildAclOuput = _manager.GetAclProperty && _manager.HideConsistentAclTree && currentNode.AllChildSameAcl;
                // If we are viewing acl only then no need to show the children since parent has consistent acl
                if (skipChildAclOuput && !_manager.GetSizeProperty)
                {
                    return;
                }
                foreach (var dirNode in currentNode.ChildDirectoryNodes)
                {
                    dirNode.SkipAclOutput = skipChildAclOuput;
                    _manager.PropertyWriterQueue.Add(new DumpFilePropertyJob(_manager, dirNode));
                }
                
                if (_manager.DisplayFiles)
                {
                    foreach (var fileNode in currentNode.ChildFileNodes)
                    {
                        fileNode.SkipAclOutput = skipChildAclOuput;
                        _manager.PropertyWriterQueue.Add(new DumpFilePropertyJob(_manager, fileNode));
                    }
                }
            }
            finally
            {
                if (!_manager.DontDeleteChildNodes)
                {
                    currentNode.ChildDirectoryNodes = null;
                    currentNode.ChildFileNodes = null;
                }
            }
        }

        protected override string JobDetails()
        {

            return $"EntryName: {_currentNode.FullPath}, EntryType: {_currentNode.Type}, DirectChildFiles: {_currentNode.DirectChildFiles}, DirectChildDirectories: {_currentNode.DirectChildDirec}, DirectChildSize: {_currentNode.DirectChildSize}{(_currentNode.Acls != null ? $", Acls: {string.Join(":", _currentNode.Acls.Entries)}" : string.Empty)}";
        }

        protected override string JobType()
        {
            return "FileProperty.EnumerateAndGetProperty";
        }

        internal EnumerateAndGetPropertyJob(PropertyTreeNode node, PropertyManager manager) : base((node.Type == DirectoryEntryType.DIRECTORY ? 1 << 31 : 0) + node.DepthLevel)
        {
            _currentNode = node;
            _manager = manager;
        }
    }
}
