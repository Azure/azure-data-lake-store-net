using System.Collections.Generic;
using Microsoft.Azure.DataLake.Store.Acl;

namespace Microsoft.Azure.DataLake.Store.FileProperties
{
    internal class PropertyTreeNode
    {
        internal List<PropertyTreeNode> ChildDirectoryNodes { get; set; }
        internal List<PropertyTreeNode> ChildFileNodes { get; set; }
        internal long DepthLevel;
        internal PropertyTreeNode ParentNode { get; set; }

        internal string FullPath;
        internal DirectoryEntryType Type;

        internal long TotChildSize;
        internal long TotChildFiles;
        internal long TotChildDirec;
        internal long DirectChildFiles;
        internal long DirectChildDirec;
        internal long DirectChildSize;

        internal AclStatus Acls;
        // If all childs (dorectpries and files) have same acl as the current node. This makes sense for directory node only
        internal bool AllChildSameAcl;
        // If the parent node has AllChildAclSame true and user wants consistent acl ouput then it set true
        internal bool SkipAclOutput;

        // These are updated by child nodes. These are counters tracking number of children that have reported their properties to their parent
        private long _numchildDirectoryNodesSizeCalculated;
        private long _numChildsAclProcessed;

        private readonly object _lock = new object();
        internal PropertyTreeNode(string fullPath, DirectoryEntryType type, long size, PropertyTreeNode parent, bool calculateFile) : this(fullPath, type, size, parent)
        {
            ChildDirectoryNodes = new List<PropertyTreeNode>();
            if (calculateFile)
            {
                ChildFileNodes = new List<PropertyTreeNode>();
            }
        }

        internal PropertyTreeNode(string fullPath, DirectoryEntryType type, long size, PropertyTreeNode parent)
        {
            FullPath = fullPath;
            Type = type;
            DirectChildSize = TotChildSize = size;
            DepthLevel = parent?.DepthLevel + 1 ?? 0;
            ParentNode = parent;
            ChildDirectoryNodes = ChildFileNodes = null;
            AllChildSameAcl = true;
            _numchildDirectoryNodesSizeCalculated = 0;
            _numChildsAclProcessed = 0;
            SkipAclOutput = false;
        }
        // This can be a directory with no chiildren or a file
        internal bool NoChildren()
        {
            return (ChildDirectoryNodes == null || ChildDirectoryNodes.Count == 0) && (ChildFileNodes == null || ChildFileNodes.Count == 0);
        }

        #region SizeProperty

        private bool CheckAllChildDirectoryNodesCalculated()
        {
            return _numchildDirectoryNodesSizeCalculated >= ChildDirectoryNodes.Count;
        }
        // This is always called by the child nodes - Will return true if all the child directory node sizes are computed
        private bool UpdateNumChildNodeSizeCalculated()
        {
            _numchildDirectoryNodesSizeCalculated++;
            return _numchildDirectoryNodesSizeCalculated >= ChildDirectoryNodes.Count;
        }
        // Updates the current node's disk properties with the child's properties. If all childs have updated, then just return true.
        private bool UpdateNodeSize(long childDirec, long childFiles, long size)
        {
            if (CheckAllChildDirectoryNodesCalculated())
            {
                return true;
            }
            TotChildDirec += childDirec;
            TotChildFiles += childFiles;
            TotChildSize += size;
            return UpdateNumChildNodeSizeCalculated();
        }
        #endregion

        #region AclPoperty
        private bool CheckAllAclChildNodesProcessed()
        {
            return _numChildsAclProcessed >= ChildDirectoryNodes.Count + ChildFileNodes.Count;
        }
        // Updates the number of childs whose acl has been compared
        private bool UpdateNumChildAclProcessed()
        {
            _numChildsAclProcessed++;
            return _numChildsAclProcessed >= (ChildDirectoryNodes.Count + ChildFileNodes.Count);
        }

        private bool CompareAclAndUpdateChildAclProcessed(List<AclEntry> acls, bool childAclSame)
        {

            if (CheckAllAclChildNodesProcessed()) //If acl for all child nodes are computed
            {
                return true;
            }
            bool isAclSame = true;
            if (Acls.Entries.Count == acls.Count)
            {
                HashSet<string> hset = new HashSet<string>();
                foreach (var aclsEntry in Acls.Entries)
                {
                    hset.Add(aclsEntry.ToString());
                }

                foreach (var aclsEntry in acls)
                {
                    if (!hset.Contains(aclsEntry.ToString()))
                    {
                        isAclSame = false;
                        break;
                    }
                }
            }
            else
            {
                isAclSame = false;
            }
            AllChildSameAcl = isAclSame && AllChildSameAcl && childAclSame;
            return UpdateNumChildAclProcessed();
        }
        #endregion
        /// <summary>
        /// This update the Acl property or size proerty or both of the current node. This is called by a child node.
        /// </summary>
        /// <param name="getAclProperty">True if we want the acl as one property</param>
        /// <param name="getSizeProperty">True if we want size as one property</param>
        /// <param name="childNode">Child node that is updating the parent node</param>
        /// <param name="checkBaseCase">Whether it is the base case or whether it is the case when we are going upo the tree</param>
        /// <returns></returns>
        private bool CheckAndUpdateProperties(bool getAclProperty, bool getSizeProperty, PropertyTreeNode childNode, bool checkBaseCase)
        {
            // Whether to compute Acl this turn- when we have reached end of tree i.e. there are no children or if child is a file or if we are recursively moving up
            // Whether to compute size this turn- when we have reached end of tree i.e. there are no children directories or if we are moving recursively up
            bool computeAclThisTurn = getAclProperty && (!checkBaseCase || childNode.NoChildren());
            bool computeSizeThisTurn = getSizeProperty && (!checkBaseCase || (childNode.Type == DirectoryEntryType.DIRECTORY && childNode.NoChildren()));
            if (!(computeAclThisTurn || computeSizeThisTurn)) // If we do not have to compute acl or size property just return
            {
                return false;
            }
            lock (_lock)
            {
                bool allProperty = true;
                // Logic below: One thread updates the size counter or acl counter or both. If a thread is updating acl counter
                // it might not update size counter so after updating acl it needs to check whether the size counters are already complete
                if (getSizeProperty)
                {
                    if (computeSizeThisTurn)
                    {
                        allProperty = UpdateNodeSize(childNode.TotChildDirec, childNode.TotChildFiles, childNode.TotChildSize);
                    }
                    else // This is the case where we are computing acl for a child file, so we do not need to calculate size
                    {// because that is already accounted in the parent directory size
                        allProperty = CheckAllChildDirectoryNodesCalculated();
                    }
                }
                if (getAclProperty)
                {
                    if (computeAclThisTurn)
                    {
                        allProperty = CompareAclAndUpdateChildAclProcessed(childNode.Acls.Entries, childNode.AllChildSameAcl) && allProperty;
                    }
                    else// Currently this will never arise 
                    {
                        allProperty = CheckAllAclChildNodesProcessed() && allProperty;
                    }
                }
                return allProperty;
            }
        }
        /// <summary>
        /// For the current node updates the size or/and acl property of the parent. After this update if the parent's Acl or/and
        /// size properties have accounted all of it's childs then returns true
        /// </summary>
        /// <param name="getAclProperty">Whether we are running this app for acl property</param>
        /// <param name="getSizeProperty">whether we are </param>
        /// <param name="isFirstTurn">Whether we should look at the base case: base case for getAcl is when current node has no children, for getSize when the node should be directory and has no children</param>
        /// <returns>True if all necessary properties are updated by all the childs else false</returns>
        internal bool CheckAndUpdateParentProperties(bool getAclProperty, bool getSizeProperty, bool isFirstTurn)
        {
            return ParentNode.CheckAndUpdateProperties(getAclProperty, getSizeProperty, this, isFirstTurn);
        }

    }
}
