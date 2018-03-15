using System;
using System.Collections.Generic;
using Microsoft.Azure.DataLake.Store.Acl;
using NLog;

namespace Microsoft.Azure.DataLake.Store.FileProperties
{
    internal class PropertyTreeNode
    {
        /// <summary>
        /// This logging is only for debuging purposes, it will dump huge amount of per-thread data
        /// </summary>
        private static readonly Logger PropertyTreeNodeLog = LogManager.GetLogger("adls.dotnetverbose.PropertyTreeNode");
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
        private long _numChildDirectoryNodesSizeCalculated;
        private long _numChildsAclProcessed;

        internal long GetNumChildsAclProcessed()
        {
            lock (_lock)
            {
                return _numChildsAclProcessed;
            }
        }
        internal long GetNumChildDirectoryProcessed()
        {
            return _numChildDirectoryNodesSizeCalculated;
        }
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
            _numChildDirectoryNodesSizeCalculated = 0;
            _numChildsAclProcessed = 0;
            SkipAclOutput = false;
        }
        // Checks whether the directory has no directory children. File children is ok.
        internal bool NoDirectoryChildren()
        {
            return (ChildDirectoryNodes == null || ChildDirectoryNodes.Count == 0);
        }
        // Checks whether the directory has no children
        internal bool NoChildren()
        {
            return (ChildDirectoryNodes == null || ChildDirectoryNodes.Count == 0) && (ChildFileNodes == null || ChildFileNodes.Count == 0);
        }

        #region SizeProperty

        private bool CheckAllChildDirectoryNodesCalculated()
        {
            return _numChildDirectoryNodesSizeCalculated >= ChildDirectoryNodes.Count;
        }
        
        // Updates the current node's disk properties with the child's properties. If all childs have updated, then just return true.
        private bool UpdateNodeSize(long childDirec, long childFiles, long size)
        {
            if (CheckAllChildDirectoryNodesCalculated())
            {
                // This should never be entered
                throw new Exception($"Size property of Parent: {FullPath} is getting updated more than it should be {ChildDirectoryNodes.Count}");
            }
            TotChildDirec += childDirec;
            TotChildFiles += childFiles;
            TotChildSize += size;
            // Will return true if all the child directory node sizes are computed
            _numChildDirectoryNodesSizeCalculated++;
            return CheckAllChildDirectoryNodesCalculated();
        }
        #endregion

        #region AclPoperty
        private bool CheckAllAclChildNodesProcessed()
        {
            return _numChildsAclProcessed >= ChildDirectoryNodes.Count + ChildFileNodes.Count;
        }
        
        private bool CompareAclAndUpdateChildAclProcessed(List<AclEntry> acls, bool childAclSame)
        {
            if (CheckAllAclChildNodesProcessed())
            {
                // This should never be entered
                throw new Exception($"Acl property of Parent: {FullPath} is getting updated more than it should be {ChildDirectoryNodes.Count + ChildFileNodes.Count}");
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

            // Updates the number of childs whose acl has been compared
            _numChildsAclProcessed++;
            return CheckAllAclChildNodesProcessed();
        }

        #endregion
        /// <summary>
        /// This updates the Acl property or size proerty or both of the current node. This is called by a child node.
        /// If both are getting computed: For a directory we must update them together only. Meaning /Data should update both proerpties of / at the same time
        /// </summary>
        /// <param name="getAclProperty">True if we want the acl as one property</param>
        /// <param name="getSizeProperty">True if we want size as one property</param>
        /// <param name="childNode">Child node that is updating the parent node</param>
        /// <param name="checkBaseCase">Whether it is the base case or whether it is the case when we are going upo the tree</param>
        /// <returns></returns>
        private bool CheckAndUpdateProperties(bool getAclProperty, bool getSizeProperty, PropertyTreeNode childNode, bool checkBaseCase)
        {
            // Whether to compute Acl this turn- when we have reached end of tree i.e. there are no children (files and directories) or if child is a file or if we are recursively moving up
            bool computeAclThisTurn = !checkBaseCase || childNode.NoChildren();

            // Whether to compute size this turn- 1) if we are moving recursively up 2) when we have reached end of tree there are two options: 
            //a) if we are getting Acl then we will only update size if it has no files and directories For ex: childNode is /Data which has few files under it. In this case we cannot update size of / with size of /Data because acl of files under /data is still not computed 
            //b) if acl is not computed then just see if childnode has any directories or not. If it has files we do not need to wait since acl is not getting computed
            bool computeSizeThisTurn = !checkBaseCase || childNode.Type == DirectoryEntryType.DIRECTORY && (getAclProperty ? childNode.NoChildren() : childNode.NoDirectoryChildren());
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
                if (PropertyTreeNodeLog.IsDebugEnabled)
                {
                    PropertyTreeNodeLog.Debug(
                        $"UpdateParentPorperty, allPropertyUpdated: {allProperty}, checkBase: {checkBaseCase}, JobEntryNode: {childNode.FullPath}, ParentNode: {FullPath}{(getSizeProperty ? $", TotChildSizeDone: {GetNumChildDirectoryProcessed()}/{ChildDirectoryNodes.Count}, TotFiles: {TotChildFiles}, TotDirecs: {TotChildDirec}, Totsizes: {TotChildSize}" : string.Empty)}{(getAclProperty ? $", TotChildAclDone: {GetNumChildsAclProcessed()}/{ChildDirectoryNodes.Count + ChildFileNodes.Count}, IsAclSameForAllChilds: {AllChildSameAcl}" : string.Empty)}");
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
