using System.IO;

namespace Microsoft.Azure.DataLake.Store
{
    internal class Utils
    {
        internal static void CreateParentDirectory(string path)
        {
            string directoryName = Path.GetDirectoryName(path);
            if (!string.IsNullOrEmpty(directoryName))
            {
                Directory.CreateDirectory(directoryName);
            }
        }
    }
}
