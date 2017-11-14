using System;
using System.IO;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Azure.DataLake.Store;
using Microsoft.Azure.DataLake.Store.FileTransfer;
[assembly: InternalsVisibleTo("Microsoft.Azure.DataLake.Store.UnitTest")]
namespace TestDataCreator
{
    internal class DataCreator
    {
        private static readonly Random Random = new Random();
        internal static long BuffSize = 4 * 1024 * 1024;
        internal static void CreateDirRecursiveLocal(string path, int recursLevel, int noDirEntries, int lowFileEntries, int highFileEntries, int lowStringLength, int highStringLength, string filePrefix = "", bool writeInNewLines = false)
        {
            Directory.CreateDirectory(path);
            if (recursLevel == 0)
            {
                return;
            }
            string[] str = path.Split('\\');
            char nextLevel = str[str.Length - 1][0];
            int noFileEntries = Random.Next(lowFileEntries, highFileEntries);
            for (int i = 0; i < noFileEntries; i++)
            {
                int stringLength = (Random.Next(lowStringLength, highStringLength));
                using (var ostream = new StreamWriter(new FileStream(path + "\\" + nextLevel + filePrefix + i + "File.txt", FileMode.Create, FileAccess.Write), Encoding.UTF8))
                {
                    if (stringLength > 0)
                    {
                        int lengthToRead = stringLength;
                        using (Stream rndStream = new RandomDataStream(stringLength))
                        {
                            byte[] readBytes = new byte[BuffSize];
                            while (lengthToRead > 0)
                            {
                                int bytesRead = rndStream.Read(readBytes, 0, Math.Min((int)BuffSize, lengthToRead));

                                // Break when the end of the file is reached.
                                if (bytesRead > 0)
                                {
                                    if (writeInNewLines)
                                    {
                                        ostream.WriteLine(Encoding.UTF8.GetString(readBytes, 0, bytesRead));
                                    }
                                    else
                                    {
                                        ostream.Write(Encoding.UTF8.GetString(readBytes, 0, bytesRead));
                                    }
                                }
                                else
                                {
                                    break;
                                }
                                lengthToRead -= bytesRead;
                            }
                        }

                    }
                }
            }
            nextLevel++;
            string newPath = path + "\\";
            for (int i = 0; i < noDirEntries; i++)
            {
                CreateDirRecursiveLocal(newPath + nextLevel + i, recursLevel - 1, noDirEntries, lowFileEntries, highFileEntries, lowStringLength, highStringLength, filePrefix, writeInNewLines);
            }
        }
        internal static void CreateDirRecursiveRemote(AdlsClient client, string path, int recursLevel, int noDirEntries, int lowFileEntries, int highFileEntries, int lowStringLength, int highStringLength, bool keepBottomLevelFolderEmpty = false, string filePrefix = "")
        {
            client.CreateDirectory(path);
            if (recursLevel == 0 && keepBottomLevelFolderEmpty)
            {
                return;
            }
            string[] str = path.Split('/');
            char nextLevel = str[str.Length - 1][0];
            int noFileEntries = Random.Next(lowFileEntries, highFileEntries);
            for (int i = 0; i < noFileEntries; i++)
            {
                long stringLength = (Random.Next(lowStringLength, highStringLength));
                using (var ostream = client.CreateFile(path + "/" + nextLevel + filePrefix + i + "File.txt", IfExists.Overwrite))
                {
                    if (stringLength > 0)
                    {
                        long lengthToRead = stringLength;
                        using (Stream rndStream = new RandomDataStream(stringLength))
                        {
                            byte[] readBytes = new byte[BuffSize];
                            while (lengthToRead > 0)
                            {
                                int bytesRead = rndStream.Read(readBytes, 0, (int)Math.Min(BuffSize, lengthToRead));

                                // Break when the end of the file is reached.
                                if (bytesRead > 0)
                                {
                                    ostream.Write(readBytes, 0, bytesRead);
                                }
                                else
                                {
                                    break;
                                }

                                lengthToRead -= bytesRead;
                            }
                        }

                    }
                }
            }
            if (recursLevel == 0)
            {
                return;
            }
            nextLevel++;
            string newPath = path + "/";
            for (int i = 0; i < noDirEntries; i++)
            {
                CreateDirRecursiveRemote(client, newPath + nextLevel + i, recursLevel - 1, noDirEntries, lowFileEntries, highFileEntries, lowStringLength, highStringLength, keepBottomLevelFolderEmpty, filePrefix);
            }
        }

        public static void DeleteRecursiveLocal(DirectoryInfo dir)
        {

            IEnumerable<DirectoryInfo> enumDir = dir.EnumerateDirectories();
            foreach (var subDir in enumDir)
            {
                DeleteRecursiveLocal(subDir);
            }

            IEnumerable<FileInfo> enumFiles = dir.EnumerateFiles();
            foreach (var file in enumFiles)
            {
                file.Delete();
            }
            dir.Delete();
        }
        static void Main(string[] args)
        {
            CreateDirRecursiveLocal("D:\\Data\\rdutta\\B", 3, 4, 3, 5, 128, 680);
        }
    }
}
