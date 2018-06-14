using System;
using System.IO;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Azure.DataLake.Store;
using Microsoft.Azure.DataLake.Store.FileTransfer;


[assembly: InternalsVisibleTo("Microsoft.Azure.DataLake.Store.UnitTest, PublicKey=" +
                              "0024000004800000940000000602000000240000525341310004000001000100b5fc90e7027f67" +
                              "871e773a8fde8938c81dd402ba65b9201d60593e96c492651e889cc13f1415ebb53fac1131ae0b" +
                              "d333c5ee6021672d9718ea31a8aebd0da0072f25d87dba6fc90ffd598ed4da35e44c398c454307" +
                              "e8e33b8426143daec9f596836f97c8f74750e5975c64e2189f45def46b2a2b1247adc3652bf5c3" +
                              "08055da9")]
namespace TestDataCreator
{
    
    internal class DataCreator
    {
        private static readonly Random Random = new Random();
        internal static long BuffSize = 4 * 1024 * 1024;

        internal class State
        {
            internal string Path;
            internal bool IsLocal;
            internal long FileLength;
            internal bool WriteInNewLines;
            internal AdlsClient Client;

            internal State(string ph, long fLength, bool writeNewLines, bool local = true, AdlsClient ct = null)
            {
                Path = ph;
                FileLength = fLength;
                WriteInNewLines = writeNewLines;
                IsLocal = local;
                Client = ct;
            }
        }

        private static void Run(object state)
        {
            var st = state as State;
            if (st.IsLocal)
            {
                using (var ostream = new StreamWriter(new FileStream(st.Path, FileMode.Create, FileAccess.Write), Encoding.UTF8))
                {
                    if (st.FileLength > 0)
                    {
                        long lengthToRead = st.FileLength;
                        using (Stream rndStream = new RandomDataStream(st.FileLength))
                        {
                            byte[] readBytes = new byte[BuffSize];
                            while (lengthToRead > 0)
                            {
                                int bytesRead = rndStream.Read(readBytes, 0, (int)Math.Min(BuffSize, lengthToRead));

                                // Break when the end of the file is reached.
                                if (bytesRead > 0)
                                {
                                    if (st.WriteInNewLines)
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
            else
            {
                using (var ostream = st.Client.CreateFile(st.Path, IfExists.Overwrite))
                {
                    if (st.FileLength> 0)
                    {
                        long lengthToRead = st.FileLength;
                        using (Stream rndStream = new RandomDataStream(st.FileLength))
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
        }
        internal static void CreateDirRecursiveLocal(string path, int recursLevel, int noDirEntries, int lowFileEntries, int highFileEntries, int lowStringLength, int highStringLength, string filePrefix = "", bool writeInNewLines = false)
        {
            MultiThreadedRunner<State> inst = new MultiThreadedRunner<State>(6, Run);
            CreateDirRecursiveLocal(inst, path, recursLevel, noDirEntries, lowFileEntries, highFileEntries,
                lowStringLength, highStringLength,filePrefix,writeInNewLines);
            inst.RunMultiThreaded();
            inst.StopMultiThreaded();
        }
        internal static void CreateDirRecursiveLocal(MultiThreadedRunner<State> inst, string path, int recursLevel, int noDirEntries, int lowFileEntries, int highFileEntries, int lowStringLength, int highStringLength, string filePrefix = "", bool writeInNewLines = false)
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
                inst.AddToQueue(new State(path + "\\" + nextLevel + filePrefix + i + "File.txt",stringLength,writeInNewLines));
            }
            nextLevel++;
            string newPath = path + "\\";
            for (int i = 0; i < noDirEntries; i++)
            {
                CreateDirRecursiveLocal(inst,newPath + nextLevel + i, recursLevel - 1, noDirEntries, lowFileEntries, highFileEntries, lowStringLength, highStringLength, filePrefix, writeInNewLines);
            }
        }

        internal static void CreateDirRecursiveRemote(AdlsClient client, string path, int recursLevel, int noDirEntries,
            int lowFileEntries, int highFileEntries, int lowStringLength, int highStringLength,
            bool keepBottomLevelFolderEmpty = false, string filePrefix = "")
        {
            MultiThreadedRunner<State> inst = new MultiThreadedRunner<State>(6, Run);
            CreateDirRecursiveRemote(inst, client, path, recursLevel, noDirEntries, lowFileEntries, highFileEntries,
                lowStringLength, highStringLength, keepBottomLevelFolderEmpty, filePrefix);
            inst.RunMultiThreaded();
            inst.StopMultiThreaded();
        }

        internal static void CreateDirRecursiveRemote(MultiThreadedRunner<State> inst,AdlsClient client, string path, int recursLevel, int noDirEntries, int lowFileEntries, int highFileEntries, int lowStringLength, int highStringLength, bool keepBottomLevelFolderEmpty = false, string filePrefix = "")
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
                inst.AddToQueue(new State(path + "/" + nextLevel + filePrefix + i + "File.txt",stringLength,false,false,client));
            }
            if (recursLevel == 0)
            {
                return;
            }
            nextLevel++;
            string newPath = path + "/";
            for (int i = 0; i < noDirEntries; i++)
            {
                CreateDirRecursiveRemote(inst,client, newPath + nextLevel + i, recursLevel - 1, noDirEntries, lowFileEntries, highFileEntries, lowStringLength, highStringLength, keepBottomLevelFolderEmpty, filePrefix);
            }
        }

        internal static void DeleteRecursiveLocal(DirectoryInfo dir)
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
