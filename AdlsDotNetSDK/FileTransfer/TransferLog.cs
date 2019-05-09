using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using Microsoft.Azure.DataLake.Store.QueueTools;

namespace Microsoft.Azure.DataLake.Store.FileTransfer
{
    internal class TransferLog
    {
        private const string FirstLineConst = "ADLS Journal,V1.0";
        private readonly QueueWrapper<string> _recordQueue;

        private readonly string _transferLogFile;
        // Datastructure storing the chunk information of incomplete files and files that are complete
        // The value is null if the file or directory is complete. The value is the metadata structure if the file is incomplete.
        internal Dictionary<string, MetaData> LoadedMetaData;
        // Underlying stream used for read and write streams
        private readonly FileStream _stream;
        private readonly StreamWriter _writeStream;
        private readonly Thread _writeThread;
        internal static char MetaDataDelimiter = '|';
        internal static char MetaDataTerminator = '*';
        private static string FlushIndicator = "|*";
        // MetaDataInfo is appen
        internal TransferLog(bool resume, string transferLogFile, string validateMetaDataInfo)
        {
            if (string.IsNullOrEmpty(transferLogFile))
            {
                throw new ArgumentNullException(nameof(transferLogFile));
            }
            _transferLogFile = transferLogFile;
            Directory.CreateDirectory(Path.GetDirectoryName(transferLogFile));
            try
            {
                _stream = new FileStream(transferLogFile, resume ? FileMode.Open : FileMode.Create,
                    FileAccess.ReadWrite);
            }
            catch (FileNotFoundException)
            {
                throw new ArgumentException("You have selected to resume but the resume file does not exist. There can be number of reasons for this: No transfer has been run before for the given source and destination or the last transfer was successful or temp folder was cleaned up. Please run without resume.");
            }
            if (resume)
            {
                LoadedMetaData = new Dictionary<string, MetaData>();
                LoadFrom(validateMetaDataInfo);
            }
            _recordQueue = new QueueWrapper<string>(-1); // Purposeful-We will close it manually
            _writeStream = new StreamWriter(_stream)
            {
                AutoFlush = true
            };
            if (!resume)
            {
                _writeStream.WriteLine($"{FirstLineConst},{validateMetaDataInfo}");
            }
            else
            {
                // This is a precaution that if the transfer broke before with an incomplete line
                // We will ignore an empty line anyways
                _writeStream.WriteLine();
            }
            _writeThread = new Thread(RunMetaDataWrite)
            {
                Name = "MetaDataWriteThread"
            };
            _writeThread.Start();
        }

        internal void EndRecording(bool closingDueToCancellation)
        {
            _recordQueue.Add(null);
            _writeThread.Join();
            if (!closingDueToCancellation)
            {
                File.Delete(_transferLogFile);
            }
        }
        internal void AddRecord(string entry,bool doFlush = false)
        {
            _recordQueue.Add(entry + MetaDataTerminator);
            if (doFlush)
            {
                _recordQueue.Add(FlushIndicator);
            }
        }
        // LoadedMetaData not null means resume was specified
        internal bool EntryTransferAttemptedLastTime(string entryName)
        {
            return LoadedMetaData != null && LoadedMetaData.ContainsKey(entryName);
        }

        internal bool EntryTransferredSuccessfulLastTime(string entryName)
        {
            return EntryTransferAttemptedLastTime(entryName) && LoadedMetaData[entryName] == null;
        }
        internal bool EntryTransferredIncompleteLastTime(string entryName)
        {
            return EntryTransferAttemptedLastTime(entryName) && LoadedMetaData[entryName] != null;
        }
        // Loads the transfer file containing the log of metadata
        private void LoadFrom(string validateMetaData)
        {
            using (var reader = new StreamReader(_stream, Encoding.UTF8, true, 4096, true))
            {
                string line = reader.ReadLine();
                // Validates the version of the metadata file and current parser is same. This will only change between 
                // version change in sdk.
                if (line == null || !line.Equals($"{FirstLineConst},{validateMetaData}"))
                {
                    throw new ArgumentException("Version line is missing or does not match with current version line. This can happen if the version of the SDK changed between runs or if you changed specification between runs. Please run without resume flag.");
                }
                while ((line = reader.ReadLine()) != null)
                {
                    string validateLine;
                    if (!ValidateMetaData(line, out validateLine))
                    {
                        continue;
                    }
                    string[] entryArr = validateLine.Split(MetaDataDelimiter);
                    string src = entryArr[1];
                    if (entryArr[0].Equals("BEGIN"))
                    {
                        if (!LoadedMetaData.ContainsKey(src))
                        {
                            var metadata = new MetaData
                            {
                                SegmentFolder = entryArr[2],
                                Chunks = new HashSet<int>()
                            };
                            LoadedMetaData.Add(src, metadata);
                        }
                        else if (!entryArr[2].Equals(LoadedMetaData[src].SegmentFolder)) // This will never happen
                        {
                            throw new Exception("Unexpected problem in the resume file. The segment file or folder can never be different");
                        }
                    }
                    else if (entryArr[0].Equals("CHUNK"))
                    {
                        if (LoadedMetaData[src]!=null)
                        {
                            LoadedMetaData[src].Chunks.Add(Int32.Parse(entryArr[2]));
                        }
                    }
                    else if (entryArr[0].Equals("COMPLETE"))
                    {
                        // If complete then store null
                        if (LoadedMetaData.ContainsKey(src)) // Meaning this was a chunked file transfer
                        {
                            LoadedMetaData[src] = null;
                        }
                        else
                        {
                            LoadedMetaData.Add(src, null);
                        }
                    }
                }
            }
        }
        // Validate the 
        private bool ValidateMetaData(string line, out string validateLine)
        {
            validateLine = null;
            if (!string.IsNullOrEmpty(line) && line[line.Length - 1] == MetaDataTerminator)
            {
                validateLine = line.Substring(0, line.Length - 1);
                return true;
            }
            return false;
        }
        private void RunMetaDataWrite()
        {
            while (true)
            {
                var entry = _recordQueue.Poll();
                if (entry == null)
                {
                    break;
                }
                if (entry.Equals(FlushIndicator))
                {
                    _writeStream.Flush();
                }
                else
                {
                    _writeStream.WriteLine(entry);
                }
            }
            _writeStream.Dispose();
        }
    }

    internal class MetaData
    {
        internal string SegmentFolder;
        internal HashSet<int> Chunks;
    }
}
