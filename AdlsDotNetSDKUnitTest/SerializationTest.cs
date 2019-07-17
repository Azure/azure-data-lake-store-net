using Microsoft.Azure.DataLake.Store.Serialization;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using System;
using System.IO;
using System.Linq;
using System.Text;

namespace Microsoft.Azure.DataLake.Store.UnitTest
{
    [TestClass]
    public class SerializationTest
    {
        internal void VerifyFileStatus(DirectoryEntry entry, string name, int length, DirectoryEntryType type, DateTime? accessTime, DateTime? modificationTime, DateTime? expireTime, string permission, string group, string owner)
        {
            Assert.IsTrue(entry.Name == name);
            Assert.IsTrue(entry.Type.Equals(type));
            if (accessTime == null)
            {
                Assert.IsNull(entry.LastAccessTime);
            }
            else
            {
                Assert.IsTrue(entry.LastAccessTime.Equals(accessTime));
            }
            if (modificationTime == null)
            {
                Assert.IsNull(entry.LastModifiedTime);
            }
            else
            {
                Assert.IsTrue(entry.LastModifiedTime.Equals(modificationTime));
            }
            if (expireTime == null)
            {
                Assert.IsNull(entry.ExpiryTime);
            }
            else
            {
                Assert.IsTrue(entry.ExpiryTime.Equals(expireTime));
            }
            Assert.IsTrue(permission == entry.Permission);
            Assert.IsTrue(group == entry.Group);
            Assert.IsTrue(owner == entry.User);
        }
        [TestMethod]
        public void TestGetFileStatusSerialization0()
        {
            string fileStatusOutput = "{\"FileStatus\":{\"length\":323,\"pathSuffix\":\"Test02\",\"type\":\"DIRECTORY\",\"blockSize\":0,\"modificationTime\":1528320362596,\"msExpirationTime\":1528320362391,\"replication\":0,\"permission\":\"770\",\"owner\":\"owner1\",\"group\":\"ownergroup1\",\"aclBit\":true}}";
            var jsonSettings = new JsonSerializerSettings();
            jsonSettings.Context = new System.Runtime.Serialization.StreamingContext(System.Runtime.Serialization.StreamingContextStates.All, "ParentDir");
            var entry = JsonCustomConvert.DeserializeObject<DirectoryEntryResult<DirectoryEntry>>(new MemoryStream(Encoding.UTF8.GetBytes(fileStatusOutput)), jsonSettings);
            Assert.IsTrue(entry != null);
            VerifyFileStatus(entry.FileStatus, "Test02", 323, DirectoryEntryType.DIRECTORY, null, DirectoryEntry.GetDateTimeFromServerTime(1528320362596), DirectoryEntry.GetDateTimeFromServerTime(1528320362391), "770", "ownergroup1", "owner1");
        }
   
        [TestMethod]
        public void TestGetFileStatusSerialization1()
        {
            string fileStatusOutput = "{\"FileStatus\":{\"length\":23,\"pathSuffix\":\"Test01\",\"type\":\"DIRECTORY\",\"blockSize\":0,\"modificationTime\":1528320362596,\"msExpirationTime\":1528320362391,\"replication\":0,\"permission\":\"770\",\"owner\":\"owner1\",\"group\":\"ownergroup1\",\"aclBit\":true,\"attributes\":[\"Link\"],\"newperm\":\"sdsds\"}}";
            var jsonSettings = new JsonSerializerSettings();
            jsonSettings.Context = new System.Runtime.Serialization.StreamingContext(System.Runtime.Serialization.StreamingContextStates.All, "ParentDir");
            var entry = JsonCustomConvert.DeserializeObject<DirectoryEntryResult<DirectoryEntry>>(new MemoryStream(Encoding.UTF8.GetBytes(fileStatusOutput)), jsonSettings);
            Assert.IsTrue(entry != null);
            VerifyFileStatus(entry.FileStatus, "Test01", 23, DirectoryEntryType.DIRECTORY, null, DirectoryEntry.GetDateTimeFromServerTime(1528320362596), DirectoryEntry.GetDateTimeFromServerTime(1528320362391), "770", "ownergroup1", "owner1");
        }

        [TestMethod]
        public void TestGetFileStatusSerialization2()
        {
            string fileStatusOutput = "{\"FileStatus\":{\"pathSuffix\":\"Test01\",\"type\":\"DIRECTORY\",\"blockSize\":0,\"modificationTime\":1528320362596,\"replication\":0,\"permission\":\"770\",\"owner\":\"owner1\",\"group\":\"ownergroup1\",\"aclBit\":true,\"attributes\":[\"Link\"],\"newperm\":\"sdsds\"}}";
            var jsonSettings = new JsonSerializerSettings();
            jsonSettings.Context = new System.Runtime.Serialization.StreamingContext(System.Runtime.Serialization.StreamingContextStates.All, "ParentDir");
            var entry = JsonCustomConvert.DeserializeObject<DirectoryEntryResult<DirectoryEntry>>(new MemoryStream(Encoding.UTF8.GetBytes(fileStatusOutput)), jsonSettings);
            Assert.IsTrue(entry != null);
            VerifyFileStatus(entry.FileStatus, "Test01", 0, DirectoryEntryType.DIRECTORY, null, DirectoryEntry.GetDateTimeFromServerTime(1528320362596), null, "770", "ownergroup1", "owner1");
        }

        [TestMethod]
        public void TestGetFileStatusSerialization3()
        {
            string fileStatusOutput = "{\"FileStatus\":{\"length\":23,\"type\":\"DIRECTORY\",\"blockSize\":0,\"modificationTime\":1528320362596,\"replication\":0,\"permission\":\"770\",\"owner\":\"owner1\",\"group\":\"ownergroup1\",\"aclBit\":true,\"attributes\":[\"Link\"],\"newperm\":\"sdsds\"}}";
            var jsonSettings = new JsonSerializerSettings();
            jsonSettings.Context = new System.Runtime.Serialization.StreamingContext(System.Runtime.Serialization.StreamingContextStates.All, "ParentDir");
            var entry = JsonCustomConvert.DeserializeObject<DirectoryEntryResult<DirectoryEntry>>(new MemoryStream(Encoding.UTF8.GetBytes(fileStatusOutput)), jsonSettings);
            Assert.IsTrue(entry != null);
            VerifyFileStatus(entry.FileStatus, null, 23, DirectoryEntryType.DIRECTORY, null, DirectoryEntry.GetDateTimeFromServerTime(1528320362596), null, "770", "ownergroup1", "owner1");
        }

        [TestMethod]
        public void TestGetFileStatusSerialization4()
        {
            string fileStatusOutput = "{\"FileStatus\":{\"length\":23,\"type\":\"DIRECTORY\",\"blockSize\":0,\"modificationTime\":1528320362596,\"replication\":0,\"owner\":\"owner1\",\"group\":\"ownergroup1\",\"aclBit\":true,\"attributes\":[\"Link\"],\"newperm\":\"sdsds\"}}";
            var jsonSettings = new JsonSerializerSettings();
            jsonSettings.Context = new System.Runtime.Serialization.StreamingContext(System.Runtime.Serialization.StreamingContextStates.All, "ParentDir");
            var entry = JsonCustomConvert.DeserializeObject<DirectoryEntryResult<DirectoryEntry>>(new MemoryStream(Encoding.UTF8.GetBytes(fileStatusOutput)), jsonSettings);
            Assert.IsTrue(entry != null);
            VerifyFileStatus(entry.FileStatus, null, 23, DirectoryEntryType.DIRECTORY, null, DirectoryEntry.GetDateTimeFromServerTime(1528320362596), null, null, "ownergroup1", "owner1");
        }

        [TestMethod]
        public void TestGetFileStatusSerialization5()
        {
            string fileStatusOutput = "{\"FileStatus\":{\"length\":23,\"type\":\"DIRECTORY\",\"blockSize\":0,\"replication\":0,\"owner\":\"owner1\",\"group\":\"ownergroup1\",\"aclBit\":true,\"attributes\":[\"Link\"],\"newperm\":\"sdsds\"}}";
            var jsonSettings = new JsonSerializerSettings();
            jsonSettings.Context = new System.Runtime.Serialization.StreamingContext(System.Runtime.Serialization.StreamingContextStates.All, "ParentDir");
            var entry = JsonCustomConvert.DeserializeObject<DirectoryEntryResult<DirectoryEntry>>(new MemoryStream(Encoding.UTF8.GetBytes(fileStatusOutput)), jsonSettings);
            Assert.IsTrue(entry != null);
            VerifyFileStatus(entry.FileStatus, null, 23, DirectoryEntryType.DIRECTORY, null, null, null, null, "ownergroup1", "owner1");
        }

        [TestMethod]
        public void TestGetFileStatusSerialization6()
        {
            string fileStatusOutput = "{\"FileStatus\":{\"type\":\"DIRECTORY\",\"blockSize\":0,\"accessTime\":1528320290048,\"modificationTime\":1528320362596,\"replication\":0,\"owner\":\"owner1\",\"group\":\"ownergroup1\",\"aclBit\":true,\"attributes\":[\"Link\"],\"newperm\":\"sdsds\"}}";
            var jsonSettings = new JsonSerializerSettings();
            jsonSettings.Context = new System.Runtime.Serialization.StreamingContext(System.Runtime.Serialization.StreamingContextStates.All, "ParentDir");
            var entry = JsonCustomConvert.DeserializeObject<DirectoryEntryResult<DirectoryEntry>>(new MemoryStream(Encoding.UTF8.GetBytes(fileStatusOutput)), jsonSettings);
            Assert.IsTrue(entry != null);
            VerifyFileStatus(entry.FileStatus, null, 0, DirectoryEntryType.DIRECTORY, DirectoryEntry.GetDateTimeFromServerTime(1528320290048), DirectoryEntry.GetDateTimeFromServerTime(1528320362596), null, null, "ownergroup1", "owner1");
        }

        [TestMethod]
        public void TestListFileStatusSerialization1()
        {
            string liststatusOutput = "{\"FileStatuses\":{\"FileStatus\":[{\"length\":0,\"pathSuffix\":\"Test01\",\"type\":\"DIRECTORY\",\"blockSize\":0,\"accessTime\":1528320290048,\"modificationTime\":1528320362596,\"msExpirationTime\":1528320362391,\"replication\":0,\"permission\":\"770\",\"owner\":\"owner1\",\"group\":\"ownergroup1\",\"aclBit\":true},{\"length\":0,\"pathSuffix\":\"Test02\",\"type\":\"DIRECTORY\",\"blockSize\":0,\"accessTime\":1531515372559,\"modificationTime\":1531523888360,\"replication\":0,\"owner\":\"owner2\",\"group\":\"ownergroup2\",\"aclBit\":true,\"attributes\":[\"Link\"]}]}}";
            var jsonSettings = new JsonSerializerSettings();
            jsonSettings.Context = new System.Runtime.Serialization.StreamingContext(System.Runtime.Serialization.StreamingContextStates.All, "ParentDir");

            var entry = JsonCustomConvert.DeserializeObject<DirectoryEntryListResult<DirectoryEntry>>(new MemoryStream(Encoding.UTF8.GetBytes(liststatusOutput)), jsonSettings);
            Assert.IsNotNull(entry);
            Assert.IsNotNull(entry.FileStatuses.FileStatus);
            Assert.IsTrue(entry.FileStatuses.FileStatus.Count == 2);
            VerifyFileStatus(entry.FileStatuses.FileStatus[0], "Test01", 0, DirectoryEntryType.DIRECTORY, DirectoryEntry.GetDateTimeFromServerTime(1528320290048), DirectoryEntry.GetDateTimeFromServerTime(1528320362596), DirectoryEntry.GetDateTimeFromServerTime(1528320362391), "770", "ownergroup1", "owner1");
            VerifyFileStatus(entry.FileStatuses.FileStatus[1], "Test02", 0, DirectoryEntryType.DIRECTORY, DirectoryEntry.GetDateTimeFromServerTime(1531515372559), DirectoryEntry.GetDateTimeFromServerTime(1531523888360), null, null, "ownergroup2", "owner2");
        }

        [TestMethod]
        public void TestListFileStatusSerialization2()
        {
            string liststatusOutput = "{\"FileStatuses\":{\"FileStatus\":[{\"length\":0,\"pathSuffix\":\"Test01\",\"type\":\"DIRECTORY\",\"blockSize\":0,\"accessTime\":1528320290048,\"modificationTime\":1528320362596,\"msExpirationTime\":1528320362391,\"replication\":0,\"permission\":\"770\",\"owner\":\"owner1\",\"group\":\"ownergroup1\",\"aclBit\":true},{\"length\":0,\"pathSuffix\":\"Test02\",\"type\":\"DIRECTORY\",\"blockSize\":0,\"accessTime\":1531515372559,\"modificationTime\":1531523888360,\"replication\":0,\"owner\":\"owner2\",\"group\":\"ownergroup2\",\"aclBit\":true,\"attributes\":[\"Link\"]},{\"length\":0,\"pathSuffix\":\"Test03\",\"type\":\"FILE\",\"blockSize\":0,\"accessTime\":1531515372559,\"replication\":0,\"owner\":\"owner3\",\"aclBit\":true}]}}";
            var jsonSettings = new JsonSerializerSettings();
            jsonSettings.Context = new System.Runtime.Serialization.StreamingContext(System.Runtime.Serialization.StreamingContextStates.All, "ParentDir");

            var entry = JsonCustomConvert.DeserializeObject<DirectoryEntryListResult<DirectoryEntry>>(new MemoryStream(Encoding.UTF8.GetBytes(liststatusOutput)), jsonSettings);
            Assert.IsNotNull(entry);
            Assert.IsNotNull(entry.FileStatuses.FileStatus);
            Assert.IsTrue(entry.FileStatuses.FileStatus.Count == 3);
            VerifyFileStatus(entry.FileStatuses.FileStatus[0], "Test01", 0, DirectoryEntryType.DIRECTORY, DirectoryEntry.GetDateTimeFromServerTime(1528320290048), DirectoryEntry.GetDateTimeFromServerTime(1528320362596), DirectoryEntry.GetDateTimeFromServerTime(1528320362391), "770", "ownergroup1", "owner1");
            VerifyFileStatus(entry.FileStatuses.FileStatus[1], "Test02", 0, DirectoryEntryType.DIRECTORY, DirectoryEntry.GetDateTimeFromServerTime(1531515372559), DirectoryEntry.GetDateTimeFromServerTime(1531523888360), null, null, "ownergroup2", "owner2");
            VerifyFileStatus(entry.FileStatuses.FileStatus[2], "Test03", 0, DirectoryEntryType.FILE, DirectoryEntry.GetDateTimeFromServerTime(1531515372559), null, null, null, null, "owner3");
        }

        [TestMethod]
        public void TestListFileStatusSerialization3()
        {
            string liststatusOutput = "{\"FileStatuses\":{\"FileStatus\":[{\"length\":0,\"pathSuffix\":\"Test01\",\"type\":\"DIRECTORY\",\"blockSize\":0,\"accessTime\":-922337203685477,\"modificationTime\":-922337203685477,\"msExpirationTime\":1528320362391,\"replication\":0,\"permission\":\"770\",\"owner\":\"owner1\",\"group\":\"ownergroup1\",\"aclBit\":true}," +
                "{\"length\":0,\"pathSuffix\":\"Test02\",\"type\":\"DIRECTORY\",\"blockSize\":0,\"accessTime\":1531515372559,\"modificationTime\":1531523888360,\"replication\":0,\"owner\":\"owner2\",\"group\":\"ownergroup2\",\"aclBit\":true,\"attributes\":[\"Link\"]}]}}";
            var jsonSettings = new JsonSerializerSettings();
            jsonSettings.Context = new System.Runtime.Serialization.StreamingContext(System.Runtime.Serialization.StreamingContextStates.All, "ParentDir");

            var entry = JsonCustomConvert.DeserializeObject<DirectoryEntryListResult<DirectoryEntry>>(new MemoryStream(Encoding.UTF8.GetBytes(liststatusOutput)), jsonSettings);
            Assert.IsNotNull(entry);
            Assert.IsNotNull(entry.FileStatuses.FileStatus);
            Assert.IsTrue(entry.FileStatuses.FileStatus.Count == 2);
            VerifyFileStatus(entry.FileStatuses.FileStatus[0], "Test01", 0, DirectoryEntryType.DIRECTORY, null, null, DirectoryEntry.GetDateTimeFromServerTime(1528320362391), "770", "ownergroup1", "owner1");
            VerifyFileStatus(entry.FileStatuses.FileStatus[1], "Test02", 0, DirectoryEntryType.DIRECTORY, DirectoryEntry.GetDateTimeFromServerTime(1531515372559), DirectoryEntry.GetDateTimeFromServerTime(1531523888360), null, null, "ownergroup2", "owner2");
        }
        [TestMethod]
        public void TestTrashEnumerateSerialization1()
        {
            string trashOutput = "{\"trashDir\":{\"trashDirEntry\":[{\"type\":\"FILE\",\"creationTime\":1548266625223,\"originalPath\":\"adl://test.azuredatalake.com/Testf1c36522-02a1-4fae-86e7-72eb396e6e05/file_qapQg9Z1E1ZIGGP7.txt_file_3f4KjaewFTgnePx0.txt\",\"trashDirPath\":\"02b3f334-a6af-47f4-80ca-acff0464f324\"},{\"type\":\"FILE\",\"creationTime\":1548266623692,\"originalPath\":\"adl://test.azuredatalake.com/Testf1c36522-02a1-4fae-86e7-72eb396e6e05/file_qapQg9Z1E1ZIGGP7.txt_file_DRuXVefrpukWAuIs.txt\",\"trashDirPath\":\"adl://test.azuredatalake.com/$temp/trash/131926752000000000/bn6sch104331623/deleted_feace1a1-ce0f-4576-82da-a2427aaf35b3\"}],\"nextListAfter\":\"something\"}}";
            var jsonSettings = new JsonSerializerSettings();
            jsonSettings.Context = new System.Runtime.Serialization.StreamingContext(System.Runtime.Serialization.StreamingContextStates.All, "ParentDir");

            var entry = JsonCustomConvert.DeserializeObject<TrashStatusResult>(new MemoryStream(Encoding.UTF8.GetBytes(trashOutput)), jsonSettings);
            Assert.IsTrue(entry.TrashStatusRes.NumSearched == 0);
            Assert.IsTrue(!string.IsNullOrEmpty(entry.TrashStatusRes.NextListAfter));
        }

        [TestMethod]
        public void TestTrashEnumerateSerialization2()
        {
            string trashOutput = "{\"trashDir\":{\"trashDirEntry\":[{\"type\":\"FILE\",\"creationTime\":1548266625223,\"originalPath\":\"adl://test.azuredatalake.com/Testf1c36522-02a1-4fae-86e7-72eb396e6e05/file_qapQg9Z1E1ZIGGP7.txt_file_3f4KjaewFTgnePx0.txt\",\"trashDirPath\":\"02b3f334-a6af-47f4-80ca-acff0464f324\"}]}}";
            var jsonSettings = new JsonSerializerSettings();
            jsonSettings.Context = new System.Runtime.Serialization.StreamingContext(System.Runtime.Serialization.StreamingContextStates.All, "ParentDir");

            var entry = JsonCustomConvert.DeserializeObject<TrashStatusResult>(new MemoryStream(Encoding.UTF8.GetBytes(trashOutput)), jsonSettings);
            Assert.IsTrue(entry.TrashStatusRes.NumSearched == 0);
            Assert.IsTrue(string.IsNullOrEmpty(entry.TrashStatusRes.NextListAfter));
        }

        [TestMethod]
        public void TestTrashEnumerateSerialization3()
        {
            string trashOutput = "{\"trashDir\":{\"trashDirEntry\":[{\"type\":\"FILE\",\"creationTime\":1548266625223,\"originalPath\":\"adl://test.azuredatalake.com/Testf1c36522-02a1-4fae-86e7-72eb396e6e05/file_qapQg9Z1E1ZIGGP7.txt_file_3f4KjaewFTgnePx0.txt\",\"trashDirPath\":\"02b3f334-a6af-47f4-80ca-acff0464f324\"},{\"type\":\"FILE\",\"creationTime\":1548266623692,\"originalPath\":\"adl://test.azuredatalake.com/Testf1c36522-02a1-4fae-86e7-72eb396e6e05/file_qapQg9Z1E1ZIGGP7.txt_file_DRuXVefrpukWAuIs.txt\"}],\"numSearched\":\"23\"}}";
            var jsonSettings = new JsonSerializerSettings();
            jsonSettings.Context = new System.Runtime.Serialization.StreamingContext(System.Runtime.Serialization.StreamingContextStates.All, "ParentDir");

            var entry = JsonCustomConvert.DeserializeObject<TrashStatusResult>(new MemoryStream(Encoding.UTF8.GetBytes(trashOutput)), jsonSettings);
            Assert.IsTrue(entry.TrashStatusRes.NumSearched == 23);
            Assert.IsTrue(string.IsNullOrEmpty(entry.TrashStatusRes.NextListAfter));
        }
        [TestMethod]
        public void TestEnumerateWithLinkSerialization1()
        {
            string liststatusOutput = "{\"FileStatuses\":{\"FileStatus\":[{\"length\":0,\"pathSuffix\":\"Test01\",\"type\":\"DIRECTORY\",\"blockSize\":0,\"accessTime\":1528320290048,\"modificationTime\":1528320362596,\"msExpirationTime\":1528320362391,\"replication\":0,\"permission\":\"770\",\"owner\":\"owner1\",\"group\":\"ownergroup1\",\"aclBit\":true}," +
                "{\"length\":0,\"pathSuffix\":\"Test02\",\"type\":\"DIRECTORY\",\"blockSize\":0,\"accessTime\":1531515372559,\"modificationTime\":1531523888360,\"replication\":0,\"owner\":\"owner2\",\"group\":\"ownergroup2\",\"aclBit\":true,\"attributes\":[\"Link\"]}," +
                "{\"length\":0,\"pathSuffix\":\"Test03\",\"type\":\"FILE\",\"blockSize\":0,\"accessTime\":1531515372559,\"replication\":0,\"owner\":\"owner3\",\"aclBit\":true}]}}";
            var jsonSettings = new JsonSerializerSettings();
            jsonSettings.Context = new System.Runtime.Serialization.StreamingContext(System.Runtime.Serialization.StreamingContextStates.All, "ParentDir");

            var entry = JsonCustomConvert.DeserializeObject<DirectoryEntryListResult<DirectoryEntry>>(new MemoryStream(Encoding.UTF8.GetBytes(liststatusOutput)), jsonSettings);
            var entryList = entry.FileStatuses.FileStatus;
            Assert.IsTrue(entryList.Count == 3);
            Assert.IsTrue(entryList[0].Attribute == null);
            Assert.IsTrue(entryList[1].Attribute.Count == 1);
            Assert.IsTrue(entryList[1].Attribute != null && entryList[1].Attribute.Any(attr => attr == DirectoryEntryAttributeType.Link));
        }
    }
}
