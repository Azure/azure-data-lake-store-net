# Changes to the SDK
### Version 1.1.4
- Fix FileProperties bug for disk usage and acl usage for files
- Internal changes to logging
- Add Microsoft license and icon url for nuget in csproj
### Version 1.1.3
- Internal changes.
- Fix some unit tests
### Version 1.1.2
- Internal changes only.
### Version 1.1.1
- Internal changes only
### Version 1.1.0
- Add query parameter getConsistentLength to getfilestatus to get consistent length and pass it for open and append
- Fix retrieving x-ms-requestid for remote exception
- [Internal Only] In Core pass opcode as string to webtransport instead of enum
- Fix the accessibility of Operation class 
- For FileProperties add entry type column
### Version 1.0.5
- Fix the condition for going up the tree while calculating disk usage.
- Remove setting of ServicePointManager global properties in SDK.
- Allow '-' in account names
- Fix property Name in DirectoryEntry for GetFileStatus
- Use ConfigureAwait(false) for async methods
### Version 1.0.4
- Fix FileVersion in csproj
- Fix transfer metadata file name length so that it is less than 260
- Fix the Acl consistency check for FileProperties
### Version 1.0.3
- Fix account validation
### Version 1.0.2
- Fix path encoding
- Changed Api version
- Strongly name and sign the assemblies
- Fix the implict reference problem
- Remove the reference to NLog.config
- Make the async methods virtual and make some contructors public or protected to make unit testing with Moq like tools easier.
### Version 1.0.1
- Added feature resume in upload and download, non-binary uploads, recursive acl processor, diskusage, acl dump. Added Mock client for unit test. Fixed doc generation.
### Version 1.0.0
- Add bulk upload and bulk download
### Version 0.1.3-beta
- Fix the Newtonsoft.Json reference to 6.0.8 for .NET452 to be compatible with powershell and other azure libraries
### Version 0.1.2-beta
- Fix objectid bug in Core.getfileliststatus and Core.getaclstatus and add unit test for it
- Remove blocksize and replicationfactor from directory entry
- Change parameter names for some public APIs
### Version 0.1.1-beta
- Reference Microsoft.Rest.ClientRuntime instead of Microsoft.Rest.ClientRuntime.Azure.Authentication in Microsoft.Azure.DataLake.Store project.
- Reference Microsoft.Rest.ClientRuntime.Azure.Authentication in Microsoft.Azure.DataLake.Store.UnitTest.
### Version 0.1.0-beta
- Initial PreRelease