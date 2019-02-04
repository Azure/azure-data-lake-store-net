# Changes to the SDK
### Version 1.1.15
- Use minimal flag for liststatus in recursive acl. This improves the performance.
- Fix remote json response that is not parsable in the exception message
- Improve the enumerate logic for recursive acl tool, this prevents high heap consumption for very large directories
- Set null to the array element of the priority queue that is dequeued
- Use continutation token internally for enumerate
- Fix json parsing of EnumerateDeletedItems
### Version 1.1.14
- Add SDk support for enumerate and restore APIs for deleted items
- Fix handling of negative acesstime and modificationtime for getfilestatus and liststatus
- Fix async cancellation token
- Minor fixes: Fix MockADlsClient Create with overwrite, Fix HasAcl in copy constructor
### Version 1.1.13
- Fix timeouts not getting retried 
### Version 1.1.12
- Increase the default thread count if physical cores is 0, Fix threadcount to be defaultthreadcount if input is 0
- Enable getconsistent length in getfilestatus
- Fix cancellation support in Enumerate
- Fix timeout for async, netcore
- Add unittests for timeout and connection broken
- Fix mockclient account validation, ChangeAcl api
- Fix priorityqueue parent calculation
### Version 1.1.11
- Change test framework
- Refactor ListStatus
- Throw exception when token is malformed so that retryable requests are retried
- Fix passing exception messages as a part of latency header
### Version 1.1.10
- Support for Json formatted requests in Request body, e:g MsConcat.
- Supporting special characters in msconcat api for source files.
- Updated the server to latest version
- Fix liststatus response parsing when filestatus object contains array in one field.
- Fix the default thread calculation, make minimum threads as 1 if number of cores returned is 0.
### Version 1.1.9
- Fix error handling for chunked error response
- Allow concat for single source file and add unittest
- Fix BulkUpload for very large files which reaches the concatenate limit
### Version 1.1.8
- Fix FileTransfer upload for relative input path. Add unittest
- Fix error message for remote exceptions which does not return json output. 
- Add Authentication header length for logging and Adls exception message for 401
- Add async api corresponding to non-async methods in mock adlsclient
- Add cancellation token to recursive acl change and recursive DU and acl dump. Add progress tracking to recursive acl change
- Allow RecursiveAcl application of default acl only to ignore file
- For recursive acl change have priority of Acl change more than enumerate
- Fix overflow of offset in AdlsOutputStream.WriteAsync
- Fix encoding of query parameters
- Fix useragent so that it does not contain localized values
### Version 1.1.7
- Consider only access acls for comparing files to parent directory for determining consistent acls in recursive acl dump. Add unittest
- Add netcore support for unittest. Change MockServer to use weblistener for it's netcore support.
- Fix mock testing for GetContentSummary, GetFileProperties
### Version 1.1.6
- Mock api tests for GetContentSummary, ChangeAcl recursively and FileProperties dump
### Version 1.1.5
- Fix Copyright message
- Fix missing / prefix in path
- Fix NullException error for GetFileProperties with input path as a file
- Add documentation for threadsafety of Apis in ADlsClient and Core
- Update the NLog version from beta to stable for .NETStandard
- Use System.Net.Requests instead of System.Net.Http
- Internal changes
- Add NonIdempotentRetryPolicy for operations that can retry on 429, and use it instead of NoRetryPolicy
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