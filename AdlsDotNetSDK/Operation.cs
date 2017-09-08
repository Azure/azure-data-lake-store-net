using System.Collections.Generic;

namespace Microsoft.Azure.DataLake.Store
{
    /// <summary>
    /// Lists a dictionary of all available Operations for a Http request
    /// </summary>
    public sealed class Operation
    {
        /// <summary>
        /// HTTP method the operation uses like GET/PUT/POST
        /// </summary>
        public string Method { get; }
        /// <summary>
        /// Whether the http request for this operation requires request body (data)
        /// </summary>
        public bool RequiresBody { get; }
        /// <summary>
        /// Whether the http response for this operation returns response body (data)
        /// </summary>
        public bool ReturnsBody { get; }
        /// <summary>
        /// What handler it uses WebHdfs or WebHdfsExt
        /// </summary>
        public string Namespace { get; }
        private Operation(string mthd, bool reqBody, bool retBody, string nmSpc)
        {
            Method = mthd;
            RequiresBody = reqBody;
            ReturnsBody = retBody;
            Namespace = nmSpc;
        }
        /// <summary>
        /// Dictionary containing the Operations
        /// </summary>
        public static Dictionary<OperationCodes, Operation> Operations = new Dictionary<OperationCodes, Operation>()
        {
            {OperationCodes.OPEN,new Operation("GET", Constants.RequiresBodyFalse, Constants.ReturnsBodyTrue, Constants.WebHdfs)},
            {OperationCodes.CREATE,new Operation("PUT", Constants.RequiresBodyTrue, Constants.ReturnsBodyFalse, Constants.WebHdfs)},
            {OperationCodes.MKDIRS,new Operation("PUT", Constants.RequiresBodyFalse, Constants.ReturnsBodyTrue, Constants.WebHdfs)},
            {OperationCodes.APPEND,new Operation("POST",Constants.RequiresBodyTrue, Constants.ReturnsBodyFalse, Constants.WebHdfs)},
            {OperationCodes.CONCURRENTAPPEND,new Operation("POST", Constants.RequiresBodyTrue, Constants.ReturnsBodyTrue, Constants.WebHdfsExt)},
            {OperationCodes.DELETE,new Operation("DELETE", Constants.RequiresBodyFalse, Constants.ReturnsBodyTrue, Constants.WebHdfs)},
            {OperationCodes.RENAME,new Operation("PUT", Constants.RequiresBodyFalse, Constants.ReturnsBodyTrue, Constants.WebHdfs)},
            {OperationCodes.SETEXPIRY,new Operation("PUT", Constants.RequiresBodyFalse, Constants.ReturnsBodyFalse, Constants.WebHdfsExt)},
            {OperationCodes.MSCONCAT,new Operation("POST", Constants.RequiresBodyTrue, Constants.ReturnsBodyFalse, Constants.WebHdfs)},
            {OperationCodes.GETFILESTATUS,new Operation("GET", Constants.RequiresBodyFalse, Constants.ReturnsBodyTrue, Constants.WebHdfs)},
            {OperationCodes.LISTSTATUS,new Operation("GET", Constants.RequiresBodyFalse, Constants.ReturnsBodyTrue, Constants.WebHdfs)},
            {OperationCodes.SETTIMES,new Operation("PUT", Constants.RequiresBodyFalse, Constants.ReturnsBodyFalse, Constants.WebHdfs)},
            {OperationCodes.CHECKACCESS,new Operation("GET", Constants.RequiresBodyFalse, Constants.ReturnsBodyFalse, Constants.WebHdfs)},
            {OperationCodes.SETPERMISSION,new Operation("PUT", Constants.RequiresBodyFalse, Constants.ReturnsBodyFalse, Constants.WebHdfs)},
            {OperationCodes.MODIFYACLENTRIES,new Operation("PUT", Constants.RequiresBodyFalse, Constants.ReturnsBodyFalse, Constants.WebHdfs)},
            {OperationCodes.SETACL,new Operation("PUT", Constants.RequiresBodyFalse, Constants.ReturnsBodyFalse, Constants.WebHdfs)},
            {OperationCodes.SETOWNER,new Operation("PUT",Constants.RequiresBodyFalse, Constants.ReturnsBodyFalse, Constants.WebHdfs)},
            {OperationCodes.REMOVEACLENTRIES,new Operation("PUT", Constants.RequiresBodyFalse, Constants.ReturnsBodyFalse, Constants.WebHdfs)},
            {OperationCodes.REMOVEACL,new Operation("PUT", Constants.RequiresBodyFalse, Constants.ReturnsBodyFalse, Constants.WebHdfs)},
            {OperationCodes.REMOVEDEFAULTACL,new Operation("PUT", Constants.RequiresBodyFalse, Constants.ReturnsBodyFalse, Constants.WebHdfs)},
            {OperationCodes.GETACLSTATUS,new Operation("GET", Constants.RequiresBodyFalse, Constants.ReturnsBodyTrue, Constants.WebHdfs)},
            {OperationCodes.GETCONTENTSUMMARY,new Operation("GET", Constants.RequiresBodyFalse, Constants.ReturnsBodyTrue,  Constants.WebHdfs)}
        };

    }
    /// <summary>
    /// Operation Codes of all the operation available as a part of the SDK
    /// </summary>
    public enum OperationCodes
    {
        OPEN,
        CREATE,
        MKDIRS,
        APPEND,
        CONCURRENTAPPEND,
        DELETE,
        RENAME,
        SETEXPIRY,
        MSCONCAT,
        GETFILESTATUS,
        LISTSTATUS,
        SETTIMES,
        CHECKACCESS,
        SETPERMISSION,
        MODIFYACLENTRIES,
        SETACL,
        SETOWNER,
        REMOVEACLENTRIES,
        REMOVEACL,
        REMOVEDEFAULTACL,
        GETACLSTATUS,
        GETCONTENTSUMMARY
    }
    /// <summary>
    /// Constants that describes properties of HttpWeb request
    /// </summary>
    internal static class Constants
    {
        public static bool RequiresBodyTrue = true;
        public static bool RequiresBodyFalse = false;
        public static bool ReturnsBodyTrue = true;
        public static bool ReturnsBodyFalse = false;
        public static string WebHdfs = "/webhdfs/v1";
        public static string WebHdfsExt = "/WebHdfsExt";
    }

}
