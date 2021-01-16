using System;
using System.Collections.Generic;
using System.Text;

namespace Microsoft.Azure.DataLake.Store
{
    /// <summary>
    /// It contains the parameters and their respective values for each operation in Operation.cs. For example:
    /// For operation Create the valid parameters are syncFlag, write,filesessionid,leaseid. Below is a example of the http request for create which shows the parameters and their corresponding values 
    /// https://something.azuredatalakestore.net/webhdfs/v1/a/b/c.txt?op=CREATE&syncFlag=DATA&overwrite=true&write=true&filesessionid=1d897148-0a5e-4ae2-b66e-f197c6cdc67d&leaseid=1d897148-0a5e-4ae2-b66e-f197c6cdc67d
    /// </summary>
    internal class QueryParams
    {
        /// <summary>
        /// Separator for appending the http request parameters for an operation
        /// </summary>
        private const string Separator = "&";
        /// <summary>
        /// This is the value of one parameter "api-version" that is passed along every request
        /// </summary>
        internal static string ApiVersion = "2018-12-01";
        internal QueryParams()
        {
            Param = new Dictionary<string, string>();
        }
        /// <summary>
        /// Dictionary containing parameter and their corresponding values
        /// </summary>
        private Dictionary<string, string> Param { get; }
        /// <summary>
        /// Adds the value for each parameter
        /// </summary>
        /// <param name="key">parameter name</param>
        /// <param name="value">value</param>
        internal void Add(string key, string value)
        {
            Param.Add(key, value);
        }

        /// <summary>
        /// Removes the key-value pair for the given parameter
        /// </summary>
        /// <param name="key">parameter name</param>
        internal void Remove(string key)
        {
            Param.Remove(key);
        }

        /// <summary>
        /// Serializes the parameters and their values in form of a string
        /// </summary>
        /// <param name="opCodes">Operation Code which is the value of parameter "op"</param>
        /// <returns>Serialized parameter:value string for the request</returns>
        internal string Serialize(string opCodes)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("op=");
            sb.Append(opCodes);
            if (Param.Count > 0)
            {
                foreach (string nm in Param.Keys)
                {
                    sb.Append(Separator);
                    sb.Append(nm);
                    sb.Append("=");
                    // This is necessary for encoding query values like for rename we have the destination path as a query parameter
                    sb.Append(Uri.EscapeDataString(Param[nm]));

                }
            }
            sb.Append(Separator);
            sb.Append("api-version");
            sb.Append("=");
            sb.Append(ApiVersion);
            return sb.ToString();
        }
    }
}
