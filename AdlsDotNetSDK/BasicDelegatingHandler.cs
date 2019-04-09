using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.DataLake.Store
{
    internal class BasicDelegatingHandler : DelegatingHandler
    {
        public BasicDelegatingHandler(HttpMessageHandler innerHandler)
            : base(innerHandler)
        {
        }
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            return await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
        }
    }
}
