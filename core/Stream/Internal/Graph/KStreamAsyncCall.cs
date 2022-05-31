using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph
{
    internal class KStreamAsyncCall<K, V, K1, V1> : IProcessorSupplier<K, V>
    {
        private readonly Func<ExternalRecord<K, V>, ExternalContext, Task<KeyValuePair<K1, V1>>> asyncExternalCall;
        private readonly RetryPolicy retryPolicy;

        public KStreamAsyncCall(
            Func<ExternalRecord<K, V>, ExternalContext, Task<KeyValuePair<K1, V1>>> asyncExternalCall,
            RetryPolicy retryPolicy)
        {
            this.asyncExternalCall = asyncExternalCall;
            this.retryPolicy = retryPolicy;
        }

        public IProcessor<K, V> Get()
            => new KStreamAsyncCallProcessor<K, V, K1, V1>(asyncExternalCall, retryPolicy);
    }
}