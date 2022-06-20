using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph
{
    internal class KStreamFlatMapAsync<K, V, K1, V1> : IProcessorSupplier<K, V>
    {
        private readonly Func<ExternalRecord<K, V>, ExternalContext, Task<IEnumerable<KeyValuePair<K1, V1>>>> asyncMapper;
        private readonly RetryPolicy retryPolicy;

        public KStreamFlatMapAsync(Func<ExternalRecord<K, V>, ExternalContext, Task<IEnumerable<KeyValuePair<K1, V1>>>> asyncMapper, RetryPolicy retryPolicy)
        {
            this.asyncMapper = asyncMapper;
            this.retryPolicy = retryPolicy ?? RetryPolicyBuilder.NewBuilder().Build();
        }

        public IProcessor<K, V> Get()
            => new KStreamFlatMapAsyncProcessor<K, V, K1, V1>(asyncMapper, retryPolicy);
    }


}