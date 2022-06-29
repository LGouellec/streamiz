using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph
{
    internal class KStreamMapValuesAsync<K, V, V1> : IProcessorSupplier<K, V>
    {
        private readonly Func<ExternalRecord<K, V>, ExternalContext, Task<V1>> asyncMapper;
        private readonly RetryPolicy retryPolicy;

        public KStreamMapValuesAsync(
            Func<ExternalRecord<K, V>, ExternalContext, Task<V1>> asyncMapper,
            RetryPolicy retryPolicy)
        {
            this.asyncMapper = asyncMapper;
            this.retryPolicy = retryPolicy ?? RetryPolicy.NewBuilder().Build();
        }

        public IProcessor<K, V> Get()
        {
            async Task<KeyValuePair<K, V1>> Wrapper(ExternalRecord<K, V> e, ExternalContext c)
            {
                var newValue = await asyncMapper(e, c);
                return new KeyValuePair<K, V1>(e.Key, newValue);
            }

            return new KStreamMapAsyncProcessor<K, V, K, V1>(Wrapper, retryPolicy);
        }
    }
}