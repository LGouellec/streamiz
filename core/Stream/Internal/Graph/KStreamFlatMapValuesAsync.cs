using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph
{
    internal class KStreamFlatMapValuesAsync<K, V, V1> : IProcessorSupplier<K, V>
    {
        private readonly Func<ExternalRecord<K, V>, ExternalContext, Task<IEnumerable<V1>>> asyncMapper;
        private readonly RetryPolicy retryPolicy;

        public KStreamFlatMapValuesAsync(
            Func<ExternalRecord<K, V>, ExternalContext, Task<IEnumerable<V1>>> asyncMapper,
            RetryPolicy retryPolicy)
        {
            this.asyncMapper = asyncMapper;
            this.retryPolicy = retryPolicy ?? RetryPolicy.NewBuilder().Build();
        }

        public IProcessor<K, V> Get()
        {
            async Task<IEnumerable<KeyValuePair<K, V1>>> Wrapper(ExternalRecord<K, V> e, ExternalContext c)
            {
                var newValue = await asyncMapper(e, c);
                return newValue.Select(v => new KeyValuePair<K, V1>(e.Key, v));
            }

            return new KStreamFlatMapAsyncProcessor<K, V, K, V1>(Wrapper, retryPolicy);
        }
    }
}