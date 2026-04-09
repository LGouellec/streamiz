using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph
{
    internal class KStreamMapAsync<K, V, K1, V1> : IProcessorSupplier<K, V>
    {
        private readonly Func<ExternalRecord<K, V>, ExternalContext, Task<KeyValuePair<K1, V1>>> asyncMapper;
        private readonly RetryPolicy retryPolicy;
        private readonly ParallelProcessingConfig parallelProcessingConfig;

        public KStreamMapAsync(
            Func<ExternalRecord<K, V>, ExternalContext, Task<KeyValuePair<K1, V1>>> asyncMapper,
            RetryPolicy retryPolicy,
            ParallelProcessingConfig parallelProcessingConfig = null)
        {
            this.asyncMapper = asyncMapper;
            this.retryPolicy = retryPolicy ?? RetryPolicy.NewBuilder().Build();
            this.parallelProcessingConfig = parallelProcessingConfig;
        }

        public IProcessor<K, V> Get()
            => new KStreamMapAsyncProcessor<K, V, K1, V1>(asyncMapper, retryPolicy, parallelProcessingConfig);
    }
}