using System;
using System.Threading.Tasks;
using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph
{
    internal class KStreamForeachAsync<K, V> : IProcessorSupplier<K, V>
    {
        private readonly Func<ExternalRecord<K, V>, ExternalContext, Task> asyncCall;
        private readonly RetryPolicy retryPolicy;
        private readonly ParallelProcessingConfig parallelProcessingConfig;

        public KStreamForeachAsync(
            Func<ExternalRecord<K, V>, ExternalContext, Task> asyncCall,
            RetryPolicy retryPolicy,
            ParallelProcessingConfig parallelProcessingConfig = null)
        {
            this.asyncCall = asyncCall;
            this.retryPolicy = retryPolicy ?? RetryPolicy.NewBuilder().Build();
            this.parallelProcessingConfig = parallelProcessingConfig;
        }

        public IProcessor<K, V> Get()
            => new KStreamForeachAsyncProcessor<K, V>(asyncCall, retryPolicy, parallelProcessingConfig);

    }
}