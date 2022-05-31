using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Processors
{
    internal interface IAsyncProcessor
    {
        public RetryPolicy Policy { get; }
    }
    
    internal interface IAsyncProcessor<K, V, K1, V1> : IAsyncProcessor
    {
        public Task<KeyValuePair<K1, V1>> ProcessAsync(K key, V value, Headers headers, long timestamp, ExternalContext context);
    }
}