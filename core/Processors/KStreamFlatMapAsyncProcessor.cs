using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamFlatMapAsyncProcessor<K, V, K1, V1> : 
        AbstractAsyncProcessor<K, V, K1, V1>
    {
        private readonly Func<ExternalRecord<K, V>, ExternalContext, Task<IEnumerable<KeyValuePair<K1, V1>>>> asyncMapper;

        public KStreamFlatMapAsyncProcessor(
            Func<ExternalRecord<K, V>, ExternalContext, Task<IEnumerable<KeyValuePair<K1, V1>>>> asyncMapper,
            RetryPolicy retryPolicy) : base(retryPolicy)
        {
            this.asyncMapper = asyncMapper;
        }

        public override async Task<IEnumerable<KeyValuePair<K1, V1>>> ProcessAsync(K key, V value, Headers headers, long timestamp, ExternalContext context)
        {
            var record = new ExternalRecord<K, V>(key, value, headers, timestamp);

            return await asyncMapper(
                record,
                context);
        }
    }
}