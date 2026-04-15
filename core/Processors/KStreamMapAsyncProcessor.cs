using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamMapAsyncProcessor<K, V, K1, V1> :
        AbstractAsyncProcessor<K, V, K1, V1>
    {
        private readonly Func<ExternalRecord<K, V>, ExternalContext, Task<KeyValuePair<K1, V1>>> asyncMapper;

        public KStreamMapAsyncProcessor(
            Func<ExternalRecord<K, V>, ExternalContext, Task<KeyValuePair<K1, V1>>> asyncMapper,
            RetryPolicy retryPolicy)
            : base(retryPolicy)
        {
            this.asyncMapper = asyncMapper;
        }

        public override async Task<IEnumerable<KeyValuePair<K1, V1>>> ProcessAsync(K key, V value, Headers headers,
            long timestamp, ExternalContext context)
        {
            var record = new ExternalRecord<K, V>(key, value, headers, timestamp);

            async Task<IEnumerable<KeyValuePair<K1, V1>>> WrapperFunction(ExternalRecord<K, V> e, ExternalContext c)
            {
                var map = await asyncMapper(e, c);
                return map.ToSingle();
            }

            return await WrapperFunction(
                record,
                context);
        }
    }
}