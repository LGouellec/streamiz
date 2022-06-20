using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamForeachAsyncProcessor<K, V> : 
        AbstractAsyncProcessor<K, V, K, V>
    {
        private readonly Func<ExternalRecord<K, V>, ExternalContext, Task> asyncCall;

        public KStreamForeachAsyncProcessor(Func<ExternalRecord<K, V>, ExternalContext, Task> asyncCall, RetryPolicy policy) 
            : base(policy)
        {
            this.asyncCall = asyncCall;
        }

        public override async Task<IEnumerable<KeyValuePair<K, V>>> ProcessAsync(K key, V value, Headers headers, long timestamp, ExternalContext context)
        {
            var record = new ExternalRecord<K, V>(key, value, headers, timestamp);
            await asyncCall(record, context);
            return null;
        }
    }
}