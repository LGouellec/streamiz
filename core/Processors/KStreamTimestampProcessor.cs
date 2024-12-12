﻿using System;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamTimestampProcessor<K, V> : AbstractProcessor<K, V>
    {
        private readonly Func<K, V, IRecordContext, long> timestampExtractor;

        public KStreamTimestampProcessor(Func<K, V, IRecordContext, long> timestampExtractor)
        {
            this.timestampExtractor = timestampExtractor;
        }

        public override void Process(K key, V value)
        {
            LogProcessingKeyValue(key, value);
            var timestamp = timestampExtractor(key, value, Context.RecordContext);
            if (timestamp >= 0)
            {
                Context.RecordContext.ChangeTimestamp(timestamp);
            }

            this.Forward(key, value);
        }
    }
}
