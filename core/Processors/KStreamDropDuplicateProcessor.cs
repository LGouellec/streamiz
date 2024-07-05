using System;
using System.Collections.Generic;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamDropDuplicateProcessor<K, V> : AbstractProcessor<K, V>
    {
        private IWindowStore<K, V> windowEventStore;
        private ProcessorContext context;
        private readonly long leftDurationMs;
        private readonly long rightDurationMs;
        private readonly string storeName;
        private readonly Func<K, V, V, bool> valueComparer;

        public KStreamDropDuplicateProcessor(
            string name,
            string storeName,
            Func<K, V, V, bool> valueComparer,
            long maintainDurationMs) 
            : base(name)
        {
            leftDurationMs = maintainDurationMs / 2;
            rightDurationMs = maintainDurationMs - leftDurationMs;
            this.storeName = storeName;
            this.valueComparer = valueComparer;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);
            this.context = context;
            windowEventStore = (IWindowStore<K, V>)context.GetStateStore(storeName);
        }

        public override void Process(K key, V value)
        {
            LogProcessingKeyValue(key, value);
            
            if (!EqualityComparer<V>.Default.Equals(value, default(V)))
            {
                if (IsDuplicate(key, value)) {
                    windowEventStore.Put(key, value, context.Timestamp);
                } else {
                    windowEventStore.Put(key, value, context.Timestamp);
                    Forward(key, value);
                }
            }
            else
            {
                Forward(key, value);
            }
        }
        
        private bool IsDuplicate(K key, V value) {
            long eventTime = context.Timestamp;

            using var iterator = windowEventStore.Fetch(
                key,
                eventTime - leftDurationMs,
                eventTime + rightDurationMs);
            bool isDuplicate = iterator.MoveNext();
                
            return isDuplicate &&
                   iterator.Current != null &&
                   valueComparer(key, iterator.Current.Value.Value, value);
        }
    }
    
}