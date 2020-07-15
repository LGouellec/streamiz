using Streamiz.Kafka.Net.State;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamJoinWindowProcessor<K, V> : AbstractProcessor<K, V>
    {
        private readonly string storeName;
        private WindowStore<K, V> window;

        public KStreamJoinWindowProcessor(string storeName)
        {
            this.storeName = storeName;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);

            window = (WindowStore<K, V>)context.GetStateStore(storeName);
        }

        public override void Process(K key, V value)
        {
            if(key != null)
            {
                Forward(key, value);
                window.Put(key, value, Context.Timestamp);
            }
        }
    }
}
