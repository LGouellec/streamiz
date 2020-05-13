using System;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamFilterProcessor<K,V> : AbstractProcessor<K, V>
    {
        private readonly Func<K, V, bool> predicate;
        private readonly bool not;


        public KStreamFilterProcessor(Func<K, V, bool> predicate, bool not)
        {
            this.predicate = predicate;
            this.not = not;
        }

        public override void Process(K key, V value)
        {
            LogProcessingKeyValue(key, value);
            if ((!not && predicate.Invoke(key, value)) || (not && !predicate.Invoke(key, value)))
            {
                this.Forward(key, value);
            }
        }
    }
}
