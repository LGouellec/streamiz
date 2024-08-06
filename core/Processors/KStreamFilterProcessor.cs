using System;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamFilterProcessor<K,V> : AbstractProcessor<K, V>
    {
        private readonly Func<K, V,IRecordContext, bool> predicate;
        private readonly bool not;


        public KStreamFilterProcessor(Func<K, V,IRecordContext, bool> predicate, bool not)
        {
            this.predicate = predicate;
            this.not = not;
        }

        public override void Process(K key, V value)
        {
            LogProcessingKeyValue(key, value);
            if ((!not && predicate.Invoke(key, value, Context.RecordContext)) || (not && !predicate.Invoke(key, value, Context.RecordContext)))
            {
                this.Forward(key, value);
            }
        }
    }
}
