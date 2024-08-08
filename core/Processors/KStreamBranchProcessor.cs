using System;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamBranchProcessor<K, V> : AbstractProcessor<K, V>
    {
        private readonly Func<K, V,IRecordContext, bool>[] predicates;
        private readonly string[] childNodes;

        public KStreamBranchProcessor(Func<K, V,IRecordContext, bool>[] predicates, string[] childNodes)
        {
            this.predicates = predicates;
            this.childNodes = childNodes;
        }

        public override void Process(K key, V value)
        {
            LogProcessingKeyValue(key, value);
            for (int i = 0; i < this.predicates.Length; i++)
            {
                if (predicates[i].Invoke(key, value, Context.RecordContext))
                {
                    Forward(key, value, this.childNodes[i]);
                    break;
                }
            }
        }
    }
}
