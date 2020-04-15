using System;
using System.Collections.Generic;
using Kafka.Streams.Net.Stream.Internal.Graph;

namespace Kafka.Streams.Net.Processors
{
    internal class KStreamBranchProcessor<K, V> : AbstractProcessor<K, V>
    {
        private Func<K, V, bool>[] predicates;
        private string[] childNodes;

        public KStreamBranchProcessor(Func<K, V, bool>[] predicates, string[] childNodes)
        {
            this.predicates = predicates;
            this.childNodes = childNodes;
        }

        public override object Clone()
        {
            var p = new KStreamBranchProcessor<K, V>(this.predicates, this.childNodes);
            p.StateStores = new List<string>(this.StateStores);
            return p;
        }

        public override void Process(K key, V value)
        {
            LogProcessingKeyValue(key, value);
            for (int i = 0; i < this.predicates.Length; i++)
            {
                if (this.predicates[i].Invoke(key, value))
                {
                    this.Forward(key, value, this.childNodes[i]);
                    break;
                }
            }
        }
    }
}
