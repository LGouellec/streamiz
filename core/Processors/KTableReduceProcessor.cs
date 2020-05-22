using System;
using System.Collections.Generic;
using System.Text;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KTableReduceProcessor<K, V> : AbstractKTableProcessor<K, V, K, V>
    {
        private Reducer<V> adder;
        private Reducer<V> substractor;


        public KTableReduceProcessor(string storeName, bool sendOldValues, Reducer<V> adder, Reducer<V> substractor)
            : base(storeName, sendOldValues)
        {
            this.adder = adder;
            this.substractor = substractor;
        }

        public override void Process(K key, Change<V> value)
        {
            throw new NotImplementedException();
        }
    }
}
