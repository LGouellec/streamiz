
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Table.Internal;
using System;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph
{
    internal class KStreamReduce<K, V> : IKStreamAggProcessorSupplier<K, K, V, V>
    {
        private readonly string storeName;
        private readonly Reducer<V> reducer;
        private bool sendOldValues = false;

        public KStreamReduce(string storeName, Reducer<V> reducer)
        {
            this.storeName = storeName;
            this.reducer = reducer;
        }

        public void EnableSendingOldValues()
        {
            sendOldValues = true;
        }

        public IProcessor<K, V> Get() 
            => new KStreamReduceProcessor<K, V>(reducer, storeName, sendOldValues);

        // TODO
        public IKTableValueGetterSupplier<K, V> View() => null;
    }
}