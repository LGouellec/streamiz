using Confluent.Kafka;
using Streamiz.Kafka.Net.State;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class StateStoreFactory
    {
        private readonly StoreBuilder storeBuilder;
        private readonly Dictionary<(string, int), IStateStore> stores =
            new Dictionary<(string, int), IStateStore>();

        public StateStoreFactory(StoreBuilder builder)
        {
            storeBuilder = builder;
        }

        public string Name => storeBuilder.Name;
        public bool LoggingEnabled => storeBuilder.LoggingEnabled;
        public IDictionary<string, string> LogConfig => storeBuilder.LogConfig;

        public IStateStore Build(TopicPartition partition)
        {
            if (partition != null)
            {
                if (stores.ContainsKey((Name, partition.Partition.Value)))
                    return stores[(Name, partition.Partition.Value)];
                else
                {
                    var store = storeBuilder.Build() as IStateStore;
                    stores.Add((Name, partition.Partition.Value), store);
                    return store;
                }
            }
            else
                return storeBuilder.Build() as IStateStore;
        }
    }
}
