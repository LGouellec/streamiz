using Streamiz.Kafka.Net.State;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class StateStoreFactory
    {
        private readonly StoreBuilder storeBuilder;

        public StateStoreFactory(StoreBuilder builder)
        {
            storeBuilder = builder;
        }

        public string Name => storeBuilder.Name;
        public bool LoggingEnabled => storeBuilder.LoggingEnabled;
        public IDictionary<string, string> LogConfig => storeBuilder.LogConfig;

        public IStateStore Build() => storeBuilder.Build() as IStateStore;
    }
}
