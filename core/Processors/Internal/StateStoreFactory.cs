using System.Collections.Generic;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class StateStoreFactory
    {
        private readonly IStoreBuilder storeBuilder;
        private readonly Dictionary<(string, int), IStateStore> stores = new();
        internal readonly List<string> users = new();

        public StateStoreFactory(IStoreBuilder builder)
        {
            storeBuilder = builder;
        }

        public string Name => storeBuilder.Name;
        public bool LoggingEnabled => storeBuilder.LoggingEnabled;
        public IDictionary<string, string> LogConfig => storeBuilder.LogConfig;
        public bool IsWindowStore => storeBuilder.IsWindowStore;
        public long RetentionMs => storeBuilder.RetentionMs;

        public IStateStore Build(TaskId taskId, IStreamConfig config)
        {
            if (taskId != null)
            {
                if (stores.TryGetValue((Name, taskId.Partition), out var storeInstance))
                {
                    return storeInstance;
                }

                var store = storeBuilder.Build(config);
                stores.Add((Name, taskId.Partition), store);
                return store;
            }

            return storeBuilder.Build(config);
        }
    }
}
