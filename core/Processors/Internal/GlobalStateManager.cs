using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using Streamiz.Kafka.Net.Stream.Internal;
using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class GlobalStateManager : IGlobalStateManager
    {
        private readonly IDictionary<string, IStateStore> globalStores = new Dictionary<string, IStateStore>();
        private readonly ILog log = Logger.GetLogger(typeof(GlobalStateManager));
        private readonly ProcessorTopology topology;
        private readonly IAdminClient adminClient;
        private ProcessorContext context;

        public GlobalStateManager(ProcessorTopology topology, IAdminClient adminClient)
        {
            this.topology = topology;
            this.adminClient = adminClient;
        }

        public IDictionary<TopicPartition, long> ChangelogOffsets { get; } = new Dictionary<TopicPartition, long>();

        public void Close()
        {
            throw new NotImplementedException();
        }

        public void Flush()
        {
            // TODO
        }

        public IStateStore GetStore(string name)
        {
            return this.globalStores.ContainsKey(name)
                ? this.globalStores[name]
                : null;
        }

        public ISet<string> Initialize()
        {
            foreach (var store in this.topology.GlobalStateStores.Values)
            {
                store.Init(this.context, store);
            }

            return this.globalStores.Keys.ToSet();
        }

        public void Register(IStateStore store, StateRestoreCallback callback)
        {
            string storeName = store.Name;
            log.Debug($"Registering state store {storeName} to its state manager");

            if (this.globalStores.ContainsKey(store.Name))
            {
                throw new ArgumentException($" Store {storeName} has already been registered.");
            }

            var topicPartitions = this.TopicPartitionsForStore(store);
            foreach (var partition in topicPartitions)
            {
                // TODO: restore state? see java GlobalStateManagerImpl.registerStore
                // get offset from kafka?
                this.ChangelogOffsets[partition] = 0;
            }

            this.globalStores[storeName] = store;
        }

        public void SetGlobalProcessorContext(ProcessorContext processorContext)
        {
            this.context = processorContext;
        }

        private IEnumerable<TopicPartition> TopicPartitionsForStore(IStateStore store)
        {
            var topic = this.topology.StoresToTopics[store.Name];
            // TODO: how long should we wait here?
            var metadata = this.adminClient.GetMetadata(topic, TimeSpan.FromSeconds(5));

            var result = metadata.Topics.Single().Partitions.Select(partition => new TopicPartition(topic, partition.PartitionId));
            return result;
        }
    }
}
