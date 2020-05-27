using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using System;
using System.Collections.Generic;
using System.Linq;
using Streamiz.Kafka.Net.Stream.Internal;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Errors;
using System.Text;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class GlobalStateManager : IGlobalStateManager
    {
        private readonly IDictionary<string, IStateStore> globalStores = new Dictionary<string, IStateStore>();
        private readonly ILog log = Logger.GetLogger(typeof(GlobalStateManager));
        private readonly ProcessorTopology topology;
        private readonly IAdminClient adminClient;
        private readonly IStreamConfig config;
        private ProcessorContext context;

        public GlobalStateManager(ProcessorTopology topology, IAdminClient adminClient, IStreamConfig config)
        {
            this.topology = topology;
            this.adminClient = adminClient;
            this.config = config;
        }

        public IDictionary<TopicPartition, long> ChangelogOffsets { get; } = new Dictionary<TopicPartition, long>();

        public IEnumerable<string> StateStoreNames => globalStores.Keys;

        public void Close()
        {
            this.log.Debug("Closing global state manager");
            var closeFailed = new StringBuilder();
            foreach (var entry in this.globalStores)
            {
                try
                {
                    log.Debug($"Closing store {entry.Key}");
                    entry.Value.Close();
                }
                catch (Exception e)
                {
                    log.Error($"Failed to close global state store {entry.Key}", e);
                    closeFailed.AppendLine($"Failed to close global state store {entry.Key}. Reason: {e}");
                }
            }
            if (closeFailed.Length > 0)
            {
                throw new ProcessorStateException($"Exceptions caught during closing of 1 or more global state globalStores\n{closeFailed}");
            }
        }

        public void Flush()
        {
            this.log.Debug("Flushing all global globalStores registered in the state manager");
            foreach (var entry in this.globalStores)
            {
                log.Debug($"Flushing store {entry.Key}");
                entry.Value.Flush();
            }
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
                string storeName = store.Name;

                if (this.globalStores.ContainsKey(store.Name))
                {
                    throw new ArgumentException($" Store {storeName} has already been registered.");
                }

                var topicPartitions = this.TopicPartitionsForStore(store);
                foreach (var partition in topicPartitions)
                {
                    this.ChangelogOffsets[partition] = 0;
                }

                this.globalStores[storeName] = store;
            }

            return this.topology.GlobalStateStores.Values.Select(x => x.Name).ToSet();
        }

        public void Register(IStateStore store, StateRestoreCallback callback)
        {
            // nothing to do here for now. Everything is handled in Initialize method.
        }

        public void SetGlobalProcessorContext(ProcessorContext processorContext)
        {
            this.context = processorContext;
        }

        private IEnumerable<TopicPartition> TopicPartitionsForStore(IStateStore store)
        {
            var topic = this.topology.StoresToTopics[store.Name];
            var metadata = this.adminClient.GetMetadata(topic, TimeSpan.FromMilliseconds(this.config.MetadataRequestTimeoutMs));

            if (metadata == null || metadata.Topics.Count == 0)
            {
                throw new StreamsException($"There are no partitions available for topic {topic} when initializing global store {store.Name}");
            }

            var result = metadata.Topics.Single().Partitions.Select(partition => new TopicPartition(topic, partition.PartitionId));
            return result;
        }
    }
}
