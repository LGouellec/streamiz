using Confluent.Kafka;
using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Stream.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
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

        public ICollection<TopicPartition> ChangelogPartitions => throw new NotImplementedException();

        public void Checkpoint()
        {
            throw new NotImplementedException();
        }

        public void Close()
        {
            log.Debug("Closing global state manager");
            var closeFailed = new StringBuilder();
            foreach (var entry in globalStores)
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
            log.Debug("Flushing all global globalStores registered in the state manager");
            foreach (var entry in globalStores)
            {
                log.Debug($"Flushing store {entry.Key}");
                entry.Value.Flush();
            }
        }

        public TopicPartition GetRegisteredChangelogPartitionFor(string name)
        {
            // TODO : maybe
            throw new NotImplementedException();
        }

        public IStateStore GetStore(string name)
        {
            return globalStores.ContainsKey(name)
                ? globalStores[name]
                : null;
        }

        public ISet<string> Initialize()
        {
            foreach (var store in topology.GlobalStateStores.Values)
            {
                store.Init(context, store);
                string storeName = store.Name;

                if (globalStores.ContainsKey(store.Name))
                {
                    throw new ArgumentException($" Store {storeName} has already been registered.");
                }

                var topicPartitions = TopicPartitionsForStore(store);
                foreach (var partition in topicPartitions)
                {
                    ChangelogOffsets[partition] = 0;
                }

                globalStores[storeName] = store;
            }

            return topology.GlobalStateStores.Values.Select(x => x.Name).ToSet();
        }

        public void InitializeOffsetsFromCheckpoint()
        {
            throw new NotImplementedException();
        }

        public void Register(IStateStore store, StateRestoreCallback callback)
        {
            // nothing to do here for now. Everything is handled in Initialize method.
        }

        public void SetGlobalProcessorContext(ProcessorContext processorContext)
        {
            context = processorContext;
        }

        public void UpdateChangelogOffsets(IDictionary<TopicPartition, long> writtenOffsets)
        {
            throw new NotImplementedException();
        }

        private IEnumerable<TopicPartition> TopicPartitionsForStore(IStateStore store)
        {
            var topic = topology.StoresToTopics[store.Name];
            var metadata = adminClient.GetMetadata(topic, TimeSpan.FromMilliseconds(10000));

            if (metadata == null || metadata.Topics.Count == 0)
            {
                throw new StreamsException($"There are no partitions available for topic {topic} when initializing global store {store.Name}");
            }

            var result = metadata.Topics.Single().Partitions.Select(partition => new TopicPartition(topic, partition.PartitionId));
            return result;
        }
    }
}
