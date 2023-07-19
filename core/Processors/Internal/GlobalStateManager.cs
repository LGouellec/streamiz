using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Stream.Internal;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class GlobalStateManager : IGlobalStateManager
    {
        private readonly IDictionary<string, IStateStore> globalStores = new Dictionary<string, IStateStore>();
        private readonly ILogger log = Logger.GetLogger(typeof(GlobalStateManager));
        private readonly IConsumer<byte[], byte[]> globalConsumer;
        private readonly ProcessorTopology topology;
        private readonly IAdminClient adminClient;
        private readonly IStreamConfig config;
        private IOffsetCheckpointManager offsetCheckpointManager;
        private readonly List<string> globalNonPersistentStateStores = new();
        private readonly IDictionary<string, string> storesToTopic;
        private readonly List<string> changelogTopics = new();
        
        
        private ProcessorContext context;

        public GlobalStateManager(
            IConsumer<byte[], byte[]> globalConsumer,
            ProcessorTopology topology,
            IAdminClient adminClient,
            IStreamConfig config)
        {
            this.globalConsumer = globalConsumer;
            this.topology = topology;
            this.adminClient = adminClient;
            this.config = config;
            storesToTopic = this.topology.StoresToTopics;
                            
            foreach(var store in this.topology.GlobalStateStores.Values)
                if (!store.Persistent)
                    globalNonPersistentStateStores.Add(storesToTopic[store.Name]);
        }

        public IDictionary<TopicPartition, long> ChangelogOffsets { get; private set; }

        public IEnumerable<string> StateStoreNames => globalStores.Keys;

        public ICollection<TopicPartition> ChangelogPartitions => throw new NotImplementedException();

        public void Checkpoint()
        {
            try
            {
                offsetCheckpointManager.Write(context.Id,
                    ChangelogOffsets.Where(kv =>
                        !globalNonPersistentStateStores.Contains(kv.Key.Topic)).ToDictionary());
            }
            catch (Exception e)
            {
                log.LogWarning($"Failed to write offset checkpoint for global stores: {e.Message}");
            }
        }

        public void Close()
        {
            log.LogDebug("Closing global state manager");
            var closeFailed = new StringBuilder();
            foreach (var entry in globalStores)
            {
                try
                {
                    log.LogDebug($"Closing store {entry.Key}");
                    entry.Value.Close();
                }
                catch (Exception e)
                {
                    log.LogError(e, $"Failed to close global state store {entry.Key}");
                    closeFailed.AppendLine($"Failed to close global state store {entry.Key}. Reason: {e}");
                }
            }
            if (closeFailed.Length > 0)
            {
                throw new ProcessorStateException($"Exceptions caught during closing of 1 or more global state globalStores\n{closeFailed}");
            }
        }

        public string ChangelogFor(string storeName)
            => storesToTopic.Get(storeName);

        public void Flush()
        {
            log.LogDebug("Flushing all global globalStores registered in the state manager");
            foreach (var entry in globalStores)
            {
                log.LogDebug($"Flushing store {entry.Key}");
                entry.Value.Flush();
            }
        }

        public TopicPartition GetRegisteredChangelogPartitionFor(string name)
        {
            // Not used for global state store
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
            InitializeOffsetsFromCheckpoint();
            
            List<String> changelogTopics = new();
            this.changelogTopics.Clear();
            foreach (var store in topology.GlobalStateStores.Values)
            {
                string storeName = store.Name;
                string sourceTopic = storesToTopic[storeName];

                if (globalStores.ContainsKey(store.Name))
                {
                    throw new ArgumentException($" Store {storeName} has already been registered.");
                }
                
                changelogTopics.Add(sourceTopic);
                store.Init(context, store);
                globalStores[storeName] = store;
            }
            
            ChangelogOffsets.Keys.ForEach(tp =>
            {
                if (!changelogTopics.Contains(tp.Topic))
                {
                    log.LogError("Encountered a topic-partition in the global checkpoint manager not associated with any global" +
                                 $" state store, topic-partition: {tp}. If this topic-partition is no longer valid," +
                                 " an application reset and state store directory cleanup will be required.");
                    throw new StreamsException(
                        $"Encountered a topic-partition not associated with any global state store");
                }
            });

            this.changelogTopics.AddRange(changelogTopics);
            return topology.GlobalStateStores.Values.Select(x => x.Name).ToSet();
        }

        public void InitializeOffsetsFromCheckpoint()
        {
            try
            {
                ChangelogOffsets = offsetCheckpointManager.Read(context.Id);
            }
            catch (Exception e)
            {
                throw new StreamsException("Failed to read checkpoints for global state stores", e);
            }
        }

        public void Register(IStateStore store, Action<ConsumeResult<byte[], byte[]>> callback)
        {
            log.LogInformation($"Restoring state for global store {store.Name}");
            var topicPartitions = TopicPartitionsForStore(store).ToList();
            var highWatermarks = OffsetsChangelogs(topicPartitions);
            
            try
            {
                RestoreState(
                    callback,
                    topicPartitions,
                    highWatermarks,
                    StateManagerTools.ConverterForStore(store));
            }
            finally
            {
                globalConsumer.Unassign();
                log.LogInformation($"Global store {store.Name} is completely restored");
            }
        }

        public void SetGlobalProcessorContext(ProcessorContext processorContext)
        {
            context = processorContext;
            
            var offsetCheckpointMngt = config.OffsetCheckpointManager
                                       ?? new OffsetCheckpointFile(
                                           context.StateDir);
            
            offsetCheckpointMngt.Configure(config, context.Id);

            offsetCheckpointManager = offsetCheckpointMngt;
        }

        public void UpdateChangelogOffsets(IDictionary<TopicPartition, long> writtenOffsets)
            => ChangelogOffsets.AddRange(writtenOffsets);
        
        #region Private

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

        private void RestoreState(
            Action<ConsumeResult<byte[], byte[]>> restoreCallback,
            List<TopicPartition> topicPartitions,
            IDictionary<TopicPartition, (Offset, Offset)> offsetWatermarks,
            Func<ConsumeResult<byte[], byte[]>, ConsumeResult<byte[], byte[]>> recordConverter)
        {
            foreach (var topicPartition in topicPartitions)
            {
                long offset, checkpoint, highWM;
                
                if (ChangelogOffsets.ContainsKey(topicPartition)) 
                    checkpoint = ChangelogOffsets[topicPartition];
                else
                    checkpoint = Offset.Beginning.Value;
                
                globalConsumer.Assign((new TopicPartitionOffset(topicPartition, new Offset(checkpoint))).ToSingle());
                offset = checkpoint;
                var lowWM = offsetWatermarks[topicPartition].Item1;
                highWM = offsetWatermarks[topicPartition].Item2;

                while (offset < highWM - 1)
                {
                    if (offset == Offset.Beginning && highWM == 0) // no message into local and topics;
                        break;
                    
                    if (lowWM == highWM) // if low offset == high offset
                        break;
                    
                    var records = globalConsumer.ConsumeRecords(TimeSpan.FromMilliseconds(config.PollMs),
                        config.MaxPollRestoringRecords).ToList();

                    var convertedRecords = records.Select(r => recordConverter(r)).ToList();

                    foreach (var record in convertedRecords)
                        restoreCallback?.Invoke(record);

                    if (convertedRecords.Any())
                        offset = records.Last().Offset;
                }

                ChangelogOffsets.AddOrUpdate(topicPartition, offset);
            }
        }

        private IDictionary<TopicPartition, (Offset, Offset)> OffsetsChangelogs(IEnumerable<TopicPartition> topicPartitions)
        {
            return topicPartitions
                .Select(tp => {
                    var offsets = globalConsumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(5));
                    return new
                    {
                        TopicPartition = tp,
                        EndOffset = offsets.High,
                        BeginOffset = offsets.Low

                    };
                })
                .ToDictionary(i => i.TopicPartition, i => (i.BeginOffset, i.EndOffset));
        }

        #endregion
    }
}
