using Confluent.Kafka;
using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Internal;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class ProcessorStateManager : IStateManager
    {
        internal class StateStoreMetadata
        {
            internal IStateStore Store { get; set; }
            internal TopicPartition ChangelogTopicPartition { get; set; }
            internal StateRestoreCallback RestoreCallback { get; set; }
            internal Func<ConsumeResult<byte[], byte[]>, ConsumeResult<byte[], byte[]>> RecordConverter { get; set; }

            /// <summary>
            /// indicating the current snapshot of the store as the offset of last changelog record that has been restore in local state store
            /// offset upsated in three ways :
            /// 1 - when loading checkpoint file
            /// 2 - updating with restore records
            /// 3 - when checkpointing the given written offsets from record collector
            /// </summary>
            internal long? Offset;
        }

        private static readonly string STATE_CHANGELOG_TOPIC_SUFFIX = "-changelog";

        private readonly ILog log;
        private readonly string logPrefix;
        private readonly IDictionary<string, StateStoreMetadata> registeredStores = new Dictionary<string, StateStoreMetadata>();
        private readonly TaskId taskId;
        private readonly IDictionary<string, string> changelogTopics;
        private readonly IOffsetCheckpointManager offsetCheckpointManager;
        private readonly IChangelogRegister changelogRegister;
        private IDictionary<string, IStateStore> globalStateStores = new Dictionary<string, IStateStore>();

        public IEnumerable<TopicPartition> Partition { get; private set; }

        public IEnumerable<string> StateStoreNames => registeredStores.Keys;

        public ICollection<TopicPartition> ChangelogPartitions => ChangelogOffsets.Keys.ToList();

        public IDictionary<TopicPartition, long> ChangelogOffsets =>
            registeredStores.Values
                .Where(s => s.ChangelogTopicPartition != null)
                .ToDictionary(s => s.ChangelogTopicPartition, s =>
                {
                    if (s.Offset.HasValue)
                        return s.Offset.Value + 1;
                    else
                        return 0L;
                });

        public ProcessorStateManager(
            TaskId taskId,
            IEnumerable<TopicPartition> partition,
            IDictionary<string, string> changelogTopics,
            IChangelogRegister changelogReader,
            IOffsetCheckpointManager offsetCheckpointManager)
        {
            log = Logger.GetLogger(typeof(ProcessorStateManager));
            logPrefix = $"stream-task[{taskId.Id}|{taskId.Partition}] ";
            this.taskId = taskId;
            Partition = partition;

            this.changelogTopics = changelogTopics ?? new Dictionary<string, string>();
            this.changelogRegister = changelogReader;
            this.offsetCheckpointManager = offsetCheckpointManager;
        }

        public static string StoreChangelogTopic(string applicationId, String storeName)
        {
            return $"{applicationId}-{storeName}{STATE_CHANGELOG_TOPIC_SUFFIX}";
        }

        private bool IsChangelogStateStore(string storeName)
            => changelogTopics.ContainsKey(storeName);

        private TopicPartition GetStorePartition(string storeName)
            => new TopicPartition(changelogTopics[storeName], taskId.Partition);

        #region State Manager IMPL

        public void Flush()
        {
            log.Debug($"{logPrefix}Flushing all stores registered in the state manager");

            foreach (var state in registeredStores)
            {
                log.Debug($"{logPrefix}Flushing store {state.Key}");
                state.Value.Store.Flush();
            }
        }

        public void Register(IStateStore store, StateRestoreCallback callback)
        {
            string storeName = store.Name;
            log.Debug($"{logPrefix}Registering state store {storeName} to its state manager");

            if (registeredStores.ContainsKey(storeName))
            {
                throw new ArgumentException($"{logPrefix} Store {storeName} has already been registered.");
            }

            ConsumeResult<byte[], byte[]> ToTimestampInstance(ConsumeResult<byte[], byte[]> source)
            {
                var newValue = source.Message.Value == null ? null : ByteBuffer.Build(8 + source.Message.Value.Length)
                                                                                .PutLong(source.Message.Timestamp.UnixTimestampMs)
                                                                                .Put(source.Message.Value)
                                                                                .ToArray();
                return new ConsumeResult<byte[], byte[]>
                {
                    IsPartitionEOF = source.IsPartitionEOF,
                    Message = new Message<byte[], byte[]>
                    {
                        Headers = source.Message.Headers,
                        Timestamp = source.Message.Timestamp,
                        Key = source.Message.Key,
                        Value = newValue
                    },
                    Offset = source.Offset,
                    TopicPartitionOffset = source.TopicPartitionOffset,
                    Topic = source.Topic,
                    Partition = source.Partition
                };
            }

            var metadata = IsChangelogStateStore(storeName) ?
                new StateStoreMetadata
                {
                    Store = store,
                    ChangelogTopicPartition = GetStorePartition(storeName),
                    RestoreCallback = callback,
                    RecordConverter = WrappedStore.IsTimestamped(store) ? (record) => ToTimestampInstance(record) : (record) => record,
                    Offset = null
                } :
                new StateStoreMetadata
                {
                    Store = store,
                    Offset = null
                };

            registeredStores.Add(storeName, metadata);

            if (IsChangelogStateStore(storeName))
                changelogRegister.Register(GetStorePartition(storeName), this);

            log.Debug($"{logPrefix}Registered state store {storeName} to its state manager");
        }

        public void Close()
        {
            log.Debug($"{logPrefix}Closing its state manager and all the registered state stores");

            changelogRegister.Unregister(registeredStores.Values.Where(m => m.ChangelogTopicPartition != null).Select(m => m.ChangelogTopicPartition));

            foreach( var state in registeredStores)
            {
                log.Debug($"{logPrefix}Closing storage engine {state.Key}");
                state.Value.Store.Close();
            }

            registeredStores.Clear();
        }

        public IStateStore GetStore(string name)
        {
            if (registeredStores.ContainsKey(name))
            {
                return registeredStores[name].Store;
            }
            else
            {
                return null;
            }
        }

        public void RegisterGlobalStateStores(IDictionary<string, IStateStore> globalStateStores)
        {
            this.globalStateStores = globalStateStores;
        }

        public TopicPartition GetRegisteredChangelogPartitionFor(string storeName)
        {
            if (registeredStores.ContainsKey(storeName))
            {
                var metadata = registeredStores[storeName];
                if (metadata.ChangelogTopicPartition != null)
                    return metadata.ChangelogTopicPartition;
                else
                    throw new IllegalStateException(
                        @$"Registered state store {storeName} does not have a registered 
                        changelog partition. 
                        This may happen if logging is disabled for 
                        the state store.");
            }
            else
                throw new IllegalStateException(
                    @$"State store {storeName} for which the registered
                    changelog partition should be retrieved has not
                    been registered");
        }

        public void UpdateChangelogOffsets(IDictionary<TopicPartition, long> writtenOffsets)
        {
            foreach(var kv in writtenOffsets)
            {
                var storeMetadata = GetStoreMetadata(kv.Key);
                if(storeMetadata != null)
                {
                    storeMetadata.Offset = kv.Value;
                    log.Debug($"State store {storeMetadata.Store.Name} updated to written offset {kv.Value} at changelog {kv.Key}");
                }
            }
        }

        public void Checkpoint()
        {
            IDictionary<TopicPartition, long> checkpointOffsets = new Dictionary<TopicPartition, long>();
            foreach(var store in registeredStores)
            {
                if (store.Value.ChangelogTopicPartition != null && store.Value.Store.Persistent)
                {
                    checkpointOffsets.Add(store.Value.ChangelogTopicPartition, store.Value.Offset.HasValue ? store.Value.Offset.Value : OffsetCheckpointFile.OFFSET_UNKNOWN);
                }
            }

            log.Debug($"{logPrefix}Writting checkpoint");
            try {
                offsetCheckpointManager.Write(taskId, checkpointOffsets);
            }catch(Exception e)
            {
                log.Warn($"{logPrefix}Failed to write offset checkpoint. Exception: {e.Message}{Environment.NewLine}{e.StackTrace}");
            }
         }

        public void InitializeOffsetsFromCheckpoint()
        {
            var loadedCheckpoints = offsetCheckpointManager.Read(taskId);

            log.Debug($"Loaded offsets from checkpoint manager: {string.Join(",", loadedCheckpoints.Select(c => $"[{c.Key.Topic}-{c.Key.Partition}]-{c.Value}"))}");

            foreach(var kvStore in registeredStores)
            {
                if(kvStore.Value.ChangelogTopicPartition == null)
                {
                    log.Info($"State store {kvStore.Value.Store.Name} is not logged and hence would not be restored");
                }
                else if (!kvStore.Value.Store.Persistent)
                {
                    log.Info($"Initializing to the starting offset for changelog {kvStore.Value.ChangelogTopicPartition} of in-memory state store {kvStore.Value.Store.Name}");
                }
                else if(kvStore.Value.Offset == null)
                {
                    if (loadedCheckpoints.ContainsKey(kvStore.Value.ChangelogTopicPartition))
                    {
                        long offset = loadedCheckpoints[kvStore.Value.ChangelogTopicPartition];
                        kvStore.Value.Offset = offset != OffsetCheckpointFile.OFFSET_UNKNOWN ? offset : null;
                        log.Debug($"State store {kvStore.Value.Store.Name} initialized from checkpoint with offset {offset} at changelog {kvStore.Value.ChangelogTopicPartition}");
                        loadedCheckpoints.Remove(kvStore.Value.ChangelogTopicPartition);
                    }
                    else
                    {
                        log.Info($"State store {kvStore.Value.Store.Name} did not find checkpoint offset, hence would " +
                                $"default to the starting offset at changelog {kvStore.Value.ChangelogTopicPartition}");
                    }
                }
                else
                {
                    loadedCheckpoints.Remove(kvStore.Value.ChangelogTopicPartition);
                    log.Debug($"Skipping re-initialization of offset from checkpoint for recycled store {kvStore.Value.Store.Name}");
                }

                if (loadedCheckpoints.Any())
                {
                    log.Warn($"Some loaded checkpoint offsets cannot find their corresponding state stores: {string.Join(",", loadedCheckpoints.Select(c => $"[{c.Key.Topic}-{c.Key.Partition}]-{c.Value}"))}");
                }
            }
        }

        #endregion

        internal StateStoreMetadata GetStoreMetadata(TopicPartition topicPartition)
        {
            foreach (var store in registeredStores)
                if (store.Value.ChangelogTopicPartition.Equals(topicPartition))
                    return store.Value;
            return null;
        }

        internal void Restore(StateStoreMetadata storeMetadata, IEnumerable<ConsumeResult<byte[], byte[]>> records)
        {
            if (!registeredStores.ContainsKey(storeMetadata.Store.Name))
                throw new IllegalStateException($"Restoring {storeMetadata.Store.Name} store which is not registered in this state manager, this should not happen");

            if (records.Any())
            {
                var listRecords = records.ToList();
                long endOffset = listRecords[listRecords.Count - 1].Offset.Value;
                var convertedRecords = records.Select(r => storeMetadata.RecordConverter(r));

                // TODO : bach restoration behavior
                foreach (var _record in convertedRecords)
                    storeMetadata.RestoreCallback(Bytes.Wrap(_record.Message.Key), _record.Message.Value);

                storeMetadata.Offset = endOffset;
            }
        }
    }
}