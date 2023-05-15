using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Stream.Internal;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Processors.Public;

namespace Streamiz.Kafka.Net.Processors
{
    internal abstract class AbstractTask : ITask
    {
        protected readonly IStreamConfig configuration;
        protected IConsumer<byte[], byte[]> consumer;
        protected bool taskInitialized;
        protected bool commitNeeded = false;
        protected bool commitRequested = false;
        protected ProcessorStateManager stateMgr;
        protected ILogger log;
        protected readonly string logPrefix = "";
        protected TaskState state = TaskState.CREATED;
        private Dictionary<TopicPartition, long> offsetLastSnapshotOffset;

        // For testing
        internal AbstractTask() { }

        protected AbstractTask(TaskId id, IEnumerable<TopicPartition> partition, ProcessorTopology topology, IConsumer<byte[], byte[]> consumer, IStreamConfig config, IChangelogRegister changelogRegister)
        {
            log = Logger.GetLogger(GetType());
            logPrefix = $"stream-task[{id.Id}|{id.Partition}] ";

            Partition = partition;
            Id = id;
            Topology = topology;

            this.consumer = consumer;
            configuration = config;

            var offsetCheckpointMngt = config.OffsetCheckpointManager
                ?? new OffsetCheckpointFile(Path.Combine(config.StateDir, config.ApplicationId, $"{id.Id}-{id.Partition}"));
            offsetCheckpointMngt.Configure(config, id);

            stateMgr = new ProcessorStateManager(
                id,
                partition,
                topology.StoresToTopics,
                changelogRegister,
                offsetCheckpointMngt);
        }

        public TaskState State => state;

        public ProcessorTopology Topology { get; }

        public ProcessorContext Context { get; protected set; }

        public virtual TaskId Id { get; }

        public IEnumerable<TopicPartition> Partition { get; }

        public ICollection<TopicPartition> ChangelogPartitions => stateMgr.ChangelogPartitions;

        public bool HasStateStores => Topology.StateStores.Count != 0;

        public string ApplicationId => configuration.ApplicationId;

        public bool CommitNeeded => commitNeeded;
        public bool CommitRequested => commitRequested;

        public bool IsClosed { get; protected set; }

        public void RequestCommit() => commitRequested = true;

        public bool IsPersistent
             => stateMgr.StateStoreNames
                    .Select(n => stateMgr.GetStore(n))
                    .Any((s) => s.Persistent);

        #region Abstract
        public abstract IDictionary<TopicPartition, long> PurgeOffsets { get; }
        public abstract PartitionGrouper Grouper { get; }
        public abstract bool CanProcess(long now);
        public abstract void Close();
        public abstract void Commit();
        public abstract IStateStore GetStore(string name);
        public abstract void InitializeTopology();
        public abstract void RestorationIfNeeded();
        public abstract bool InitializeStateStores();
        public abstract void Resume();
        public abstract void Suspend();
        public abstract void MayWriteCheckpoint(bool force = false);
        public abstract TaskScheduled RegisterScheduleTask(TimeSpan interval, PunctuationType punctuationType, Action<long> punctuator);
        #endregion

        protected void TransitTo(TaskState newState)
        {
            if (state.IsValidTransition(newState))
            {
                log.LogInformation($"{logPrefix}Task {Id} state transition from {state} to {newState}");
                state = newState;
            }
            else
            {
                throw new IllegalStateException($"Invalid transition from {state} to {newState}");
            }
        }

        protected void RegisterStateStores()
        {
            if (!Topology.StateStores.Any() && !Topology.GlobalStateStores.Any())
            {
                return;
            }

            log.LogDebug("{LogPrefix}Initializing state stores", logPrefix);

            foreach (var kv in Topology.StateStores)
            {
                var store = kv.Value;
                log.LogDebug("{LogPrefix}Initializing store {Key}", logPrefix, kv.Key);
                store.Init(Context, store);
            }

            stateMgr.RegisterGlobalStateStores(Topology.GlobalStateStores);
        }

        protected virtual void FlushState()
        {
            try
            {
                stateMgr.Flush();
            }
            catch (Exception e)
            {
                log.LogError(e, "{LogPrefix}Error during flush state store with exception :", logPrefix);
                throw;
            }
        }

        protected void CloseStateManager()
        {
            log.LogDebug("{LogPrefix}Closing state manager", logPrefix);
            try
            {
                stateMgr.Close();
            }
            catch (Exception e)
            {
                log.LogError(e, "{LogPrefix}Error during closing state store with exception :", logPrefix);
                throw;
            }
        }

        protected void WriteCheckpoint(bool force = false)
        {
            var changelogOffsets = stateMgr.ChangelogOffsets;
            if(StateManagerTools.CheckpointNeed(force, offsetLastSnapshotOffset, changelogOffsets))
            {
                stateMgr.Flush();
                stateMgr.Checkpoint();
                offsetLastSnapshotOffset = new Dictionary<TopicPartition, long>(changelogOffsets);
            }
        }
    }
}
