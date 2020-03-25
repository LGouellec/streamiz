using Confluent.Kafka;
using kafka_stream_core.Crosscutting;
using kafka_stream_core.Processors.Internal;
using kafka_stream_core.Stream.Internal;
using log4net;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace kafka_stream_core.Processors
{
    internal abstract class AbstractTask : ITask
    {
        private readonly IStreamConfig configuration;
        protected IConsumer<byte[], byte[]> consumer;
        protected bool taskInitialized;
        protected bool taskClosed;
        protected bool commitNeeded;
        protected StateManager stateMgr;
        protected ILog log;

        internal AbstractTask(TaskId id, TopicPartition partition, ProcessorTopology topology, IConsumer<byte[], byte[]> consumer, IStreamConfig config)
        {
            this.log = Logger.GetLogger(this.GetType());

            Partition = partition;
            Id = id;
            Topology = topology;

            this.consumer = consumer;
            this.configuration = config;

            this.stateMgr = new ProcessorStateManager(id, partition);
        }

        public ProcessorTopology Topology { get; }

        public ProcessorContext Context { get; internal set; }

        public TaskId Id { get; }

        public TopicPartition Partition { get; }

        public ICollection<TopicPartition> ChangelogPartitions { get; internal set; }

        public bool HasStateStores => false;

        public string ApplicationId => configuration.ApplicationId;

        public bool CommitNeeded => commitNeeded;

        #region Abstract

        public abstract bool CanProcess { get; }
        public abstract void Close();
        public abstract void Commit();
        public abstract StateStore GetStore(string name);
        public abstract void InitializeTopology();
        public abstract bool InitializeStateStores();
        public abstract void Resume();
        public abstract void Suspend();

        #endregion

        protected void RegisterStateStores()
        {
            if (!Topology.StateStores.Any())
            {
                return;
            }

            log.Debug("Initializing state stores");

            foreach (var kv in Topology.StateStores)
            {
                var store = kv.Value;
                log.Debug($"Initializing store {kv.Key}");
                store.init(Context, store);
            }
        }
    }
}
