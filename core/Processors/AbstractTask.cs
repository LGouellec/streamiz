using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.Stream.Internal;
using log4net;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Streamiz.Kafka.Net.Processors
{
    internal abstract class AbstractTask : ITask
    {
        protected readonly IStreamConfig configuration;
        protected IConsumer<byte[], byte[]> consumer;
        protected bool taskInitialized;
        protected bool commitNeeded;
        protected IStateManager stateMgr;
        protected ILog log;
        protected readonly string logPrefix = "";

        protected AbstractTask(TaskId id, TopicPartition partition, ProcessorTopology topology, IConsumer<byte[], byte[]> consumer, IStreamConfig config)
        {
            this.log = Logger.GetLogger(this.GetType());
            logPrefix = $"stream-task[{id.Topic}|{id.Partition}] ";

            Partition = partition;
            Id = id;
            Topology = topology;

            this.consumer = consumer;
            this.configuration = config;

            this.stateMgr = new ProcessorStateManager(id, partition);
        }

        public ProcessorTopology Topology { get; }

        public ProcessorContext Context { get; protected set; }

        public TaskId Id { get; }

        public TopicPartition Partition { get; }

        public ICollection<TopicPartition> ChangelogPartitions { get; internal set; }

        public bool HasStateStores => false;

        public string ApplicationId => configuration.ApplicationId;

        public bool CommitNeeded => commitNeeded;

        public bool IsClosed { get; protected set; }

        #region Abstract

        public abstract bool CanProcess { get; }
        public abstract void Close();
        public abstract void Commit();
        public abstract IStateStore GetStore(string name);
        public abstract void InitializeTopology();
        public abstract bool InitializeStateStores();
        public abstract void Resume();
        public abstract void Suspend();

        #endregion

        protected void RegisterStateStores()
        {
            if (!Topology.StateStores.Any() && !Topology.GlobalStateStores.Any())
            {
                return;
            }

            log.Debug($"{logPrefix}Initializing state stores");

            foreach (var kv in Topology.StateStores)
            {
                var store = kv.Value;
                log.Debug($"{logPrefix}Initializing store {kv.Key}");
                store.Init(Context, store);
            }

            foreach(var kv in Topology.GlobalStateStores.Where(k => !Topology.StateStores.ContainsKey(k.Key)))
            {
                var store = kv.Value;
                log.Debug($"{logPrefix}Initializing store {kv.Key}");
                store.Init(Context, store);
            }
        }

        protected virtual void FlushState()
        {
            try
            {
                stateMgr.Flush();
            }
            catch (Exception e)
            {
                log.Error($"{logPrefix}Error during flush state store with exception :", e);
                throw;
            }
        }

        protected void CloseStateManager()
        {
            log.Debug($"{logPrefix}Closing state manager");
            try
            {
                stateMgr.Close();
            }
            catch (Exception e)
            {
                log.Error($"{logPrefix}Error during closing state store with exception :", e);
                throw;
            }
        }
    }
}
