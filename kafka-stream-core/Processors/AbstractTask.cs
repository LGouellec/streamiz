using Confluent.Kafka;
using kafka_stream_core.Processors.Internal;
using kafka_stream_core.Stream.Internal;
using System;
using System.Collections.Generic;
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

        internal AbstractTask(TaskId id, TopicPartition partition, ProcessorTopology topology, IConsumer<byte[], byte[]> consumer, IStreamConfig config)
        {
            Partition = partition;
            Id = id;
            Topology = topology;
            
            this.consumer = consumer;
            this.configuration = config;
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

        public abstract void Resume();
        public abstract void Suspend();

        #endregion
    }
}
