using System;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.SerDes.Internal;
using System.IO;
using Streamiz.Kafka.Net.Metrics;

namespace Streamiz.Kafka.Net
{
    /// <summary>
    /// Processor context
    /// </summary>
    public class ProcessorContext
    {
        private static readonly ByteArraySerDes BYTEARRAY_VALUE_SERDES = new();
        private static readonly BytesSerDes BYTES_KEY_SERDES = new();

        internal virtual IProcessor CurrentProcessor { get; set; }
        internal AbstractTask Task { get; }
        internal virtual SerDesContext SerDesContext { get; }
        internal virtual IStreamConfig Configuration { get; }
        internal virtual IRecordContext RecordContext { get; private set; }
        internal IRecordCollector RecordCollector { get; private set; }
        internal virtual IStateManager States { get; }
        internal virtual bool FollowMetadata { get; set; }
        
        /// <summary>
        /// Return the <see cref="StreamMetricsRegistry"/> instance.
        /// </summary>
        public virtual StreamMetricsRegistry Metrics { get; private set; }

        /// <summary>
        /// Current application id
        /// </summary>
        public string ApplicationId => Configuration.ApplicationId;

        /// <summary>
        /// Current timestamp of record processing
        /// </summary>
        public virtual long Timestamp => RecordContext.Timestamp;

        /// <summary>
        /// Current topic of record processing
        /// </summary>
        public string Topic => RecordContext.Topic;

        /// <summary>
        /// Current offset of record processing
        /// </summary>
        public long Offset => RecordContext.Offset;

        /// <summary>
        /// Current partition of record processing
        /// </summary>
        public Partition Partition => RecordContext.Partition;

        /// <summary>
        /// Current task id of processing
        /// </summary>
        public virtual TaskId Id => Task.Id;

        /// <summary>
        /// Returns the state directory for the partition.
        /// </summary>
        public virtual string StateDir => $"{Path.Combine(Configuration.StateDir, Configuration.ApplicationId, Id.ToString())}";

        // FOR TESTING
        internal ProcessorContext()
        {
        }

        internal ProcessorContext(AbstractTask task, IStreamConfig configuration, IStateManager stateManager,
            StreamMetricsRegistry streamMetricsRegistry)
        { 
            Task = task;
            Configuration = configuration;
            States = stateManager;
            Metrics = streamMetricsRegistry;
            
            SerDesContext = new SerDesContext(configuration);
        }

        internal ProcessorContext UseRecordCollector(IRecordCollector collector)
        {
            if (collector != null)
                RecordCollector = collector;
            return this;
        }

        internal void SetRecordMetaData(ConsumeResult<byte[], byte[]> result)
        {
            RecordContext = new RecordContext(result);
        }

        internal void SetUnknownRecordMetaData(long ts)
        {
            RecordContext = new RecordContext();
            RecordContext.ChangeTimestamp(ts);
        }

        /// <summary>
        /// Changes the record timestamp to <paramref name="ts"/>
        /// </summary>
        /// <param name="ts">The Unix timestamp value to be used by the record context.</param>
        public void ChangeTimestamp(long ts)
        {
            RecordContext.ChangeTimestamp(ts);
        }

        /// <summary>
        /// Change current list of headers
        /// </summary>
        /// <param name="headers">new headers</param>
        public void SetHeaders(Headers headers) => RecordContext.SetHeaders(headers);
        
        /// <summary>
        /// Get the state store given the store name.
        /// </summary>
        /// <param name="storeName">The store name</param>
        /// <returns>The state store instance</returns>
        public virtual IStateStore GetStateStore(string storeName) => States.GetStore(storeName);

        internal void Register(IStateStore store, StateRestoreCallback callback)
        {
            States.Register(store, callback);
        }

        internal void Log(string storeName, Bytes key, byte[] value, long timestamp)
        {
            var topicPartition = States.GetRegisteredChangelogPartitionFor(storeName);

            RecordCollector.Send(
                topicPartition.Topic,
                key,
                value,
                null,
                topicPartition.Partition,
                timestamp,
                BYTES_KEY_SERDES,
                BYTEARRAY_VALUE_SERDES);
        }

        internal string ChangelogFor(string storeName)
            => States.ChangelogFor(storeName);
    }
}