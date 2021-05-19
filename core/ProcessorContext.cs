using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.SerDes.Internal;
using System.IO;

namespace Streamiz.Kafka.Net
{
    /// <summary>
    /// Processor context
    /// </summary>
    public class ProcessorContext
    {
        internal static readonly ByteArraySerDes BYTEARRAY_VALUE_SERDES = new ByteArraySerDes();
        internal static readonly BytesSerDes BYTES_KEY_SERDES = new BytesSerDes();

        internal AbstractTask Task { get; private set; }
        internal SerDesContext SerDesContext { get; private set; }
        internal IStreamConfig Configuration { get; private set; }
        internal IRecordContext RecordContext { get; private set; }
        internal IRecordCollector RecordCollector { get; private set; }
        internal IStateManager States { get; private set; }
        internal bool FollowMetadata { get; set; }

        /// <summary>
        /// Current application id
        /// </summary>
        public string ApplicationId => Configuration.ApplicationId;

        /// <summary>
        /// Current timestamp of record processing
        /// </summary>
        public long Timestamp => RecordContext.Timestamp;

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
        public string StateDir => $"{Path.Combine(Configuration.StateDir, Configuration.ApplicationId, Id.ToString())}";

        internal ProcessorContext(AbstractTask task, IStreamConfig configuration, IStateManager stateManager)
        { 
            Task = task;
            Configuration = configuration;
            States = stateManager;

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

        internal void ChangeTimestamp(long ts)
        {
            RecordContext.ChangeTimestamp(ts);
        }

        internal virtual IStateStore GetStateStore(string storeName) => States.GetStore(storeName);

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

        /// <summary>
        /// Requests a commit
        /// </summary>
        public virtual void Commit() => Task.RequestCommit();
    }
}