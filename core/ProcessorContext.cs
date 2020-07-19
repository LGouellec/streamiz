﻿using Confluent.Kafka;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net
{
    /// <summary>
    /// Processor context
    /// </summary>
    public class ProcessorContext
    {
        internal SerDesContext SerDesContext { get; private set; }
        internal IStreamConfig Configuration { get; private set; }
        internal IRecordContext RecordContext { get; private set; }
        internal IRecordCollector RecordCollector { get; private set; }
        internal IStateManager States { get; private set; }

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

        internal ProcessorContext(IStreamConfig configuration, IStateManager stateManager)
        {
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

        internal IStateStore GetStateStore(string storeName) => States.GetStore(storeName);

        internal void Register(IStateStore store, StateRestoreCallback callback)
        {
            States.Register(store, callback);
        }
    }
}
