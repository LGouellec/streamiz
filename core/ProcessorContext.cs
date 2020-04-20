using Confluent.Kafka;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net
{
    public class ProcessorContext
    {
        internal string ApplicationId => Configuration.ApplicationId;
        internal IStreamConfig Configuration { get; private set; }
        internal IRecordContext RecordContext { get; private set; }
        internal IRecordCollector RecordCollector { get; private set; }
        internal IStateManager States { get; private set; }

        internal long Timestamp => RecordContext.Timestamp;
        internal string Topic => RecordContext.Topic;
        internal long Offset => RecordContext.Offset;
        internal Partition Partition => RecordContext.Partition;

        internal ProcessorContext(IStreamConfig configuration, IStateManager stateManager)
        {
            Configuration = configuration;
            States = stateManager;
        }

        internal ProcessorContext UseRecordCollector(IRecordCollector collector)
        {
            if (collector != null)
                RecordCollector = collector;
            return this;
        }

        internal void SetRecordMetaData(ConsumeResult<byte[], byte[]> result)
        {
            this.RecordContext = new RecordContext(result);
        }

        internal IStateStore GetStateStore(string storeName) => States.GetStore(storeName);

        internal void Register(IStateStore store, StateRestoreCallback callback)
        {
            States.Register(store, callback);
        }
    }
}
