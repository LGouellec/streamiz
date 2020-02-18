using Confluent.Kafka;
using kafka_stream_core.Kafka;
using kafka_stream_core.Processors;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core
{
    internal class ProcessorContext
    {
        internal string ApplicationId => Configuration.ApplicationId;
        internal StreamConfig Configuration { get; private set; }
        internal RecordContext RecordContext { get; private set; }
        internal IRecordCollector RecordCollector { get; private set; }
        internal long Timestamp { get; private set; }

        internal ProcessorContext(StreamConfig configuration)
        {
            Configuration = configuration;
        }

        internal void setRecordMetaData()
        {
            // TODO:
        }
    }
}
