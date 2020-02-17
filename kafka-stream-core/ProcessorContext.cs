using Confluent.Kafka;
using kafka_stream_core.Kafka;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core
{
    internal class ProcessorContext
    {
        internal Configuration Configuration { get; private set; }
        internal IConsumer<byte[], byte[]> Consumer { get; private set; }
        internal IProducer<byte[], byte[]> Producer { get; private set; }

        internal ProcessorContext(
            Configuration configuration,
            IConsumer<byte[], byte[]> consumer,
            IProducer<byte[], byte[]> producer)
        {
            Configuration = configuration;
            Consumer = consumer;
            Producer = producer;
        }
    }
}
