using kafka_stream_core.Kafka;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core
{
    internal class ContextOperator
    {
        internal Configuration Configuration { get; private set; }
        internal IKafkaClient Client { get; private set; }

        internal ContextOperator(Configuration configuration)
        {
            Configuration = configuration;
            Client = new KafkaImplementation(configuration);
        }
    }
}
