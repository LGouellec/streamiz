using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Mock.Kafka;

namespace Streamiz.Kafka.Net.Mock.Sync
{
    internal class SyncProducer : MockProducer
    {
        private ProducerConfig config;

        public SyncProducer(ProducerConfig config)
            :base(config.ClientId)
        {
            this.config = config;
        }
    }
}
