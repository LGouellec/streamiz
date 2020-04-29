using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Mock.Kafka;

namespace Streamiz.Kafka.Net.Mock.Sync
{
    internal class SyncConsumer : MockConsumer
    {
        private ConsumerConfig config;

        public SyncConsumer(ConsumerConfig config)
            : base(config.GroupId, config.ClientId)
        {
            this.config = config;
        }
    }
}
