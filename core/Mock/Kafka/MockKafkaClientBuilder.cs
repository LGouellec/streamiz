using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Kafka.Internal;

namespace Streamiz.Kafka.Net.Mock.Kafka
{
    internal class MockKafkaClientBuilder : DefaultKafkaClientBuilder
    {
        private MockKafkaSupplier mockKafkaSupplier = new MockKafkaSupplier();

        internal class MockConsumerBuilder : ConsumerBuilder<byte[], byte[]>
        {
            private readonly MockKafkaSupplier mockKafkaSupplier;

            public MockConsumerBuilder(MockKafkaSupplier mockKafkaSupplier, IEnumerable<KeyValuePair<string, string>> config) 
                : base(config)
            {
                this.mockKafkaSupplier = mockKafkaSupplier;
            }
            
            public override IConsumer<byte[], byte[]> Build()
            {
                if (PartitionsAssignedHandler == null)
                    return mockKafkaSupplier.GetConsumer(new ConsumerConfig(Config.ToDictionary(k => k.Key, k => k.Value)), null);
                else
                {
                    return mockKafkaSupplier.GetConsumer(
                        new ConsumerConfig(Config.ToDictionary(k => k.Key, k => k.Value)), null);
                }
            }
        }
        
        public override ConsumerBuilder<byte[], byte[]> GetConsumerBuilder(ConsumerConfig config)
        {
            return base.GetConsumerBuilder(config);
        }

        public override ProducerBuilder<byte[], byte[]> GetProducerBuilder(ProducerConfig config)
        {
            return base.GetProducerBuilder(config);
        }

        public override AdminClientBuilder GetAdminBuilder(AdminClientConfig config)
        {
            return base.GetAdminBuilder(config);
        }
    }
}