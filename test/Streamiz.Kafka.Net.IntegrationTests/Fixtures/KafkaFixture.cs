using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading.Tasks;
using Confluent.Kafka;
using DotNet.Testcontainers.Containers.Builders;
using DotNet.Testcontainers.Containers.Configurations.MessageBrokers;
using DotNet.Testcontainers.Containers.Modules.MessageBrokers;

namespace Streamiz.Kafka.Net.IntegrationTests.Fixtures
{
    public class KafkaFixture
    {
        private readonly KafkaTestcontainer container;
        
        public KafkaFixture()
        {
            container = new TestcontainersBuilder<KafkaTestcontainer>()
                .WithKafka(new KafkaTestcontainerConfiguration())
                .WithImage("registry.hub.docker.com/confluentinc/cp-kafka:7.2.1")
                .WithPortBinding(9092)
                .WithName("kafka-streamiz-integration-tests")
                .Build();
        }

        public string BootstrapServers => container.BootstrapServers;

        private ReadOnlyDictionary<string, string> ConsumerProperties => new(
            new Dictionary<string, string>
            {
                {"bootstrap.servers", container.BootstrapServers},
                {"auto.offset.reset", "earliest"},
                {"group.id", "sample-consumer"}
            }
        );

        private ProducerConfig ProducerProperties => new()
        {
            BootstrapServers = container.BootstrapServers
        };

        private IConsumer<string, byte[]> Consumer()
        {
            var consumer = new ConsumerBuilder<string, byte[]>(ConsumerProperties).Build();
            return consumer;
        }

        internal ConsumeResult<string, byte[]> Consume(string topic, long timeoutMs = 10000)
        {
            var consumer = Consumer();
            
            consumer.Subscribe(topic);
            var result = consumer.Consume(TimeSpan.FromMilliseconds(timeoutMs));
            
            consumer.Unsubscribe();
            consumer.Close();
            consumer.Dispose();
            
            return result;
        }

        internal async Task<DeliveryResult<string, byte[]>> Produce(string topic, string key, byte[] bytes)
        {
            using var producer = new ProducerBuilder<string, byte[]>(ProducerProperties).Build();
            return await producer.ProduceAsync(topic, new Message<string, byte[]>()
            {
                Key = key,
                Value = bytes
            });
        }

        public async Task CreateTopic(string name, int partitions = 1)
        {
            await container.ExecAsync(new List<string>() {
                "/bin/sh",
                "-c",
                $"/usr/bin/kafka-topics --create --bootstrap-server {container.IpAddress}:9092 " +
                    "--replication-factor 1 " +
                    $"--partitions {partitions} " +
                    $"--topic {name}"
            });
        }
        
        public Task DisposeAsync() => container.DisposeAsync().AsTask();

        public Task InitializeAsync() => container.StartAsync();
        
    }
}