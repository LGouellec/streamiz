using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Testcontainers.Kafka;
using Testcontainers;

namespace Streamiz.Kafka.Net.IntegrationTests.Fixtures
{
    public class KafkaFixture
    {
        private readonly KafkaContainer container;

        public KafkaFixture()
        {
            container = new KafkaBuilder()
                .WithImage("registry.hub.docker.com/confluentinc/cp-kafka:7.4.0")
                .WithPortBinding(9092)
                .WithName("kafka-streamiz-integration-tests")
                .Build();
        }

        public string BootstrapServers => container.GetBootstrapAddress();

        private ReadOnlyDictionary<string, string> ConsumerProperties => new(
            new Dictionary<string, string>
            {
                {"bootstrap.servers", BootstrapServers},
                {"auto.offset.reset", "earliest"},
                {"group.id", "sample-consumer"}
            }
        );

        private ProducerConfig ProducerProperties => new()
        {
            BootstrapServers = BootstrapServers
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

        internal bool ConsumeUntil(string topic, int size, long timeoutMs)
        {
            bool sizeCompleted = false;
            int numberRecordsConsumed = 0;

            var consumer = Consumer();
            consumer.Subscribe(topic);

            DateTime dt = DateTime.Now, now;
            TimeSpan ts = TimeSpan.FromMilliseconds(timeoutMs);
            do
            {
                var r = consumer.Consume(ts);
                now = DateTime.Now;
                if (r != null)
                    ++numberRecordsConsumed;
                else
                {
                    Thread.Sleep(10);

                    if (ts.TotalMilliseconds == 0) // if not enough time, do not call Consume(0); => break;
                        break;
                }

                if (numberRecordsConsumed >= size) // if the batch is full, break;
                    break;
            } while (dt.Add(ts) > now);

            consumer.Unsubscribe();
            consumer.Close();
            consumer.Dispose();

            if (numberRecordsConsumed == size)
                sizeCompleted = true;

            return sizeCompleted;
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

        internal async Task Produce(string topic, IEnumerable<(string, byte[])> records)
        {
            var newPropers = new ProducerConfig(ProducerProperties);
            newPropers.LingerMs = 200;

            using var producer = new ProducerBuilder<string, byte[]>(newPropers).Build();
            foreach (var r in records)
                await producer.ProduceAsync(topic, new Message<string, byte[]>()
                {
                    Key = r.Item1,
                    Value = r.Item2
                });
            producer.Flush();
        }

        internal void ProduceRandomData(string topic, int numberResult)
        {
            var newPropers = new ProducerConfig(ProducerProperties);
            newPropers.LingerMs = 200;
            newPropers.Acks = Acks.None;
            newPropers.SocketSendBufferBytes = 33554432;
            newPropers.CompressionType = CompressionType.Snappy;
            newPropers.QueueBufferingMaxMessages = 1000000;

            var rd = new Random();
            string[] key = {"France", "Italia", "Spain", "Uk", "Germany", "Portugal"};

            using var producer = new ProducerBuilder<string, byte[]>(newPropers).Build();
            
            for (int i = 0; i < numberResult; ++i)
                producer.Produce(topic, new Message<string, byte[]>()
                {
                    Key = key[rd.NextInt64(0, key.Length)],
                    Value = Encoding.UTF8.GetBytes("Hey !" + i)
                });

            producer.Flush();
        }

        public async Task CreateTopic(string name, int partitions = 1)
        {
            await container.ExecAsync(new List<string>()
            {
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