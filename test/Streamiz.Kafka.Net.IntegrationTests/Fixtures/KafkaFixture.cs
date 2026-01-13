using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Streamiz.Kafka.Net.IntegrationTests.Seed;
using Streamiz.Kafka.Net.SerDes;
using Testcontainers.Kafka;

namespace Streamiz.Kafka.Net.IntegrationTests.Fixtures
{
    public class KafkaFixture
    {
        private readonly KafkaContainer container;

        public KafkaFixture()
        {
            container = new ApacheKafkaBuilder()
                //.WithImage(ApacheKafkaBuilder.APACHE_KAFKA_NATIVE_IMAGE_NAME)
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

        internal Task ProduceContinuously<K, V>(
            string topic,
            ISeeder<K> keySeeder, ISeeder<V> valueSeeder,
            Action<K, V> alterValueAfterGeneration,
            ISerDes<K> keySerdes, ISerDes<V> valueSerdes,
            TimeSpan pauseBetweenEachIteration,
            KafkaStats stats,
            CancellationToken token)
        {
            var task = Task.Factory.StartNew(async () =>
            {
                using var producer = new ProducerBuilder<byte[], byte[]>(ProducerProperties).Build();
                while (!token.IsCancellationRequested)
                {
                    var key = keySeeder.SeedOnce();
                    var value = valueSeeder.SeedOnce();
                    alterValueAfterGeneration(key, value);
                    
                    var keyBytes = keySerdes.Serialize(key, new SerializationContext(MessageComponentType.Key, topic));
                    var valueBytes = valueSerdes.Serialize(value, new SerializationContext(MessageComponentType.Value, topic));

                    ++stats.SentMessages;
                    var result = await producer.ProduceAsync(topic, new Message<byte[], byte[]>()
                    {
                        Key = keyBytes,
                        Value = valueBytes
                    });
                    
                    if (result.Status == PersistenceStatus.Persisted)
                        ++stats.MessagesCorrectlyPersisted;
                    
                    Thread.Sleep(pauseBetweenEachIteration);
                }
            }, token);
            
            return task;
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
            ClientConfig config = new ClientConfig();
            config.BootstrapServers = BootstrapServers;
            config.ClientId = "create-topic-client";
            AdminClientBuilder builder = new AdminClientBuilder(config);
            
            using IAdminClient client = builder.Build();
            var metadata = client.GetMetadata(name, TimeSpan.FromSeconds(10));

            var tpm = metadata.Topics
                .FirstOrDefault(t => t.Topic.Contains(name) && !t.Error.IsBrokerError);
                    
            if(tpm != null)
                await client.DeleteTopicsAsync(new List<string> { name });
            try
            {
                await client.CreateTopicsAsync(new List<TopicSpecification>
                {
                    new()
                    {
                        Name = name,
                        NumPartitions = partitions
                    }
                });
            }
            catch (CreateTopicsException e)
            {
                Console.WriteLine(e.Message);
                 /* nothing */
            }
        }

        public Task DisposeAsync() => container.DisposeAsync().AsTask();

        public Task InitializeAsync() => container.StartAsync();
    }
}