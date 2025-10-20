using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock.Kafka;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Linq;
using System.Threading;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Tests.Helpers;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class StreamThreadRebalanceTests
    {
        private readonly CancellationTokenSource token1 = new System.Threading.CancellationTokenSource();
        private readonly CancellationTokenSource token2 = new System.Threading.CancellationTokenSource();

        private readonly StreamConfig<StringSerDes, StringSerDes> config =
            new StreamConfig<StringSerDes, StringSerDes>();

        private MockKafkaSupplier mockKafkaSupplier;
        private StreamThread thread1;
        private StreamThread thread2;

        [SetUp]
        public void Init()
        {
            config.ApplicationId = "test-stream-thread";
            config.StateDir = Guid.NewGuid().ToString();
            config.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            config.PollMs = 10;

            mockKafkaSupplier = new MockKafkaSupplier(4);

            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic").To("topic2");

            var topo = builder.Build();

            var processId = Guid.NewGuid();
            
            thread1 = StreamThread.Create(
                "thread-0", processId, "c0",
                topo.Builder, new StreamMetricsRegistry(), config,
                mockKafkaSupplier, mockKafkaSupplier.GetAdmin(config.ToAdminConfig("admin")),
                new StatestoreRestoreManager(null),
                0) as StreamThread;

            thread2 = StreamThread.Create(
                "thread-1", processId, "c1",
                topo.Builder, new StreamMetricsRegistry(), config,
                mockKafkaSupplier, mockKafkaSupplier.GetAdmin(config.ToAdminConfig("admin")),
                new StatestoreRestoreManager(null),
                1) as StreamThread;
        }

        [TearDown]
        public void Dispose()
        {
            token1.Cancel();
            token2.Cancel();
            thread1.Dispose();
            thread2.Dispose();
            mockKafkaSupplier.Destroy();
        }

        [Test]
        public void StreamThreadNormalWorkflowWithRebalancing()
        {
            var consumeConfig = config.Clone();
            consumeConfig.ApplicationId = "consume-test";

            var serdes = new StringSerDes();
            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic").To("topic2");

            var topo = builder.Build();

            var producer = mockKafkaSupplier.GetProducer(consumeConfig.ToProducerConfig());
            var consumer = mockKafkaSupplier.GetConsumer(consumeConfig.ToConsumerConfig("test-consum"), null);
            consumer.Subscribe("topic2");

            thread1.Start(token1.Token);
            thread2.Start(token2.Token);

            producer.Produce("topic", new Confluent.Kafka.Message<byte[], byte[]>
            {
                Key = serdes.Serialize("key1", new SerializationContext()),
                Value = serdes.Serialize("coucou", new SerializationContext())
            });
            //WAIT STREAMTHREAD PROCESS MESSAGE

            AssertExtensions.WaitUntil(() => thread1.ActiveTasks.Count() == 2, TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(10));
            AssertExtensions.WaitUntil(() => thread2.ActiveTasks.Count() == 2, TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(10));

            // 2 CONSUMER FOR THE SAME GROUP ID => TOPIC WITH 4 PARTITIONS
            Assert.AreEqual(2, thread1.ActiveTasks.Count());
            Assert.AreEqual(2, thread2.ActiveTasks.Count());

            var message = consumer.Consume(100);

            Assert.AreEqual("key1", serdes.Deserialize(message.Message.Key, new SerializationContext()));
            Assert.AreEqual("coucou", serdes.Deserialize(message.Message.Value, new SerializationContext()));
            // TODO : Finish test with a real cluster Assert.AreEqual(expectedStates, allStates);
        }

    }
}