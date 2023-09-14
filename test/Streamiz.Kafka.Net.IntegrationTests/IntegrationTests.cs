using System;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.IntegrationTests.Fixtures;
using Streamiz.Kafka.Net.SerDes;
using NUnit.Framework;

namespace Streamiz.Kafka.Net.IntegrationTests
{
    public sealed class IntegrationTests
    {
        private KafkaFixture kafkaFixture;

        [SetUp]
        public void Setup()
        {
            kafkaFixture = new KafkaFixture();
            Console.WriteLine("Starting");
            kafkaFixture.InitializeAsync().Wait(TimeSpan.FromMinutes(5));
            Console.WriteLine("Started");
        }
        
        [TearDown]
        public void TearDown()
        {
            Console.WriteLine("Pending shutdown");
            kafkaFixture.DisposeAsync().Wait(TimeSpan.FromMinutes(5));
            Console.WriteLine("Shutdown");
        }

        [Test]
        public async Task TestSimpleTopology()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test", 
                BootstrapServers = kafkaFixture.BootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            await kafkaFixture.CreateTopic("topic");
            await kafkaFixture.CreateTopic("topic2");

            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic").To("topic2");

            var t = builder.Build();
            var stream = new KafkaStream(t, config);

            await kafkaFixture.Produce(
                "topic", "", Encoding.UTF8.GetBytes("Hello world!")
            );
            
            await stream.StartAsync();

            var result = kafkaFixture.Consume("topic2");
            
            stream.Dispose();
            
            Assert.AreEqual("Hello world!", Encoding.UTF8.GetString(result.Message.Value));
            config.Logger.CreateLogger("ff").LogInformation("test ok");
        }
        
        [Test]
        public async Task TestFilteredTopology()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test", 
                BootstrapServers = kafkaFixture.BootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            await kafkaFixture.CreateTopic("filtered-topic");
            await kafkaFixture.CreateTopic("filtered-topic2");

            var builder = new StreamBuilder();
            builder.Stream<string, string>("filtered-topic")
                .Filter((key, value) => value.StartsWith("a"))
                .To("filtered-topic2");

            var t = builder.Build();
            var stream = new KafkaStream(t, config);

            await kafkaFixture.Produce(
                "filtered-topic", "c", Encoding.UTF8.GetBytes("c Hello world!")
            );
            await kafkaFixture.Produce(
                "filtered-topic", "b", Encoding.UTF8.GetBytes("b Hello world!")
            );
            await kafkaFixture.Produce(
                "filtered-topic", "a", Encoding.UTF8.GetBytes("a Hello world!")
            );

            await stream.StartAsync();

            var result = kafkaFixture.Consume("filtered-topic2");
            
            stream.Dispose();
            
            Assert.AreEqual("a", result.Message.Key);
            Assert.AreEqual("a Hello world!", Encoding.UTF8.GetString(result.Message.Value));
        }
    }
}