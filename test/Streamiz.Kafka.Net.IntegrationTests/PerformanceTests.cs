using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.IntegrationTests.Fixtures;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.IntegrationTests
{
    public class PerformanceTests
    {
        private KafkaFixture kafkaFixture;

        [OneTimeSetUp]
        public void Setup()
        {
            kafkaFixture = new KafkaFixture();
            kafkaFixture.InitializeAsync().Wait(TimeSpan.FromMinutes(5));
        }
        
        [OneTimeTearDown]
        public void TearDown()
        {
            kafkaFixture.DisposeAsync().Wait(TimeSpan.FromMinutes(5));
        }

        private IEnumerable<(string, byte[])> GenerateData(int length)
        {
            for (int i = 0; i < length; ++i)
                yield return new(i.ToString(), Encoding.UTF8.GetBytes("Hello world !"));
        }

        [Test]
        public async Task PerformanceTestStatefull()
        {
            int numberResult = 100000;
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "performance-test", 
                BootstrapServers = kafkaFixture.BootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            await kafkaFixture.CreateTopic("topic");
            await kafkaFixture.CreateTopic("topic2");

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic")
                .GroupByKey()
                .Count(RocksDb
                        .As<String, Int64>("count-store")
                        .WithKeySerdes(new StringSerDes())
                        .WithValueSerdes(new Int64SerDes()))
                .ToStream()
                .To("topic2");

            var t = builder.Build();
            var stream = new KafkaStream(t, config);

            await kafkaFixture.Produce("topic", GenerateData(numberResult));
            
            await stream.StartAsync();
            
            var result = kafkaFixture.ConsumeUntil("topic2", numberResult, 60000);
            
            stream.Dispose();
            
            Assert.IsTrue(result);
            
            // restart with another applicationID, to restore the state store and mesure the latency (first => Implement state store listener)
            
        }

    }
}