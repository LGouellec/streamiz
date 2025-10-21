using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.IntegrationTests.Fixtures;
using Streamiz.Kafka.Net.SerDes;
using RocksDb = Streamiz.Kafka.Net.Table.RocksDb;

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

        private async Task PerformanceRestore(ProcessingGuarantee guarantee, int numberOfRecords)
        {
            int numberResult = numberOfRecords;
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "performance-test-restore", 
                BootstrapServers = kafkaFixture.BootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                Guarantee = guarantee,
                LingerMs = 100
            };

            string inputTopic = "input-topic", outputTopic = "output-topic";
        
            await kafkaFixture.CreateTopic(inputTopic, 4);
            await kafkaFixture.CreateTopic(outputTopic, 4);
        
            var builder = new StreamBuilder();
            builder
                .Stream<string, string>(inputTopic)
                .GroupByKey()
                .Count(RocksDb
                    .As<String, Int64>("count-store")
                    .WithKeySerdes(new StringSerDes())
                    .WithValueSerdes(new Int64SerDes()))
                .ToStream()
                .To<StringSerDes, Int64SerDes>(outputTopic);
        
            var t = builder.Build();
            var stream = new KafkaStream(t, config);
        
            kafkaFixture.ProduceRandomData(inputTopic, numberResult);
            
            await stream.StartAsync();
            
            var result = kafkaFixture.ConsumeUntil(outputTopic, numberResult, 60000);
            
            stream.Dispose();

            Assert.IsTrue(result);
            
            Directory.Delete(Path.Combine(config.StateDir, "performance-test-restore"), true);
            
            //restart for restoration
            bool start = false, end = false, batch = false;
            
            stream = new KafkaStream(t, config);
            
            stream.OnRestoreStartEvent += (partition, name, offset, endingOffset) => { start = true; }; 
            stream.OnRestoreEndEvent += (partition, name, total) => { end = true; }; 
            stream.OnRestoreBatchEvent += (partition, name, batchOffset, total) => { batch = true; };
            
            await kafkaFixture.Produce(inputTopic, "new-zealand", Encoding.UTF8.GetBytes("coucou"));
            long startRestoration = DateTime.Now.GetMilliseconds();
            
            await stream.StartAsync();
            
            var resultNZ = kafkaFixture.ConsumeUntil(outputTopic, 1, 60000);
            
            long endRestoration = DateTime.Now.GetMilliseconds();
            
            stream.Dispose();
            
            Assert.IsTrue(resultNZ);
            
            Assert.IsTrue(start);
            Assert.IsTrue(end);
            Assert.IsTrue(batch);
            
            Directory.Delete(Path.Combine(config.StateDir, "performance-test-restore"), true);
            
            Console.WriteLine($"Restoration took approximately {endRestoration - startRestoration} ms");
        }
        
        
        [Test]
        [NonParallelizable]
        public async Task PerformanceTestStateful([Values(200000)] int x)
        {
            int numberResult = 100000;
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "performance-test", 
                BootstrapServers = kafkaFixture.BootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                LingerMs = 25
            };

            string inputTopic = "input-topic", outputTopic = "output-topic";
        
            await kafkaFixture.CreateTopic(inputTopic, 4);
            await kafkaFixture.CreateTopic(outputTopic, 4);
        
            var builder = new StreamBuilder();
            builder
                .Stream<string, string>(inputTopic)
                .GroupByKey()
                .Count(RocksDb
                    .As<String, Int64>("count-store")
                    .WithKeySerdes(new StringSerDes())
                    .WithValueSerdes(new Int64SerDes()))
                .ToStream()
                .To<StringSerDes, Int64SerDes>(outputTopic);
        
            var t = builder.Build();
            var stream = new KafkaStream(t, config);
        
            kafkaFixture.ProduceRandomData(inputTopic, numberResult);
            
            await stream.StartAsync();
            
            var result = kafkaFixture.ConsumeUntil(outputTopic, numberResult, 60000);
            
            stream.Dispose();
            
            Assert.IsTrue(result);
            
            Directory.Delete(Path.Combine(config.StateDir, "performance-test"), true);
        }
        
        [Test]
        [NonParallelizable]
        public async Task PerformanceRestoreTestStateful([Values(200000)] int x)
        {
            await PerformanceRestore(ProcessingGuarantee.AT_LEAST_ONCE, x);
        }

        [Test]
        [NonParallelizable]
        public async Task PerformanceRestoreEOSTestStateful([Values(200000)] int x)
        {
            await PerformanceRestore(ProcessingGuarantee.EXACTLY_ONCE, x);
        }
    }
}