using System;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Tests.Helpers;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class RecordCollectorTests
    {
        private RecordCollector collector;
        private StreamConfig config;
        
        [SetUp]
        public void Init()
        {
            config = new StreamConfig();
            config.ApplicationId = "collector-unit-test";
            config.ProductionExceptionHandler = (_) => ProductionExceptionHandlerResponse.RETRY;
            collector = new RecordCollector(
                "test-collector",
                config,
                new TaskId {Id = 0, Partition = 0},
                NoRunnableSensor.Empty);
        }

        [TearDown]
        public void Dispose()
        {
            collector.Close();
        }
        
        [Test]
        public void MultipleRetry()
        {
            var options = new ProducerSyncExceptionOptions
            {
                NumberOfError = 5,
            };
            var supplier = new ProducerSyncExceptionSupplier(options);
            var producer = supplier.GetProducer(config.ToProducerConfig("producer-1"));
            
            collector.Init(ref producer);
            
            // first send => retry queue
            try
            {
                collector.Send(
                    "input-topic",
                    "key1",
                    "value1",
                    new Headers(),
                    DateTime.Now.GetMilliseconds(),
                    new StringSerDes(),
                    new StringSerDes());
            }
            catch { }

            try
            {
                collector.Send(
                    "input-topic",
                    "key1",
                    "value2",
                    new Headers(),
                    DateTime.Now.GetMilliseconds(),
                    new StringSerDes(),
                    new StringSerDes());
            }
            catch { }

            var consumer = supplier.GetConsumer(config.ToConsumerConfig("consumer"), null);
            consumer.Subscribe("input-topic");
            int count = 0;
            ConsumeResult<byte[], byte[]> r = null;
            
            while((r = consumer.Consume(100)) != null)
                ++count;
            
            Assert.AreEqual(2, count);

            consumer.Close();
            consumer.Dispose();
        }
        
        [Test]
        public void TestWithFatalError()
        {
            var options = new ProducerSyncExceptionOptions
            {
                IsFatal = true
            };
            var supplier = new ProducerSyncExceptionSupplier(options);
            var producer = supplier.GetProducer(config.ToProducerConfig("producer-1"));
            
            collector.Init(ref producer);
            
            // first send => retry queue
            try
            {
                collector.Send(
                    "input-topic",
                    "key1",
                    "value1",
                    new Headers(),
                    DateTime.Now.GetMilliseconds(),
                    new StringSerDes(),
                    new StringSerDes());
            }
            catch { }

            bool error = false;
            
            try
            {
                collector.Send(
                    "input-topic",
                    "key1",
                    "value2",
                    new Headers(),
                    DateTime.Now.GetMilliseconds(),
                    new StringSerDes(),
                    new StringSerDes());
            }
            catch
            {
                error = true;
            }
            
            Assert.IsTrue(error);
            
            var consumer = supplier.GetConsumer(config.ToConsumerConfig("consumer"), null);
            consumer.Subscribe("input-topic");
            int count = 0;
            ConsumeResult<byte[], byte[]> r = null;
            
            while((r = consumer.Consume(100)) != null)
                ++count;
            
            Assert.AreEqual(0, count);

            consumer.Close();
            consumer.Dispose();
        }

        [Test]
        public void TestWithRecoverableError()
        {
            var options = new ProducerSyncExceptionOptions
            {
                IsRecoverable = true
            };
            var supplier = new ProducerSyncExceptionSupplier(options);
            var producer = supplier.GetProducer(config.ToProducerConfig("producer-1"));
            
            collector.Init(ref producer);
            
            // first send => retry queue
            try
            {
                collector.Send(
                    "input-topic",
                    "key1",
                    "value1",
                    new Headers(),
                    DateTime.Now.GetMilliseconds(),
                    new StringSerDes(),
                    new StringSerDes());
            }
            catch { }

            bool error = false;
            try
            {
                collector.Send(
                    "input-topic",
                    "key1",
                    "value2",
                    new Headers(),
                    DateTime.Now.GetMilliseconds(),
                    new StringSerDes(),
                    new StringSerDes());
            }
            catch
            {
                error = true;
            }

            Assert.IsTrue(error);
            var consumer = supplier.GetConsumer(config.ToConsumerConfig("consumer"), null);
            consumer.Subscribe("input-topic");
            int count = 0;
            ConsumeResult<byte[], byte[]> r = null;
            
            while((r = consumer.Consume(100)) != null)
                ++count;
            
            Assert.AreEqual(0, count);

            consumer.Close();
            consumer.Dispose();
        }
    }
}