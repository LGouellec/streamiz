using NUnit.Framework;
using Streamiz.Kafka.Net.Kafka.Internal;
using System;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class DefaultKafkaClientSupplierTest
    {
        private static StreamConfig GetConfig()
        {
            var config = new StreamConfig();
            config.ApplicationId = "test-DefaultKafkaClientSupplierTest";
            return config;
        }

        [Test]
        public void ShouldArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => new DefaultKafkaClientSupplier(null, null));
        }

        [Test]
        public void CreateAdminClient()
        {
            var config = GetConfig();
            var supplier = new DefaultKafkaClientSupplier(new KafkaLoggerAdapter(config), config);
            var adminClient = supplier.GetAdmin(config.ToAdminConfig("admin"));
            Assert.IsNotNull(adminClient);
            Assert.AreEqual("admin", adminClient.Name.Split("#")[0]);
        }

        [Test]
        public void CreateConsumerClient()
        {
            var config = GetConfig();
            var supplier = new DefaultKafkaClientSupplier(new KafkaLoggerAdapter(config), config);
            var consumer = supplier.GetConsumer(config.ToConsumerConfig("consume"), new StreamsRebalanceListener(null));
            Assert.IsNotNull(consumer);
            Assert.AreEqual("consume", consumer.Name.Split("#")[0]);
        }

        [Test]
        public void CreateRestoreClient()
        {
            var config = GetConfig();
            var supplier = new DefaultKafkaClientSupplier(new KafkaLoggerAdapter(config), config);
            var restore = supplier.GetRestoreConsumer(config.ToConsumerConfig("restore"));
            Assert.IsNotNull(restore);
            Assert.AreEqual("restore", restore.Name.Split("#")[0]);
        }

        [Test]
        public void CreateProducerClient()
        {
            var config = GetConfig();
            var supplier = new DefaultKafkaClientSupplier(new KafkaLoggerAdapter(config), config);
            var produce = supplier.GetProducer(config.ToProducerConfig("produce"));
            Assert.IsNotNull(produce);
            Assert.AreEqual("produce", produce.Name.Split("#")[0]);
        }
        
        [Test]
        public void CreateGlobalConsumerClient()
        {
            var config = GetConfig();
            var supplier = new DefaultKafkaClientSupplier(new KafkaLoggerAdapter(config), config);
            var globalConsumer = supplier.GetGlobalConsumer(config.ToGlobalConsumerConfig("global-consumer"));
            Assert.IsNotNull(globalConsumer);
            Assert.AreEqual("global-consumer", globalConsumer.Name.Split("#")[0]);
        }

        [Test]
        public void CreateConsumerWithStats()
        {
            var config = GetConfig();
            config.ExposeLibrdKafkaStats = true;
            config.ApplicationId = "test-app";
            config.ClientId = "test-client";
            var supplier = new DefaultKafkaClientSupplier(new KafkaLoggerAdapter(config), config);
            supplier.MetricsRegistry = new StreamMetricsRegistry();
            
            var consumerConfig = config.ToConsumerConfig("consume");
            StreamizConsumerConfig wrapper = new StreamizConsumerConfig(consumerConfig, "thread-1");

            var consumer = supplier.GetConsumer(wrapper, new StreamsRebalanceListener(null));
            Assert.IsNotNull(consumer);
        }
        
        [Test]
        public void CreateProducerWithStats()
        {
            var config = GetConfig();
            config.ExposeLibrdKafkaStats = true;
            config.ApplicationId = "test-app";
            config.ClientId = "test-client";
            var supplier = new DefaultKafkaClientSupplier(new KafkaLoggerAdapter(config), config);
            supplier.MetricsRegistry = new StreamMetricsRegistry();
            
            var producerConfig = config.ToProducerConfig("producer");
            StreamizProducerConfig wrapper = new StreamizProducerConfig(producerConfig, "thread-1", new TaskId(){Id = 0, Partition = 0});

            var producer = supplier.GetProducer(wrapper);
            Assert.IsNotNull(producer);
        }
        
        [Test]
        public void CreateGlobalConsumerWithStats()
        {
            var config = GetConfig();
            config.ExposeLibrdKafkaStats = true;
            config.ApplicationId = "test-app";
            config.ClientId = "test-client";
            
            var supplier = new DefaultKafkaClientSupplier(new KafkaLoggerAdapter(config), config);
            supplier.MetricsRegistry = new StreamMetricsRegistry();
            
            var consumerConfig = config.ToGlobalConsumerConfig("global-consume");
            StreamizConsumerConfig wrapper = new StreamizConsumerConfig(consumerConfig, "global-thread-1");
            
            var globalConsumer = supplier.GetGlobalConsumer(wrapper);
            Assert.IsNotNull(globalConsumer);
        }
    }
}