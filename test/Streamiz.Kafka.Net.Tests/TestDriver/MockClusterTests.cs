using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock.Kafka;
using System;
using System.Collections.Generic;
using System.Text;
using Streamiz.Kafka.Net.Crosscutting;
using System.Linq;

namespace Streamiz.Kafka.Net.Tests.TestDriver
{
    // TODO : more test about mockcluster
    public class MockClusterTests
    {
        [Test]
        public void TestAssignment()
        {
            var consumerConfig = new ConsumerConfig();
            var consumerConfig2 = new ConsumerConfig();
            consumerConfig.GroupId = "cg";
            consumerConfig2.GroupId = "cg";
            consumerConfig.ClientId = "cg-0";
            consumerConfig2.ClientId = "cg-1";

            var supplier = new MockKafkaSupplier(2);
            var c1 = supplier.GetConsumer(consumerConfig, null);
            var c2 = supplier.GetConsumer(consumerConfig2, null);

            c1.Subscribe(new List<string> { "topic1", "topic2" });
            c2.Subscribe(new List<string> { "topic1", "topic2" });

            c1.Consume();
            c2.Consume();
            c1.Consume();
            Assert.AreEqual(2, c1.Assignment.Count);
            Assert.AreEqual(2, c2.Assignment.Count);
        }

        [Test]
        public void TestConsume()
        {
            var consumerConfig = new ConsumerConfig();
            consumerConfig.GroupId = "cg";
            consumerConfig.ClientId = "cg-0";

            var supplier = new MockKafkaSupplier(2);
            var producer = supplier.GetProducer(new ProducerConfig());
            producer.Produce("topic", new Message<byte[], byte[]> { Key = new byte[1] { 42 }, Value = new byte[1] { 12 } });

            var c1 = supplier.GetConsumer(consumerConfig, null);
            c1.Subscribe(new List<string> { "topic"});

            var item1 = c1.Consume();
            Assert.IsNotNull(item1);
            var item2 = c1.Consume();
            Assert.IsNull(item2);
        }

        [Test]
        public void TestConsume2()
        {
            var consumerConfig = new ConsumerConfig();
            consumerConfig.GroupId = "cg";
            consumerConfig.ClientId = "cg-0";

            var supplier = new MockKafkaSupplier(2);
            var producer = supplier.GetProducer(new ProducerConfig());
            producer.Produce("topic", new Message<byte[], byte[]> { Key = new byte[1] { 42 }, Value = new byte[1] { 12 } });
            producer.Produce("topic", new Message<byte[], byte[]> { Key = new byte[1] { 42 }, Value = new byte[1] { 20 } });
            producer.Produce("topic", new Message<byte[], byte[]> { Key = new byte[1] { 32 }, Value = new byte[1] { 12 } });

            var c1 = supplier.GetConsumer(consumerConfig, null);
            c1.Subscribe(new List<string> { "topic" });

            var item = c1.Consume();
            Assert.IsNotNull(item);
            item = c1.Consume();
            Assert.IsNotNull(item);
            item = c1.Consume();
            Assert.IsNotNull(item);
            item = c1.Consume();
            Assert.IsNull(item);
        }


        [Test]
        public void TestConsumeRecords()
        {
            var consumerConfig = new ConsumerConfig();
            consumerConfig.GroupId = "cg";
            consumerConfig.ClientId = "cg-0";

            var supplier = new MockKafkaSupplier(2);
            var producer = supplier.GetProducer(new ProducerConfig());
            producer.Produce("topic", new Message<byte[], byte[]> { Key = new byte[1] { 42 }, Value = new byte[1] { 12 } });
            producer.Produce("topic", new Message<byte[], byte[]> { Key = new byte[1] { 42 }, Value = new byte[1] { 20 } });
            producer.Produce("topic", new Message<byte[], byte[]> { Key = new byte[1] { 32 }, Value = new byte[1] { 12 } });

            var c1 = supplier.GetConsumer(consumerConfig, null);
            c1.Subscribe(new List<string> { "topic" });

            var item = c1.ConsumeRecords(TimeSpan.FromSeconds(1)).ToList();
            Assert.IsNotNull(item);
            Assert.AreEqual(3, item.Count);
        }

        [Test]
        public void TestConsumeRecords2()
        {
            var consumerConfig = new ConsumerConfig();
            consumerConfig.GroupId = "cg";
            consumerConfig.ClientId = "cg-0";

            var supplier = new MockKafkaSupplier(2);
            var producer = supplier.GetProducer(new ProducerConfig());
            producer.Produce("topic", new Message<byte[], byte[]> { Key = new byte[1] { 42 }, Value = new byte[1] { 12 } });

            var c1 = supplier.GetConsumer(consumerConfig, null);
            c1.Subscribe(new List<string> { "topic" });

            var item = c1.ConsumeRecords(TimeSpan.FromSeconds(1)).ToList();
            Assert.IsNotNull(item);
            Assert.AreEqual(1, item.Count);
        }
        
        [Test]
        public void TestConsumeRebalance()
        {
            var consumerConfig = new ConsumerConfig();
            consumerConfig.GroupId = "cg";
            consumerConfig.ClientId = "cg-0";
            
            var consumerConfig1 = new ConsumerConfig();
            consumerConfig1.GroupId = "cg";
            consumerConfig1.ClientId = "cg-1";

            var supplier = new MockKafkaSupplier(2);
            var producer = supplier.GetProducer(new ProducerConfig());
            
            producer.Produce(new TopicPartition("topic", 0), new Message<byte[], byte[]> { Key = new byte[1] { 42 }, Value = new byte[1] { 12 } });
            producer.Produce(new TopicPartition("topic", 1), new Message<byte[], byte[]> { Key = new byte[1] { 43 }, Value = new byte[1] { 13 } });

            var c1 = supplier.GetConsumer(consumerConfig, null);
            var c2 = supplier.GetConsumer(consumerConfig1, null);
            c1.Subscribe(new List<string> { "topic"});
            c2.Subscribe(new List<string> { "topic"});

            var item1 = c1.Consume();
            var item2 = c2.Consume();
            item1 = c1.Consume();
            Assert.IsNotNull(item1);
            Assert.IsNotNull(item2);
        }
        
        [Test]
        public void TestConsumeRebalance2()
        {
            var consumerConfig = new ConsumerConfig();
            consumerConfig.GroupId = "cg";
            consumerConfig.ClientId = "cg-0";
            
            var consumerConfig1 = new ConsumerConfig();
            consumerConfig1.GroupId = "cg";
            consumerConfig1.ClientId = "cg-1";

            var supplier = new MockKafkaSupplier(2);
            var producer = supplier.GetProducer(new ProducerConfig());
            
            producer.Produce(new TopicPartition("topic", 0), new Message<byte[], byte[]> { Key = new byte[1] { 42 }, Value = new byte[1] { 12 } });
            producer.Produce(new TopicPartition("topic", 1), new Message<byte[], byte[]> { Key = new byte[1] { 43 }, Value = new byte[1] { 13 } });

            var c1 = supplier.GetConsumer(consumerConfig, null);
            var c2 = supplier.GetConsumer(consumerConfig1, null);
            
            c1.Subscribe(new List<string> { "topic"});
            c1.Consume();
            c2.Subscribe(new List<string> { "topic"});
            c2.Consume();
            
            c1.Consume();
            c2.Consume();
            
            Assert.AreEqual(1, c1.Assignment.Count);
            Assert.AreEqual(1, c2.Assignment.Count);

            c1.Unsubscribe();
            c2.Unsubscribe();
            
            c1.Subscribe(new List<string> { "topic"});
            c1.Consume();
            c2.Subscribe(new List<string> { "topic"});
            c2.Consume();
            
            c2.Consume();
            c1.Consume();
            c2.Consume();
            
            
            
            Assert.AreEqual(1, c1.Assignment.Count);
            Assert.AreEqual(1, c2.Assignment.Count);
        }
    }
}
