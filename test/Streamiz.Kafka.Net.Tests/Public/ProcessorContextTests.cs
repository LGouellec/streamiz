using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors.Internal;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Tests.Public
{
    public class ProcessorContextTests
    {
        [Test]
        public void ApplicationIdProperty()
        {
            var config = new StreamConfig();
            config.ApplicationId = "test";
            var stateMgt = new ProcessorStateManager(
                new TaskId() { Id = 0, Partition = 0 },
                new List<Confluent.Kafka.TopicPartition> { new Confluent.Kafka.TopicPartition("test", 0) });
            ProcessorContext context = new ProcessorContext(null, config, stateMgt);

            Assert.AreEqual("test", context.ApplicationId);
        }

        [Test]
        public void RecordContextProperty()
        {
            DateTime dt = DateTime.Now;
            var config = new StreamConfig();
            config.ApplicationId = "test";
            var stateMgt = new ProcessorStateManager(
                new TaskId() { Id = 0, Partition = 0 },
                new List<Confluent.Kafka.TopicPartition> { new Confluent.Kafka.TopicPartition("test", 0) });
            ProcessorContext context = new ProcessorContext(null, config, stateMgt);
            ConsumeResult<byte[], byte[]> result = new ConsumeResult<byte[], byte[]> { Message = new Message<byte[], byte[]>() };
            result.Topic = "topic";
            result.Partition = 0;
            result.Offset = 100;
            result.Message.Timestamp = new Timestamp(dt);
            context.SetRecordMetaData(result);

            Assert.AreEqual("topic", context.Topic);
            Assert.AreEqual(0, context.Partition.Value);
            Assert.AreEqual(100, context.Offset);
            Assert.AreEqual(dt.GetMilliseconds(), context.Timestamp);
        }
    }
}
