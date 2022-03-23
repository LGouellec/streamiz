using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class PartitionGrouperTests
    {
        private readonly TopicPartition topicPart1 = new TopicPartition("test", 0);
        private readonly TopicPartition topicPart2 = new TopicPartition("test2", 0);
        private readonly StringSerDes serdes = new StringSerDes();
        private readonly Sensor droppedSensor = new NoRunnableSensor("s", "s", MetricsRecordingLevel.DEBUG);

        private Dictionary<TopicPartition, RecordQueue> GetPartitions()
        {
            var timestampEx = new FailOnInvalidTimestamp();
            var partitions = new Dictionary<TopicPartition, RecordQueue>();
            var sourceProcessor = new SourceProcessor<string, string>("source1", "test", serdes, serdes, timestampEx);
            var sourceProcessor2 = new SourceProcessor<string, string>("source2", "test2", serdes, serdes, timestampEx);
            var recordQueue =
                new RecordQueue("", "source_queue", timestampEx, topicPart1, sourceProcessor, droppedSensor);
            var recordQueue2 = new RecordQueue("", "source2_queue", timestampEx, topicPart2, sourceProcessor2,
                droppedSensor);
            partitions.Add(topicPart1, recordQueue);
            partitions.Add(topicPart2, recordQueue2);
            return partitions;
        }

        private ConsumeResult<byte[], byte[]> MakeMessageWithKey(string key, string value)
        {
            return new ConsumeResult<byte[], byte[]>
            {
                Message = new Message<byte[], byte[]>
                {
                    Key = serdes.Serialize(key, new SerializationContext()),
                    Value = serdes.Serialize(value, new SerializationContext()),
                    Timestamp = new Timestamp(DateTime.Now),
                    Headers = new Headers()
                }
            };
        }

        private ConsumeResult<byte[], byte[]> MakeMessageWithKey(string key, string value, DateTime dt)
        {
            return new ConsumeResult<byte[], byte[]>
            {
                Message = new Message<byte[], byte[]>
                {
                    Key = serdes.Serialize(key, new SerializationContext()),
                    Value = serdes.Serialize(value, new SerializationContext()),
                    Timestamp = new Timestamp(dt),
                    Headers = new Headers()
                }
            };
        }

        [Test]
        public void AddOneRecordErrorTest()
        {
            var grouper = new PartitionGrouper(new Dictionary<TopicPartition, RecordQueue>());
            Assert.Throws<IllegalStateException>(() =>
                grouper.AddRecord(topicPart1, MakeMessageWithKey("key", "test")));
            Assert.Throws<IllegalStateException>(() => grouper.NumBuffered(topicPart1));
        }

        [Test]
        public void AddOneRecordTest()
        {
            var grouper = new PartitionGrouper(GetPartitions());
            grouper.AddRecord(topicPart1, MakeMessageWithKey("key", "test"));
            Assert.IsFalse(grouper.AllPartitionsBuffered);
            Assert.AreEqual(1, grouper.NumBuffered());
            Assert.AreEqual(1, grouper.NumBuffered(topicPart1));
        }

        [Test]
        public void AddOneRecordAndPollTest()
        {
            var grouper = new PartitionGrouper(GetPartitions());
            grouper.AddRecord(topicPart1, MakeMessageWithKey("key", "test"));
            Assert.IsFalse(grouper.AllPartitionsBuffered);
            Assert.AreEqual(1, grouper.NumBuffered());
            Assert.AreEqual(1, grouper.NumBuffered(topicPart1));
            var record = grouper.NextRecord;
            Assert.IsNotNull(record);
            Assert.AreEqual("key", serdes.Deserialize(record.Record.Message.Key, new SerializationContext()));
            Assert.AreEqual("test", serdes.Deserialize(record.Record.Message.Value, new SerializationContext()));
            Assert.AreEqual("source1", record.Processor.Name);
            Assert.IsTrue(record.Queue.IsEmpty);
        }

        [Test]
        public void AddMultipleRecordInMultiplePartitionTest()
        {
            var grouper = new PartitionGrouper(GetPartitions());
            grouper.AddRecord(topicPart1, MakeMessageWithKey("key", "test"));
            grouper.AddRecord(topicPart2, MakeMessageWithKey("key", "test2"));
            Assert.IsTrue(grouper.AllPartitionsBuffered);
            Assert.AreEqual(2, grouper.NumBuffered());
            Assert.AreEqual(1, grouper.NumBuffered(topicPart1));
            Assert.AreEqual(1, grouper.NumBuffered(topicPart2));
            var record = grouper.NextRecord;
            Assert.IsNotNull(record);
            Assert.AreEqual("key", serdes.Deserialize(record.Record.Message.Key, new SerializationContext()));
            Assert.AreEqual("test", serdes.Deserialize(record.Record.Message.Value, new SerializationContext()));
            Assert.AreEqual("source1", record.Processor.Name);
            Assert.IsTrue(record.Queue.IsEmpty);
            record = grouper.NextRecord;
            Assert.IsNotNull(record);
            Assert.AreEqual("key", serdes.Deserialize(record.Record.Message.Key, new SerializationContext()));
            Assert.AreEqual("test2", serdes.Deserialize(record.Record.Message.Value, new SerializationContext()));
            Assert.AreEqual("source2", record.Processor.Name);
            Assert.IsTrue(record.Queue.IsEmpty);
            Assert.IsNull(grouper.NextRecord);
        }

        [Test]
        public void AddMultipleRecordInMultiplePartitionDifferentDateTest()
        {
            var grouper = new PartitionGrouper(GetPartitions());
            DateTime dt = DateTime.Now;
            grouper.AddRecord(topicPart1, MakeMessageWithKey("key", "1", dt.AddMilliseconds(1)));
            grouper.AddRecord(topicPart2, MakeMessageWithKey("key", "2", dt));
            grouper.AddRecord(topicPart1, MakeMessageWithKey("key", "3", dt.AddMilliseconds(-13)));
            Assert.IsTrue(grouper.AllPartitionsBuffered);
            Assert.AreEqual(3, grouper.NumBuffered());
            Assert.AreEqual(2, grouper.NumBuffered(topicPart1));
            Assert.AreEqual(1, grouper.NumBuffered(topicPart2));
            var record = grouper.NextRecord;
            Assert.IsNotNull(record);
            Assert.AreEqual("key", serdes.Deserialize(record.Record.Message.Key, new SerializationContext()));
            Assert.AreEqual("2", serdes.Deserialize(record.Record.Message.Value, new SerializationContext()));
            Assert.AreEqual("source2", record.Processor.Name);
            Assert.IsTrue(record.Queue.IsEmpty);
            record = grouper.NextRecord;
            Assert.IsNotNull(record);
            Assert.AreEqual("key", serdes.Deserialize(record.Record.Message.Key, new SerializationContext()));
            Assert.AreEqual("1", serdes.Deserialize(record.Record.Message.Value, new SerializationContext()));
            Assert.AreEqual("source1", record.Processor.Name);
            Assert.IsFalse(record.Queue.IsEmpty);
            record = grouper.NextRecord;
            Assert.IsNotNull(record);
            Assert.AreEqual("key", serdes.Deserialize(record.Record.Message.Key, new SerializationContext()));
            Assert.AreEqual("3", serdes.Deserialize(record.Record.Message.Value, new SerializationContext()));
            Assert.AreEqual("source1", record.Processor.Name);
            Assert.IsTrue(record.Queue.IsEmpty);
            Assert.IsNull(grouper.NextRecord);
        }
    }
}