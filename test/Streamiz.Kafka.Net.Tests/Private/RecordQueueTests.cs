﻿using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class RecordQueueTests
    {
        private readonly Sensor droppedSensor = new NoRunnableSensor("s", "s", MetricsRecordingLevel.DEBUG);

        private class NegativeTimestampExtractor : ITimestampExtractor
        {
            public long Extract(ConsumeResult<object, object> record, long partitionTime)
            {
                return -1;
            }
        }

        [Test]
        public void QueueOneMessageTest()
        {
            var timestampEx = new FailOnInvalidTimestamp();
            var serdes = new StringSerDes();
            var sourceProcessor = new SourceProcessor<string, string>("source", "test", serdes, serdes, timestampEx);
            var recordQueue = new RecordQueue("", "", timestampEx, new TopicPartition("test", 0), sourceProcessor,
                droppedSensor);
            int size = recordQueue.Queue(new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>
                {
                    Key = serdes.Serialize("key", new SerializationContext()),
                    Value = serdes.Serialize("test", new SerializationContext())
                }
            });
            Assert.AreEqual(1, size);
        }

        [Test]
        public void QueueDequeueOneMessageTest()
        {
            var timestampEx = new FailOnInvalidTimestamp();
            var serdes = new StringSerDes();
            var sourceProcessor = new SourceProcessor<string, string>("source", "test", serdes, serdes, timestampEx);
            var recordQueue = new RecordQueue("", "", timestampEx, new TopicPartition("test", 0), sourceProcessor,
                droppedSensor);
            recordQueue.Queue(new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>
                {
                    Key = serdes.Serialize("key", new SerializationContext()),
                    Value = serdes.Serialize("test", new SerializationContext())
                }
            });
            var r = recordQueue.Poll();
            Assert.IsNotNull(r);
            Assert.AreEqual("key", serdes.Deserialize(r.Message.Key, new SerializationContext()));
            Assert.AreEqual("test", serdes.Deserialize(r.Message.Value, new SerializationContext()));
            Assert.AreEqual(0, recordQueue.Size);
            Assert.IsTrue(recordQueue.IsEmpty);
        }

        [Test]
        public void QueueClearTest()
        {
            var timestampEx = new FailOnInvalidTimestamp();
            var serdes = new StringSerDes();
            var sourceProcessor = new SourceProcessor<string, string>("source", "test", serdes, serdes, timestampEx);
            var recordQueue = new RecordQueue("", "", timestampEx, new TopicPartition("test", 0), sourceProcessor,
                droppedSensor);
            int size = recordQueue.Queue(new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>
                {
                    Key = serdes.Serialize("key", new SerializationContext()),
                    Value = serdes.Serialize("test", new SerializationContext())
                }
            });
            recordQueue.Clear();
            Assert.AreEqual(1, size);
            Assert.IsTrue(recordQueue.IsEmpty);
            Assert.AreEqual(0, recordQueue.Size);
        }

        [Test]
        public void IgnoreNegativeTimestampTest()
        {
            var timestampEx = new NegativeTimestampExtractor();
            var serdes = new StringSerDes();
            var sourceProcessor = new SourceProcessor<string, string>("source", "test", serdes, serdes, timestampEx);
            var recordQueue = new RecordQueue("", "", timestampEx, new TopicPartition("test", 0), sourceProcessor,
                droppedSensor);
            int size = recordQueue.Queue(new ConsumeResult<byte[], byte[]>()
            {
                Message = new Message<byte[], byte[]>
                {
                    Key = serdes.Serialize("key", new SerializationContext()),
                    Value = serdes.Serialize("test", new SerializationContext())
                }
            });
            Assert.AreEqual(0, size);
            Assert.IsTrue(recordQueue.IsEmpty);
            Assert.AreEqual(0, recordQueue.Size);
        }
    }
}