using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class OffsetCheckpointFileTest
    {
        private OffsetCheckpointFile _offsetCheckpointFile;
        private readonly static string offsetCheckpointPath = ".";

        [SetUp]
        public void Setup()
        {
            _offsetCheckpointFile = new OffsetCheckpointFile(offsetCheckpointPath);
            _offsetCheckpointFile.Configure(null, new Net.Processors.Internal.TaskId {Id = 0, Partition = 1});
        }

        [TearDown]
        public void Close()
        {
            _offsetCheckpointFile.Destroy(new Net.Processors.Internal.TaskId {Id = 0, Partition = 1});
        }

        [Test]
        public void WriteOffsetCheckpoint()
        {
            var offsets = new Dictionary<TopicPartition, long>();
            offsets.Add(new TopicPartition("topic1", 0), 100);
            offsets.Add(new TopicPartition("topic2", 0), 40);
            offsets.Add(new TopicPartition("topic3", 0), 325);
            _offsetCheckpointFile.Write(new Net.Processors.Internal.TaskId {Id = 0, Partition = 1}, offsets);

            var offsetsRead = _offsetCheckpointFile.Read(new Net.Processors.Internal.TaskId {Id = 0, Partition = 1});

            Assert.AreEqual(3, offsetsRead.Count);
            Assert.AreEqual(100, offsetsRead[new TopicPartition("topic1", 0)]);
            Assert.AreEqual(40, offsetsRead[new TopicPartition("topic2", 0)]);
            Assert.AreEqual(325, offsetsRead[new TopicPartition("topic3", 0)]);
        }

        [Test]
        public void WriteOffsetInvalid()
        {
            var offsets = new Dictionary<TopicPartition, long>();
            offsets.Add(new TopicPartition("topic1", 0), -1);
            Assert.Throws<StreamsException>(() =>
                _offsetCheckpointFile.Write(new Net.Processors.Internal.TaskId {Id = 0, Partition = 1}, offsets));
        }

        [Test]
        public void WriteOffsetEmpty()
        {
            var offsets = new Dictionary<TopicPartition, long>();
            _offsetCheckpointFile.Write(new Net.Processors.Internal.TaskId {Id = 0, Partition = 1}, offsets);
        }

        [Test]
        public void ReadMalformedOffsetFile()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("0")
                .AppendLine("1")
                .AppendLine("topic 0");
            File.WriteAllText(Path.Combine(offsetCheckpointPath, OffsetCheckpointFile.CHECKPOINT_FILE_NAME),
                sb.ToString());

            Assert.False(_offsetCheckpointFile.Read(new Net.Processors.Internal.TaskId {Id = 0, Partition = 1}).Any());
        }

        [Test]
        public void ReadInvalidOffset()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("0")
                .AppendLine("2")
                .AppendLine("topic 0 1")
                .AppendLine("topic2 0 -1");

            File.WriteAllText(Path.Combine(offsetCheckpointPath, OffsetCheckpointFile.CHECKPOINT_FILE_NAME),
                sb.ToString());

            Assert.True(_offsetCheckpointFile.Read(new Net.Processors.Internal.TaskId {Id = 0, Partition = 1}).Any());
            Assert.AreEqual(1,
                _offsetCheckpointFile.Read(new Net.Processors.Internal.TaskId {Id = 0, Partition = 1})[
                    new TopicPartition("topic", 0)]);
        }

        [Test]
        public void ReadIncorrectVersion()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("12")
                .AppendLine("2")
                .AppendLine("topic 0 1")
                .AppendLine("topic2 0 -1");

            File.WriteAllText(Path.Combine(offsetCheckpointPath, OffsetCheckpointFile.CHECKPOINT_FILE_NAME),
                sb.ToString());

            Assert.Throws<ArgumentException>(() =>
                _offsetCheckpointFile.Read(new Net.Processors.Internal.TaskId {Id = 0, Partition = 1}));
        }
    }
}