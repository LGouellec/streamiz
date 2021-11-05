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
    public class OffsetCheckpointFileTest {

        private OffsetCheckpointFile _offsetCheckpointFile;
        private readonly static string offsetCheckpointPath = ".checkpoint-unit-testfile";

        [SetUp]
        public void Setup()
        {
            _offsetCheckpointFile = new OffsetCheckpointFile(offsetCheckpointPath);
        }

        [TearDown]
        public void Close(){
            _offsetCheckpointFile.Destroy();
        }

        [Test]
        public void WriteOffsetCheckpoint()
        {
            var offsets = new Dictionary<TopicPartition, long>();
            offsets.Add(new TopicPartition("topic1", 0), 100);
            offsets.Add(new TopicPartition("topic2", 0), 40);
            offsets.Add(new TopicPartition("topic3", 0), 325);
            _offsetCheckpointFile.Write(offsets);

            var offsetsRead = _offsetCheckpointFile.Read();

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
            Assert.Throws<StreamsException>(() => _offsetCheckpointFile.Write(offsets));
        }

        [Test]
        public void WriteOffsetEmpty()
        {
            var offsets = new Dictionary<TopicPartition, long>();
            _offsetCheckpointFile.Write(offsets);
        }

        [Test]
        public void ReadMalformedOffsetFile()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("0")
                .AppendLine("1")
                .AppendLine("topic 0");
            File.WriteAllText(offsetCheckpointPath, sb.ToString());

            Assert.False(_offsetCheckpointFile.Read().Any());
        }

        [Test]
        public void ReadInvalidOffset()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("0")
                .AppendLine("2")
                .AppendLine("topic 0 1")
                .AppendLine("topic2 0 -1");

            File.WriteAllText(offsetCheckpointPath, sb.ToString());

            Assert.False(_offsetCheckpointFile.Read().Any());
        }

        [Test]
        public void ReadIncorrectVersion()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("12")
                .AppendLine("2")
                .AppendLine("topic 0 1")
                .AppendLine("topic2 0 -1");

            File.WriteAllText(offsetCheckpointPath, sb.ToString());

            Assert.Throws<ArgumentException>(() => _offsetCheckpointFile.Read());
        }
    }

}