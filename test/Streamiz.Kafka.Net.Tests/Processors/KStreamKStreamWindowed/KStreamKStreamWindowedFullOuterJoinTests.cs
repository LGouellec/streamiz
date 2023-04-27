using NUnit.Framework;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Linq;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamKStreamWindowedFullOuterJoinTests : KStreamKStreamWindowedTestsBase
    {
        protected override JoinWindowOptions CreateJoinWindow()
            => new JoinSlidingWindowOptions(0L, 1000L, -1L, JoinSlidingWindowOptions.DEFAULT_RETENTION_MS);

        protected override JoinDelegate GetJoinDelegate(IKStream<string, string> stream1)
            => stream1.OuterJoin;

        [Test]
        public void StreamStreamFullOuterJoin()
        {
            DateTime dateTime = DateTime.Today;
            inputTopic2.PipeInput("test", "right-0", dateTime.AddSeconds(0));
            inputTopic1.PipeInput("test", "left-1", dateTime.AddSeconds(1));
            inputTopic2.PipeInput("test", "right-2", dateTime.AddSeconds(2));
            inputTopic2.PipeInput("test", "right-3", dateTime.AddSeconds(3));
            inputTopic1.PipeInput("test", "left-5.1", dateTime.AddSeconds(5.1));
            inputTopic1.PipeInput("test", "left-5.2", dateTime.AddSeconds(5.2));
            inputTopic2.PipeInput("test", "right-6", dateTime.AddSeconds(6));

            var records = outputTopic.ReadValueList().ToArray();
            Assert.AreEqual(8, records.Length);

            Assert.That(records[0], Is.EqualTo("-right-0"));
            Assert.That(records[1], Is.EqualTo("left-1-"));
            Assert.That(records[2], Is.EqualTo("left-1-right-2"));
            Assert.That(records[3], Is.EqualTo("-right-3"));
            Assert.That(records[4], Is.EqualTo("left-5.1-"));
            Assert.That(records[5], Is.EqualTo("left-5.2-"));
            Assert.That(records[6], Is.EqualTo("left-5.1-right-6"));
            Assert.That(records[7], Is.EqualTo("left-5.2-right-6"));
        }
    }
}