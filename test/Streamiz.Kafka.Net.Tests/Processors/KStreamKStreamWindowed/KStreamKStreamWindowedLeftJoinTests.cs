using NUnit.Framework;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Linq;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamKStreamWindowedLeftJoinTests : KStreamKStreamWindowedTestsBase
    {
        protected override JoinWindowOptions CreateJoinWindow() 
            => new JoinSlidingWindowOptions(0L, 1000L, -1L, JoinSlidingWindowOptions.DEFAULT_RETENTION_MS);

        protected override JoinDelegate GetJoinDelegate(IKStream<string, string> stream1) 
            => stream1.LeftJoin;

        [Test]
        public void StreamStreamLeftJoin()
        {
            DateTime dateTime = DateTime.Today;
            inputTopic2.PipeInput("test", "right-0", dateTime.AddSeconds(0));
            inputTopic1.PipeInput("test", "left-1", dateTime.AddSeconds(1));
            inputTopic2.PipeInput("test", "right-2", dateTime.AddSeconds(2));
            inputTopic2.PipeInput("test", "right-3", dateTime.AddSeconds(3));

            var records = outputTopic.ReadValueList().ToArray();
            Assert.AreEqual(2, records.Length);

            Assert.That(records[0], Is.EqualTo("left-1-"));
            Assert.That(records[1], Is.EqualTo("left-1-right-2"));
        }
    }
}