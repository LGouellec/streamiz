using NUnit.Framework;
using Streamiz.Kafka.Net.Stream;
using System.Linq;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamKStreamWindowedInnerJoinTests : KStreamKStreamWindowedTestsBase
    {
        protected override JoinWindowOptions CreateJoinWindow()
            => new JoinSlidingWindowOptions(0L, 1000L, -1L, JoinSlidingWindowOptions.DEFAULT_RETENTION_MS);

        protected override JoinDelegate GetJoinDelegate(IKStream<string, string> stream1)
            => stream1.Join;

        [Test]
        public void StreamStreamInnerJoin()
        {
            inputTopic2.PipeInput("test", "right-0", TestTime.AddSeconds(0));
            inputTopic1.PipeInput("test", "left-1", TestTime.AddSeconds(1));
            inputTopic2.PipeInput("test", "right-2", TestTime.AddSeconds(2));
            inputTopic2.PipeInput("test", "right-3", TestTime.AddSeconds(3));

            var records = outputTopic.ReadValueList().ToArray();
            Assert.AreEqual(1, records.Length);

            Assert.That(records[0], Is.EqualTo("left-1-right-2"));
        }
    }
}