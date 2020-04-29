using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Streamiz.Kafka.Net.TestProcessors
{
    public class KStreamPeekTests
    {
        [Test]
        public void ShouldObserveStreamElements()
        {
            var builder = new StreamBuilder();
            var peekObserved = new List<KeyValuePair<string, string>>();
            var streamObserved = new List<KeyValuePair<string, string>>();

            builder.Stream<string, string>("topic")
                .Peek((k, v) => peekObserved.Add(KeyValuePair.Create(k, v)))
                .Foreach((k, v) => streamObserved.Add(KeyValuePair.Create(k, v)));

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-peek";
            
            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var expected = new List<KeyValuePair<string, string>>();
                for (int i = 0; i < 32; i++)
                {
                    string key = i.ToString();
                    string value = $"V{i}";
                    inputTopic.PipeInput(key, value);
                    expected.Add(KeyValuePair.Create(key, value));
                }
                Thread.Sleep(1000);

                Assert.AreEqual(expected.Count, peekObserved.Count);
                Assert.AreEqual(expected.Count, streamObserved.Count);
                foreach (var e in expected)
                {
                    Assert.IsTrue(peekObserved.Contains(e));
                    Assert.IsTrue(streamObserved.Contains(e));
                }
            }
        }

        [Test]
        public void ShouldNotAllowNullAction()
        {
            var builder = new StreamBuilder();
            IKStream<string, string> stream = builder.Stream<string, string>("topic");

            Assert.Throws<ArgumentNullException>(() => stream.Peek(null));

        }
    }
}