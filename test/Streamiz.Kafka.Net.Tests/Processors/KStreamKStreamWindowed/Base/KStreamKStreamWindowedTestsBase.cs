using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public abstract class KStreamKStreamWindowedTestsBase
    {
        private TopologyTestDriver driver;
        protected TestInputTopic<string, string> inputTopic1;
        protected TestInputTopic<string, string> inputTopic2;
        protected TestOutputTopic<string, string> outputTopic;
        protected DateTime TestTime { get; } = DateTime.Today;

        protected delegate IKStream<string, string> JoinDelegate(IKStream<string, string> stream2, Func<string, string, string> valueJoiner, JoinWindowOptions joinWindow, StreamJoinProps<string, string, string> props);
        protected abstract JoinDelegate GetJoinDelegate(IKStream<string, string> stream1);
        protected abstract JoinWindowOptions CreateJoinWindow();

        [SetUp]
        public void Setup()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = $"test-stream-stream-{GetType().Name}"
            };

            StreamBuilder builder = new StreamBuilder();

            var stream1 = builder.Stream<string, string>("topic1");
            var stream2 = builder.Stream<string, string>("topic2");

            var window = CreateJoinWindow();
            var joinDelegate = GetJoinDelegate(stream1);
            CreateOutputStream(stream2, joinDelegate, window).To("output-join");

            var topology = builder.Build();

            driver = new TopologyTestDriver(topology, config);

            inputTopic1 = driver.CreateInputTopic<string, string>("topic1");
            inputTopic2 = driver.CreateInputTopic<string, string>("topic2");
            outputTopic = driver.CreateOutputTopic<string, string>("output-join");
        }

        [TearDown]
        public void TearDown()
        {
            driver?.Dispose();
        }

        private IKStream<string, string> CreateOutputStream(
            IKStream<string, string> stream2,
            JoinDelegate joinDelegate,
            JoinWindowOptions joinWindow)
        {
            var windowSize = joinWindow.Size;
            var props = StreamJoinProps.With<string, string, string>(
               Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("windowed-join-1-store", TimeSpan.FromDays(1),
                   TimeSpan.FromMilliseconds(windowSize)),
               Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("windowed-join-2-store", TimeSpan.FromDays(1),
                   TimeSpan.FromMilliseconds(windowSize)));

            return joinDelegate(
               stream2,
               (s, v) => $"{s}-{v}",
               joinWindow,
               props);
        }
    }
}