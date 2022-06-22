using System;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Tests.Helpers
{
    public class AbstractAsyncTests
    {
        private readonly string applicationId;

        protected AbstractAsyncTests(string applicationId)
        {
            this.applicationId = applicationId;
        }
        
        protected TopologyTestDriver BuildTopology(Action<StreamBuilder> funcBuilder, TopologyTestDriver.Mode mode = TopologyTestDriver.Mode.SYNC_TASK, Action<StreamConfig> configAction = null)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = $"{applicationId}-{Guid.NewGuid().ToString()}";
            config.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            config.PollMs = 10;
            config.MaxPollRecords = 10;
            config.CommitIntervalMs = 100;
            config.MetricsRecording = MetricsRecordingLevel.INFO;
            configAction?.Invoke(config);
            
            var builder = new StreamBuilder();
            funcBuilder(builder);

            var topo = builder.Build();

            return new TopologyTestDriver(topo, config, mode);
        }
        
        protected void AssertKeyValue<K, V>(ConsumeResult<K, V> result, K expectedKey, V expectedValue)
        {
            Assert.AreEqual(expectedKey, result.Message.Key);
            Assert.AreEqual(expectedValue, result.Message.Value);
        }
    }
}