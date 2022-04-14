using System;
using System.Linq;
using System.Reflection;
using Avro.Util;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;

namespace Streamiz.Kafka.Net.Tests.Metrics
{
    public class GenericClientMetricsTests
    {
        private StreamMetricsRegistry streamMetricsRegistry = null;

        [SetUp]
        public void Initialize()
        {
            streamMetricsRegistry
                = new StreamMetricsRegistry(Guid.NewGuid().ToString(),
                    MetricsRecordingLevel.DEBUG);
        }
        
        [Test]
        public void StreamsAppSensorTest()
        {
            
            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic").To("topic2");

            var sensor = GeneralClientMetrics.StreamsAppSensor(
                "my-application",
                builder.Build().Describe().ToString(),
                () => 0,
                () => 1, streamMetricsRegistry);

            Assert.AreEqual(5, sensor.Metrics.Keys.Count());
            
            Assert.AreEqual(Assembly.GetExecutingAssembly().GetName().Version.ToString(),
                sensor.Metrics[MetricName.NameAndGroup(
                GeneralClientMetrics.VERSION,
                StreamMetricsRegistry.CLIENT_LEVEL_GROUP)].Value);
            
            Assert.AreEqual(0,
                sensor.Metrics[MetricName.NameAndGroup(
                    GeneralClientMetrics.STATE,
                    StreamMetricsRegistry.CLIENT_LEVEL_GROUP)].Value);
            
            
            Assert.AreEqual(builder.Build().Describe().ToString(),
                sensor.Metrics[MetricName.NameAndGroup(
                    GeneralClientMetrics.TOPOLOGY_DESCRIPTION,
                    StreamMetricsRegistry.CLIENT_LEVEL_GROUP)].Value);
            
            
            Assert.AreEqual(1,
                sensor.Metrics[MetricName.NameAndGroup(
                    GeneralClientMetrics.STREAM_THREADS,
                    StreamMetricsRegistry.CLIENT_LEVEL_GROUP)].Value);
            
            
            Assert.AreEqual("my-application",
                sensor.Metrics[MetricName.NameAndGroup(
                    GeneralClientMetrics.APPLICATION_ID,
                    StreamMetricsRegistry.CLIENT_LEVEL_GROUP)].Value);
        }
    }
}