using System;
using System.Collections.Generic;
using System.Reflection;

namespace Streamiz.Kafka.Net.Metrics.Internal
{
    internal class GeneralClientMetrics
    {
        private static readonly string APP_INFO = "app-info";
        private static readonly string APP_INFO_DESCRIPTION = "The application information metrics";
        private static readonly string VERSION = "version";
        private static readonly string APPLICATION_ID = "application-id";
        private static readonly string TOPOLOGY_DESCRIPTION = "topology-description";
        private static readonly string STATE = "state";
        private static readonly string STREAM_THREADS = "stream-threads";
        private static readonly string VERSION_FROM_ASSEMBLY;
        private static readonly string DEFAULT_VALUE = "unknown";

        private static readonly string VERSION_DESCRIPTION = "The version of the Kafka Streams client";
        private static readonly string APPLICATION_ID_DESCRIPTION = "The application ID of the Kafka Streams client";
        private static readonly string TOPOLOGY_DESCRIPTION_DESCRIPTION =
        "The description of the topology executed in the Kafka Streams client";
        private static readonly string STATE_DESCRIPTION = "The state of the Kafka Streams client";
        private static readonly string STREAM_THREADS_DESCRIPTION = "The number of stream threads that are running or participating in rebalance";

        static GeneralClientMetrics()
        {
            try
            {
                VERSION_FROM_ASSEMBLY = Assembly.GetExecutingAssembly().GetName().Version.ToString();
            }
            catch
            {
                VERSION_FROM_ASSEMBLY = DEFAULT_VALUE;
            }
        }

        public static Sensor StreamsAppSensor(
            string applicationId,
            string topologyDescription,
            Func<KafkaStream.State> stateStreamFunc,
            Func<int> streamThreadsFunc,
            StreamMetricsRegistry metricsRegistry)
        {
            var sensor = metricsRegistry.ClientLevelSensor(
                APP_INFO,
                APP_INFO_DESCRIPTION,
                MetricsRecordingLevel.INFO);
            var tags = metricsRegistry.ClientTags();

            var tagsVersion = new Dictionary<string, string>(tags);
            tagsVersion.Add(VERSION, VERSION_FROM_ASSEMBLY);
            sensor.AddImmutableMetric(
                new MetricName(
                    VERSION,
                    StreamMetricsRegistry.CLIENT_LEVEL_GROUP,
                    VERSION_DESCRIPTION,
                    tagsVersion), VERSION_FROM_ASSEMBLY);
            
            var tagsApp = new Dictionary<string, string>(tags);
            tagsApp.Add(APPLICATION_ID, applicationId);
            sensor.AddImmutableMetric(
                new MetricName(
                    APPLICATION_ID,
                    StreamMetricsRegistry.CLIENT_LEVEL_GROUP,
                    APPLICATION_ID_DESCRIPTION,
                    tagsApp), applicationId);
            
            var tagsTopo = new Dictionary<string, string>(tags);
            tagsApp.Add(TOPOLOGY_DESCRIPTION, topologyDescription);
            sensor.AddImmutableMetric(
                new MetricName(
                    TOPOLOGY_DESCRIPTION,
                    StreamMetricsRegistry.CLIENT_LEVEL_GROUP,
                    TOPOLOGY_DESCRIPTION_DESCRIPTION,
                    tagsApp), topologyDescription);
            
            sensor.AddProviderMetric(
                new MetricName(
                    STATE,
                    StreamMetricsRegistry.CLIENT_LEVEL_GROUP,
                    STATE_DESCRIPTION,
                    tags), stateStreamFunc);
            
            sensor.AddProviderMetric(
                new MetricName(
                    STREAM_THREADS,
                    StreamMetricsRegistry.CLIENT_LEVEL_GROUP,
                    STREAM_THREADS_DESCRIPTION,
                    tags), streamThreadsFunc);
            
            return sensor;
        }
    }
}