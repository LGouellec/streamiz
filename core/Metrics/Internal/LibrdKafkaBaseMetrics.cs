using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Metrics.Internal
{
    internal class LibrdKafkaBaseMetrics
    {
        internal static readonly string BROKER_ID_TAG = "broker_id";
        internal static readonly string TOPIC_TAG = "topic";
        internal static readonly string PARTITION_ID_TAG = "partition_id";
        
        protected static Sensor CreateSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            string sensorName,
            string sensorDescription,
            (bool brokerSensor, bool topicSensor, bool partitionSensor) options,
            bool isConsumer,
            StreamMetricsRegistry metricsRegistry)
        {
            var sensor = metricsRegistry.LibrdKafkaSensor(
                threadId,
                librdKafkaClientId,
                sensorName,
                sensorDescription,
                MetricsRecordingLevel.INFO);

            var tags = metricsRegistry.LibrdKafkaLevelTags(threadId, librdKafkaClientId, streamAppId);
            
            if(options.brokerSensor)
                tags.Add(BROKER_ID_TAG, string.Empty);
            if(options.topicSensor)
                tags.Add(TOPIC_TAG, string.Empty);
            if(options.partitionSensor)
                tags.Add(PARTITION_ID_TAG, string.Empty);
            
            AddValueMetric(
                sensor,
                sensorName,
                sensorDescription,
                tags,
                isConsumer);
            
            return sensor;
        }

        protected static T CreateSensor<T>(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            string sensorName,
            string sensorDescription,
            (bool brokerSensor, bool topicSensor, bool partitionSensor) options,
            bool isConsumer,
            StreamMetricsRegistry metricsRegistry)
            where T : Sensor
        {
            var sensor = metricsRegistry.LibrdKafkaSensor<T>(
                threadId,
                librdKafkaClientId,
                sensorName,
                sensorDescription,
                MetricsRecordingLevel.INFO);

            var tags = metricsRegistry.LibrdKafkaLevelTags(threadId, librdKafkaClientId, streamAppId);
            
            if(options.brokerSensor)
                tags.Add(BROKER_ID_TAG, string.Empty);
            if(options.topicSensor)
                tags.Add(TOPIC_TAG, string.Empty);
            if(options.partitionSensor)
                tags.Add(PARTITION_ID_TAG, string.Empty);
            
            AddValueMetric(
                sensor,
                sensorName,
                sensorDescription,
                tags,
                isConsumer);
            
            return sensor;
        }

        
        protected static void AddValueMetric(Sensor sensor,
            string name,
            string description,
            IDictionary<string, string> tags,
            bool isConsumer)
        {
            SensorHelper.AddValueMetricToSensor(
                sensor,
                isConsumer ? StreamMetricsRegistry.LIBRDKAFKA_CONSUMER_LEVEL_GROUP : StreamMetricsRegistry.LIBRDKAFKA_PRODUCER_LEVEL_GROUP,
                tags,
                name,
                description);
        }
    }
}