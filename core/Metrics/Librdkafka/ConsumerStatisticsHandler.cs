using Streamiz.Kafka.Net.Metrics.Internal;

namespace Streamiz.Kafka.Net.Metrics.Librdkafka
{
    public class ConsumerStatisticsHandler : IStatisticsHandler
    {
        private readonly string threadId;
        private readonly string clientId;
        private readonly string streamAppId;

        private Sensor TotalNumberOfMessagesConsumedSensor;
        private Sensor TotalNumberOfMessageBytesConsumedSensor;
        private Sensor NumberOfOpsWaitinInQueueSensor; // Sensor
        private Sensor TotalNumberOfResponsesReceivedFromKafkaSensor;
        private Sensor TotalNumberOfBytesReceivedFromKafkaSensor;
        private Sensor RebalanceAgeSensor; // Sensor
        private Sensor TotalNumberOfRelabalanceSensor; // Sensor assign or revoke

        //PER BROKER (add Broker NodeId as label)
        private Sensor TotalNumberOfResponsesReceivedSensor;
        private Sensor TotalNumberOfBytesReceivedSensor;
        private Sensor TotalNumberOfReceivedErrorsSensor;
        private Sensor NumberOfConnectionAttempsSensor; // Including successful, failed and name resolution failures
        private Sensor NumberOfDisconnectsSensor;
        private Sensor BrokerLatencyAverageMsSensor;

        // Per Topic (add topic name as label)			
        private Sensor BatchSizeAverageBytesSensor;
        private Sensor BatchMessageCountsAverageSensor;

        // Per partition(topic brokder id PartitionId as label)

        private Sensor ConsumerLagSensor; // Sensor
        private Sensor TotalNumberOfMessagesConsumedByPartitionSensor; // Sensor
        private Sensor TotalNumberOfBytesConsumedByPartitionSensor; // Sensor

        public ConsumerStatisticsHandler(
            string clientId,
            string streamAppId)
        {
            this.clientId = clientId;
            this.streamAppId = streamAppId;
        }

        public void Register(StreamMetricsRegistry metricsRegistry)
        {
            TotalNumberOfMessagesConsumedSensor =
                LibrdKafkaConsumerMetrics.TotalNumberOfMessagesConsumedSensor(threadId, clientId, streamAppId,
                    metricsRegistry);
            TotalNumberOfMessageBytesConsumedSensor =
                LibrdKafkaConsumerMetrics.TotalNumberOfMessageBytesConsumedSensor(threadId, clientId, streamAppId,
                    metricsRegistry);
            NumberOfOpsWaitinInQueueSensor =
                LibrdKafkaConsumerMetrics.NumberOfOpsWaitinInQueueSensor(threadId, clientId, streamAppId,
                    metricsRegistry);
            TotalNumberOfResponsesReceivedFromKafkaSensor =
                LibrdKafkaConsumerMetrics.TotalNumberOfResponsesReceivedFromKafkaSensor(threadId, clientId, streamAppId,
                    metricsRegistry);
            TotalNumberOfBytesReceivedFromKafkaSensor =
                LibrdKafkaConsumerMetrics.TotalNumberOfBytesReceivedFromKafkaSensor(threadId, clientId, streamAppId,
                    metricsRegistry);
            RebalanceAgeSensor =
                LibrdKafkaConsumerMetrics.RebalanceAgeSensor(threadId, clientId, streamAppId, metricsRegistry);
            TotalNumberOfRelabalanceSensor =
                LibrdKafkaConsumerMetrics.TotalNumberOfRelabalanceSensor(threadId, clientId, streamAppId,
                    metricsRegistry);
            TotalNumberOfResponsesReceivedSensor =
                LibrdKafkaConsumerMetrics.TotalNumberOfResponsesReceivedSensor(threadId, clientId, streamAppId,
                    metricsRegistry);
            TotalNumberOfBytesReceivedSensor =
                LibrdKafkaConsumerMetrics.TotalNumberOfBytesReceivedSensor(threadId, clientId, streamAppId,
                    metricsRegistry);
            TotalNumberOfReceivedErrorsSensor =
                LibrdKafkaConsumerMetrics.TotalNumberOfReceivedErrorsSensor(threadId, clientId, streamAppId,
                    metricsRegistry);
            NumberOfConnectionAttempsSensor =
                LibrdKafkaConsumerMetrics.NumberOfConnectionAttempsSensor(threadId, clientId, streamAppId,
                    metricsRegistry);
            NumberOfDisconnectsSensor =
                LibrdKafkaConsumerMetrics.NumberOfDisconnectsSensor(threadId, clientId, streamAppId, metricsRegistry);
            BrokerLatencyAverageMsSensor =
                LibrdKafkaConsumerMetrics.BrokerLatencyAverageMsSensor(threadId, clientId, streamAppId,
                    metricsRegistry);
            BatchSizeAverageBytesSensor =
                LibrdKafkaConsumerMetrics.BatchSizeAverageBytesSensor(threadId, clientId, streamAppId, metricsRegistry);
            BatchMessageCountsAverageSensor =
                LibrdKafkaConsumerMetrics.BatchMessageCountsAverageSensor(threadId, clientId, streamAppId,
                    metricsRegistry);
            ConsumerLagSensor =
                LibrdKafkaConsumerMetrics.ConsumerLagSensor(threadId, clientId, streamAppId, metricsRegistry);
            TotalNumberOfMessagesConsumedByPartitionSensor =
                LibrdKafkaConsumerMetrics.TotalNumberOfMessagesConsumedByPartitionSensor(threadId, clientId,
                    streamAppId, metricsRegistry);
            TotalNumberOfBytesConsumedByPartitionSensor =
                LibrdKafkaConsumerMetrics.TotalNumberOfBytesConsumedByPartitionSensor(threadId, clientId, streamAppId,
                    metricsRegistry);
        }

        public void Publish(Statistics statistics)
        {
            // TODO : 
        }

        public void Unregister(StreamMetricsRegistry metricsRegistry)
        {
            // TODO : 
        }
    }
}