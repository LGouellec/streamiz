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
            TotalNumberOfMessagesConsumedSensor.Record(statistics.TotalNumberOfMessagesConsumed);
            TotalNumberOfMessageBytesConsumedSensor.Record(statistics.TotalNumberOfMessageBytesConsumed);
            NumberOfOpsWaitinInQueueSensor.Record(statistics.NumberOfOpsWaitinInQueue);
            TotalNumberOfResponsesReceivedFromKafkaSensor.Record(statistics.TotalNumberOfResponsesReceivedFromKafka);
            TotalNumberOfBytesReceivedFromKafkaSensor.Record(statistics.TotalNumberOfBytesReceivedFromKafka);
            RebalanceAgeSensor.Record(statistics.ConsumerGroups.RebalanceAge);
            TotalNumberOfRelabalanceSensor.Record(statistics.ConsumerGroups.TotalNumberOfRelabalance);

            foreach (var broker in statistics.Brokers)
            {
                TotalNumberOfResponsesReceivedSensor.ChangeTagValue(LibrdKafkaBaseMetrics.BROKER_ID_TAG, broker.Value.NodeId.ToString()).Record(broker.Value.TotalNumberOfResponsesReceived);
                TotalNumberOfBytesReceivedSensor.ChangeTagValue(LibrdKafkaBaseMetrics.BROKER_ID_TAG, broker.Value.NodeId.ToString()).Record(broker.Value.TotalNumberOfBytesReceived);
                TotalNumberOfReceivedErrorsSensor.ChangeTagValue(LibrdKafkaBaseMetrics.BROKER_ID_TAG, broker.Value.NodeId.ToString()).Record(broker.Value.TotalNumberOfReceivedErrors);
                NumberOfConnectionAttempsSensor.ChangeTagValue(LibrdKafkaBaseMetrics.BROKER_ID_TAG, broker.Value.NodeId.ToString()).Record(broker.Value.NumberOfConnectionAttemps);
                NumberOfDisconnectsSensor.ChangeTagValue(LibrdKafkaBaseMetrics.BROKER_ID_TAG, broker.Value.NodeId.ToString()).Record(broker.Value.NumberOfDisconnects);
                BrokerLatencyAverageMsSensor.ChangeTagValue(LibrdKafkaBaseMetrics.BROKER_ID_TAG, broker.Value.NodeId.ToString()).Record(broker.Value.BrokerLatency.Average);
            }

            foreach (var topic in statistics.Topics)
            {
                BatchSizeAverageBytesSensor.ChangeTagValue(LibrdKafkaBaseMetrics.TOPIC_TAG, topic.Value.TopicName).Record(topic.Value.BatchSize.Average);
                BatchMessageCountsAverageSensor.ChangeTagValue(LibrdKafkaBaseMetrics.TOPIC_TAG, topic.Value.TopicName).Record(topic.Value.BatchMessageCounts.Average);

                foreach (var partition in topic.Value.Partitions)
                {
                    ConsumerLagSensor
                        .ChangeTagValue(LibrdKafkaBaseMetrics.TOPIC_TAG, topic.Value.TopicName)
                        .ChangeTagValue(LibrdKafkaBaseMetrics.BROKER_ID_TAG, partition.Value.BrokerId.ToString())
                        .ChangeTagValue(LibrdKafkaBaseMetrics.PARTITION_ID_TAG, partition.Value.PartitionId.ToString())
                        .Record(partition.Value.ConsumerLag);
                    
                    TotalNumberOfMessagesConsumedByPartitionSensor
                        .ChangeTagValue(LibrdKafkaBaseMetrics.TOPIC_TAG, topic.Value.TopicName)
                        .ChangeTagValue(LibrdKafkaBaseMetrics.BROKER_ID_TAG, partition.Value.BrokerId.ToString())
                        .ChangeTagValue(LibrdKafkaBaseMetrics.PARTITION_ID_TAG, partition.Value.PartitionId.ToString())
                        .Record(partition.Value.TotalNumberOfMessagesconsumed);
                    
                    TotalNumberOfBytesConsumedByPartitionSensor
                        .ChangeTagValue(LibrdKafkaBaseMetrics.TOPIC_TAG, topic.Value.TopicName)
                        .ChangeTagValue(LibrdKafkaBaseMetrics.BROKER_ID_TAG, partition.Value.BrokerId.ToString())
                        .ChangeTagValue(LibrdKafkaBaseMetrics.PARTITION_ID_TAG, partition.Value.PartitionId.ToString())
                        .Record(partition.Value.TotalNumberOfBytesConsumed);
                }

            }
        }

        public void Unregister(StreamMetricsRegistry metricsRegistry)
        {
            // TODO : 
        }
    }
}