using Streamiz.Kafka.Net.Metrics.Internal;

namespace Streamiz.Kafka.Net.Metrics.Librdkafka
{
    public class ProducerStatisticsHandler : IStatisticsHandler
    {
        private readonly string producerId;
        private readonly string threadId;
        private readonly string streamAppId;

        private Sensor TotalNumberOfMessagesProducedSensor;
        private Sensor TotalNumberOfMessageBytesProducedSensor;
        private Sensor NumberOfOpsWaitinInQueueSensor;
        private Sensor CurrentNumberOfMessagesInProducerQueuesSensor;
        private Sensor CurrentSizeOfMessagesInProducerQueuesSensor;
        private Sensor MaxMessagesAllowedOnProducerQueuesSensor;
        private Sensor MaxSizeOfMessagesAllowedOnProducerQueuesSensor;
        private Sensor TotalNumberOfRequestSentToKafkaSensor;
        private Sensor TotalNumberOfBytesTransmittedToKafkaSensor;

        //PER BROKER (add Broker NodeId as label)
        private Sensor NumberOfRequestAwaitingTransmissionSensor;
        private Sensor NumberOfMessagesAwaitingTransmissionSensor;
        private Sensor NumberOfRequestInFlightSensor;
        private Sensor NumberOfMessagesInFlightSensor;
        private Sensor TotalNumberOfRequestSentSensor;
        private Sensor TotalNumberOfBytesSentSensor;
        private Sensor TotalNumberOfTransmissionErrorsSensor;
        private Sensor TotalNumberOfRequestRetriesSensor;
        private Sensor TotalNumberOfRequestTimeoutSensor;
        private Sensor NumberOfConnectionAttempsSensor; // Including successful, failed and name resolution failures
        private Sensor NumberOfDisconnectsSensor;
        private Sensor InternalQueueProducerLatencyAverageMsSensor;
        private Sensor InternalRequestQueueLatencyAverageMsSensor;
        private Sensor BrokerLatencyAverageMsSensor;

        // Per Topic(add topic name as label)
        private Sensor BatchSizeAverageBytesSensor;
        private Sensor BatchMessageCountsAverageSensor;

        //  Per Partition(topic brokder id PartitionId as label)
        private Sensor PartitionTotalNumberOfMessagesProducedSensor;
        private Sensor PartitionTotalNumberOfBytesProducedSensor;
        private Sensor PartitionNumberOfMessagesInFlightSensor;
        private Sensor PartitionNextExpectedAckSequenceSensor;
        private Sensor PartitionLastInternalMessageIdAckedSensor;

        public ProducerStatisticsHandler(string producerId, string streamAppId)
        {
            this.producerId = producerId;
            this.streamAppId = streamAppId;
        }

        public void Register(StreamMetricsRegistry metricsRegistry)
        {
            TotalNumberOfMessagesProducedSensor =
                LibrdKafkaProducerMetrics.TotalNumberOfMessagesProducedSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            TotalNumberOfMessageBytesProducedSensor =
                LibrdKafkaProducerMetrics.TotalNumberOfMessageBytesProducedSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            NumberOfOpsWaitinInQueueSensor =
                LibrdKafkaProducerMetrics.NumberOfOpsWaitinInQueueSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            CurrentNumberOfMessagesInProducerQueuesSensor =
                LibrdKafkaProducerMetrics.CurrentNumberOfMessagesInProducerQueuesSensor(threadId, producerId,
                    streamAppId, metricsRegistry);
            CurrentSizeOfMessagesInProducerQueuesSensor =
                LibrdKafkaProducerMetrics.CurrentSizeOfMessagesInProducerQueuesSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            MaxMessagesAllowedOnProducerQueuesSensor =
                LibrdKafkaProducerMetrics.MaxMessagesAllowedOnProducerQueuesSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            MaxSizeOfMessagesAllowedOnProducerQueuesSensor =
                LibrdKafkaProducerMetrics.MaxSizeOfMessagesAllowedOnProducerQueuesSensor(threadId, producerId,
                    streamAppId, metricsRegistry);
            TotalNumberOfRequestSentToKafkaSensor =
                LibrdKafkaProducerMetrics.TotalNumberOfRequestSentToKafkaSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            TotalNumberOfBytesTransmittedToKafkaSensor =
                LibrdKafkaProducerMetrics.TotalNumberOfBytesTransmittedToKafkaSensor(threadId, producerId, streamAppId,
                    metricsRegistry);

            //PER BROKER (add Broker NodeId as label)
            NumberOfRequestAwaitingTransmissionSensor =
                LibrdKafkaProducerMetrics.NumberOfRequestAwaitingTransmissionSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            NumberOfMessagesAwaitingTransmissionSensor =
                LibrdKafkaProducerMetrics.NumberOfMessagesAwaitingTransmissionSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            NumberOfRequestInFlightSensor =
                LibrdKafkaProducerMetrics.NumberOfRequestInFlightSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            NumberOfMessagesInFlightSensor =
                LibrdKafkaProducerMetrics.NumberOfMessagesInFlightSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            TotalNumberOfRequestSentSensor =
                LibrdKafkaProducerMetrics.TotalNumberOfRequestSentSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            TotalNumberOfBytesSentSensor =
                LibrdKafkaProducerMetrics.TotalNumberOfBytesSentSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            TotalNumberOfTransmissionErrorsSensor =
                LibrdKafkaProducerMetrics.TotalNumberOfTransmissionErrorsSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            TotalNumberOfRequestRetriesSensor =
                LibrdKafkaProducerMetrics.TotalNumberOfRequestRetriesSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            TotalNumberOfRequestTimeoutSensor =
                LibrdKafkaProducerMetrics.TotalNumberOfRequestTimeoutSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            NumberOfConnectionAttempsSensor =
                LibrdKafkaProducerMetrics.NumberOfConnectionAttempsSensor(threadId, producerId, streamAppId,
                    metricsRegistry); // Including successful, failed and name resolution failures
            NumberOfDisconnectsSensor =
                LibrdKafkaProducerMetrics.NumberOfDisconnectsSensor(threadId, producerId, streamAppId, metricsRegistry);
            InternalQueueProducerLatencyAverageMsSensor =
                LibrdKafkaProducerMetrics.InternalQueueProducerLatencyAverageMsSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            InternalRequestQueueLatencyAverageMsSensor =
                LibrdKafkaProducerMetrics.InternalRequestQueueLatencyAverageMsSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            BrokerLatencyAverageMsSensor =
                LibrdKafkaProducerMetrics.BrokerLatencyAverageMsSensor(threadId, producerId, streamAppId,
                    metricsRegistry);

            // Per Topic(add topic name as label)
            BatchSizeAverageBytesSensor =
                LibrdKafkaProducerMetrics.BatchSizeAverageBytesSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            BatchMessageCountsAverageSensor =
                LibrdKafkaProducerMetrics.BatchMessageCountsAverageSensor(threadId, producerId, streamAppId,
                    metricsRegistry);

            //  Per Partition(topic brokder id PartitionId as label)
            PartitionTotalNumberOfMessagesProducedSensor =
                LibrdKafkaProducerMetrics.PartitionTotalNumberOfMessagesProducedSensor(threadId, producerId,
                    streamAppId, metricsRegistry);
            PartitionTotalNumberOfBytesProducedSensor =
                LibrdKafkaProducerMetrics.PartitionTotalNumberOfBytesProducedSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            PartitionNumberOfMessagesInFlightSensor =
                LibrdKafkaProducerMetrics.PartitionNumberOfMessagesInFlightSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            PartitionNextExpectedAckSequenceSensor =
                LibrdKafkaProducerMetrics.PartitionNextExpectedAckSequenceSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            PartitionLastInternalMessageIdAckedSensor =
                LibrdKafkaProducerMetrics.PartitionLastInternalMessageIdAckedSensor(threadId, producerId, streamAppId,
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