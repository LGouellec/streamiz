namespace Streamiz.Kafka.Net.Metrics.Librdkafka
{
    public class ProducerStatisticsHandler : IStatisticsHandler
    {
        private readonly string producerId;

        private Sensor TotalNumberOfMessagesProduced;
        private Sensor TotalNumberOfMessageBytesProduced;
        private Sensor NumberOfOpsWaitinInQueue; // Sensor
        private Sensor CurrentNumberOfMessagesInProducerQueues; // Sensor
        private Sensor CurrentSizeOfMessagesInProducerQueues; // Sensor
        private Sensor MaxMessagesAllowedOnProducerQueues;
        private Sensor MaxSizeOfMessagesAllowedOnProducerQueues;
        private Sensor TotalNumberOfRequestSentToKafka;
        private Sensor TotalNumberOfBytesTransmittedToKafka;

        //PER BROKER (add Broker NodeId as label)
        private Sensor NumberOfRequestAwaitingTransmission; // Sensor
        private Sensor NumberOfMessagesAwaitingTransmission; //Sensor
        private Sensor NumberOfRequestInFlight; // Sensor
        private Sensor NumberOfMessagesInFlight; // Sensor
        private Sensor TotalNumberOfRequestSent;
        private Sensor TotalNumberOfBytesSent;
        private Sensor TotalNumberOfTransmissionErrors;
        private Sensor TotalNumberOfRequestRetries;
        private Sensor TotalNumberOfRequestTimeout;
        private Sensor NumberOfConnectionAttemps; // Including successful, failed and name resolution failures
        private Sensor NumberOfDisconnects;
        private Sensor InternalQueueProducerLatencyAverageMs;
        private Sensor InternalRequestQueueLatencyAverageMs;
        private Sensor BrokerLatencyAverageMs;

        // Per Topic(add topic name as label)
        private Sensor BatchSizeAverageBytes;
        private Sensor BatchMessageCountsAverage;

        //  Per Partition(topic brokder id PartitionId as label)
        private Sensor PartitionTotalNumberOfMessagesProduced; // Sensor
        private Sensor PartitionTotalNumberOfBytesProduced; // Sensor
        private Sensor PartitionNumberOfMessagesInFlight; // Sensor
        private Sensor PartitionNextExpectedAckSequence; // Sensor idempotent producer
        private Sensor PartitionLastInternalMessageIdAcked; // Sensor idempotent producer

        public ProducerStatisticsHandler(string producerId)
        {
            this.producerId = producerId;
        }

        public void Register(StreamMetricsRegistry metricsRegistry)
        {
            // TODO : 
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