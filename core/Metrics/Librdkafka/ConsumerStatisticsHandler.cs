namespace Streamiz.Kafka.Net.Metrics.Librdkafka
{
    public class ConsumerStatisticsHandler : IStatisticsHandler
    {
        private readonly string groupId;
        private readonly string clientId;

        private Sensor TotalNumberOfMessagesConsumed;
        private Sensor TotalNumberOfMessageBytesConsumed;
        private Sensor NumberOfOpsWaitinInQueue; // Sensor
        private Sensor TotalNumberOfResponsesReceivedFromKafka;
        private Sensor TotalNumberOfBytesReceivedFromKafka;
        private Sensor RebalanceAge; // Sensor
        private Sensor TotalNumberOfRelabalance; // Sensor assign or revoke

        //PER BROKER (add Broker NodeId as label)
        private Sensor TotalNumberOfResponsesReceived;
        private Sensor TotalNumberOfBytesReceived;
        private Sensor TotalNumberOfReceivedErrors;
        private Sensor NumberOfConnectionAttemps; // Including successful, failed and name resolution failures
        private Sensor NumberOfDisconnects;
        private Sensor BrokerLatencyAverageMs;

        // Per Topic (add topic name as label)			
        private Sensor BatchSizeAverageBytes;
        private Sensor BatchMessageCountsAverage;

        // Per partition(topic brokder id PartitionId as label)

        private Sensor ConsumerLag; // Sensor
        private Sensor TotalNumberOfMessagesconsumed; // Sensor
        private Sensor TotalNumberOfBytesConsumed; // Sensor

        public ConsumerStatisticsHandler(string groupId, string clientId)
        {
            this.groupId = groupId;
            this.clientId = clientId;
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