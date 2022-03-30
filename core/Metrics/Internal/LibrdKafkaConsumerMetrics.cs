namespace Streamiz.Kafka.Net.Metrics.Internal
{
    public class LibrdKafkaConsumerMetrics
    {
        internal static string TOTAL_MESSAGE_CONSUMED = "total-messages-consumed";
        internal static string TOTAL_MESSAGE_CONSUMED_DESCRIPTION = "Total number of messages consumed, not including ignored messages (due to offset, etc), from Kafka brokers";
        
        internal static string TOTAL_BYTES_CONSUMED = "total-bytes-consumed";
        internal static string TOTAL_BYTES_CONSUMED_DESCRIPTION = "Total number of bytes received from Kafka brokers";
        
        internal static string TOTAL_OPS_WAITING_QUEUE = "total-ops-waiting-queue";
        internal static string TOTAL_OPS_WAITING_QUEUE_DESCRIPTION = "Number of ops (callbacks, events, etc) waiting in queue for application to serve with rd_kafka_poll().";
        
        internal static string TOTAL_RESPONSES_RECEIVED = "total-responses-received";
        internal static string TOTAL_RESPONSES_RECEIVED_DESCRIPTION = "Total number of responses received from Kafka brokers.";

        internal static string TOTAL_RESPONSES_BYTES_RECEIVED = "total-responses-bytes-received";
        internal static string TOTAL_RESPONSES_BYTES_RECEIVED_DESCRIPTION = "Total number of reponses bytes received from Kafka brokers.";

        internal static string TIME_REBALANCE_AGE = "time-rebalance-age";
        internal static string TIME_REBALANCE_AGE_DESCRIPTION = "Time elapsed since last rebalance (assign or revoke) (milliseconds).";
        
        internal static string TOTAL_REBALANCE = "total-rebalance";
        internal static string TOTAL_REBALANCE_DESCRIPTION = "Total number of rebalances (assign or revoke).";

        // TODO : 
    }
}