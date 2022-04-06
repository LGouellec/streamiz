namespace Streamiz.Kafka.Net.Metrics.Internal
{
    internal class LibrdKafkaConsumerMetrics : LibrdKafkaBaseMetrics
    {
        internal static string TOTAL_MESSAGE_CONSUMED = "messages-consumed-total";

        internal static string TOTAL_MESSAGE_CONSUMED_DESCRIPTION =
            "Total number of messages consumed, not including ignored messages (due to offset, etc), from Kafka brokers";

        internal static string TOTAL_BYTES_CONSUMED = "bytes-consumed-total";
        internal static string TOTAL_BYTES_CONSUMED_DESCRIPTION = "Total number of bytes received from Kafka brokers";

        internal static string TOTAL_OPS_WAITING_QUEUE = "ops-waiting-queue-total";

        internal static string TOTAL_OPS_WAITING_QUEUE_DESCRIPTION =
            "Number of ops (callbacks, events, etc) waiting in queue for application to serve with rd_kafka_poll().";

        internal static string TOTAL_RESPONSES_RECEIVED = "responses-received-total";

        internal static string TOTAL_RESPONSES_RECEIVED_DESCRIPTION =
            "Total number of responses received from Kafka brokers.";

        internal static string TOTAL_RESPONSES_BYTES_RECEIVED = "responses-bytes-received-total";

        internal static string TOTAL_RESPONSES_BYTES_RECEIVED_DESCRIPTION =
            "Total number of reponses bytes received from Kafka brokers.";

        internal static string TIME_REBALANCE_AGE = "time-rebalance-age";

        internal static string TIME_REBALANCE_AGE_DESCRIPTION =
            "Time elapsed since last rebalance (assign or revoke) (milliseconds).";

        internal static string TOTAL_REBALANCE = "rebalance-total";
        internal static string TOTAL_REBALANCE_DESCRIPTION = "Total number of rebalances (assign or revoke).";

        //PER BROKER (add Broker NodeId as label)
        internal static string TOTAL_BROKER_RESPONSES_RECEIVED = "broker-responses-received-total";
        internal static string TOTAL_BROKER_RESPONSES_RECEIVED_DESCRIPTION = "Total number of responses received.";

        internal static string TOTAL_BROKER_RESPONSES_BYTES_RECEIVED = "broker-responses-bytes-received-total";
        internal static string TOTAL_BROKER_RESPONSES_BYTES_RECEIVED_DESCRIPTION = "Total number of bytes received.";

        internal static string TOTAL_BROKER_ERROR_RECEIVED = "broker-error-received-total";
        internal static string TOTAL_BROKER_ERROR_RECEIVED_DESCRIPTION = "Total number of receive errors.";

        internal static string TOTAL_BROKER_CONNECTION = "broker-connection-total";

        internal static string TOTAL_BROKER_CONNECTION_DESCRIPTION =
            "Number of connection attempts, including successful and failed, and name resolution failures.";

        internal static string TOTAL_BROKER_DISCONNECTION = "broker-disconnection-total";

        internal static string TOTAL_BROKER_DISCONNECTION_DESCRIPTION =
            "Number of disconnects (triggered by broker, network, load-balancer, etc.).";

        internal static string BROKER_LATENCY_AVG = "broker-latency-avg";
        internal static string BROKER_LATENCY_AVG_DESCRIPTION = "Broker latency / round-trip time in microseconds.";

        // Per Topic (add topic name as label)		
        internal static string BATCH_SIZE_AVG = "topic-batch-size-bytes-avg";
        internal static string BATCH_SIZE_AVG_DESCRIPTION = "Batch sizes in bytes average.";

        internal static string BATCH_MESSAGE_COUNT_AVG = "topic-batch-message-count-avg";
        internal static string BATCH_MESSAGE_COUNT_AVG_DESCRIPTION = "Batch message counts average.";

        // Per partition(topic broker_id PartitionId as label)
        internal static string COUNSUMER_LAG = "consumer-lag";

        internal static string COUNSUMER_LAG_DESCRIPTION =
            "Difference between (hi_offset or ls_offset) - max(app_offset, committed_offset). hi_offset is used when isolation.level=read_uncommitted, otherwise ls_offset.";

        internal static string TOTAL_NUMBER_MESSAGE_CONSUMED = "partition-messages-consumed-total";

        internal static string TOTAL_NUMBER_MESSAGE_CONSUMED_DESCRIPTION =
            "Total number of messages consumed, not including ignored messages (due to offset, etc).";

        internal static string TOTAL_NUMBER_BYTES_CONSUMED = "partition-bytes-consumed-total";
        internal static string TOTAL_NUMBER_BYTES_CONSUMED_DESCRIPTION = "Total number of bytes received for rxmsgs.";

        #region Top Metrics

        public static Sensor TotalNumberOfMessagesConsumedSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_MESSAGE_CONSUMED,
                TOTAL_MESSAGE_CONSUMED_DESCRIPTION,
                (false, false, false),
                true,
                metricsRegistry);
        }

        public static Sensor TotalNumberOfMessageBytesConsumedSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_BYTES_CONSUMED,
                TOTAL_BYTES_CONSUMED_DESCRIPTION,
                (false, false, false),
                true,
                metricsRegistry);
        }

        public static Sensor NumberOfOpsWaitinInQueueSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_OPS_WAITING_QUEUE,
                TOTAL_OPS_WAITING_QUEUE_DESCRIPTION,
                (false, false, false),
                true,
                metricsRegistry);
        }

        public static Sensor TotalNumberOfResponsesReceivedFromKafkaSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_RESPONSES_RECEIVED,
                TOTAL_RESPONSES_RECEIVED_DESCRIPTION,
                (false, false, false),
                true,
                metricsRegistry);
        }

        public static Sensor TotalNumberOfBytesReceivedFromKafkaSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_RESPONSES_BYTES_RECEIVED,
                TOTAL_RESPONSES_BYTES_RECEIVED_DESCRIPTION,
                (false, false, false),
                true,
                metricsRegistry);
        }

        public static Sensor RebalanceAgeSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TIME_REBALANCE_AGE,
                TIME_REBALANCE_AGE_DESCRIPTION,
                (false, false, false),
                true,
                metricsRegistry);
        }


        public static Sensor TotalNumberOfRelabalanceSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_REBALANCE,
                TOTAL_REBALANCE_DESCRIPTION,
                (false, false, false),
                true,
                metricsRegistry);
        }

        #endregion

        #region Broker Metrics

        public static Sensor TotalNumberOfResponsesReceivedSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_BROKER_RESPONSES_RECEIVED,
                TOTAL_BROKER_RESPONSES_RECEIVED_DESCRIPTION,
                (true, false, false),
                true,
                metricsRegistry);
        }

        public static Sensor TotalNumberOfBytesReceivedSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_BROKER_RESPONSES_BYTES_RECEIVED,
                TOTAL_BROKER_RESPONSES_BYTES_RECEIVED_DESCRIPTION,
                (true, false, false),
                true,
                metricsRegistry);
        }

        public static Sensor TotalNumberOfReceivedErrorsSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_BROKER_ERROR_RECEIVED,
                TOTAL_BROKER_ERROR_RECEIVED_DESCRIPTION,
                (true, false, false),
                true,
                metricsRegistry);
        }

        public static Sensor NumberOfConnectionAttempsSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_BROKER_CONNECTION,
                TOTAL_BROKER_CONNECTION_DESCRIPTION,
                (true, false, false),
                true,
                metricsRegistry);
        }

        public static Sensor NumberOfDisconnectsSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_BROKER_DISCONNECTION,
                TOTAL_BROKER_DISCONNECTION_DESCRIPTION,
                (true, false, false),
                true,
                metricsRegistry);
        }

        public static Sensor BrokerLatencyAverageMsSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor(
                threadId,
                librdKafkaClientId,
                streamAppId,
                BROKER_LATENCY_AVG,
                BROKER_LATENCY_AVG_DESCRIPTION,
                (true, false, false),
                true,
                metricsRegistry);
        }

        #endregion

        #region Topic Metrics

        public static Sensor BatchSizeAverageBytesSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor(
                threadId,
                librdKafkaClientId,
                streamAppId,
                BATCH_SIZE_AVG,
                BATCH_SIZE_AVG_DESCRIPTION,
                (true, true, false),
                true,
                metricsRegistry);
        }

        public static Sensor BatchMessageCountsAverageSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor(
                threadId,
                librdKafkaClientId,
                streamAppId,
                BATCH_MESSAGE_COUNT_AVG,
                BATCH_MESSAGE_COUNT_AVG_DESCRIPTION,
                (true, true, false),
                true,
                metricsRegistry);
        }

        #endregion

        #region Partition Metrics

        public static Sensor ConsumerLagSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor(
                threadId,
                librdKafkaClientId,
                streamAppId,
                COUNSUMER_LAG,
                COUNSUMER_LAG_DESCRIPTION,
                (true, true, true),
                true,
                metricsRegistry);
        }

        public static Sensor TotalNumberOfMessagesConsumedByPartitionSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_NUMBER_MESSAGE_CONSUMED,
                TOTAL_NUMBER_MESSAGE_CONSUMED_DESCRIPTION,
                (true, true, true),
                true,
                metricsRegistry);
        }

        public static Sensor TotalNumberOfBytesConsumedByPartitionSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_NUMBER_BYTES_CONSUMED,
                TOTAL_NUMBER_BYTES_CONSUMED_DESCRIPTION,
                (true, true, true),
                true,
                metricsRegistry);
        }

        #endregion
    }
}