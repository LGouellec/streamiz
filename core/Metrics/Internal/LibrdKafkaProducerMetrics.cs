using Streamiz.Kafka.Net.Metrics.Librdkafka;

namespace Streamiz.Kafka.Net.Metrics.Internal
{
    internal class LibrdKafkaProducerMetrics : LibrdKafkaBaseMetrics
    {
        internal static string TOTAL_MESSAGE_PRODUCED = "messages-produced-total";

        internal static string TOTAL_MESSAGE_PRODUCED_DESCRIPTION =
            "Total number of messages transmitted (produced) to Kafka brokers";

        internal static string TOTAL_BYTES_PRODUCED = "bytes-produced-total";

        internal static string TOTAL_BYTES_PRODUCED_DESCRIPTION =
            "Total number of message bytes (including framing, such as per-Message framing and MessageSet/batch framing) transmitted to Kafka brokers";

        internal static string TOTAL_OPS_WAITING_QUEUE = "ops-waiting-queue-total";

        internal static string TOTAL_OPS_WAITING_QUEUE_DESCRIPTION =
            "Number of ops (callbacks, events, etc) waiting in queue for application to serve with rd_kafka_poll()";

        internal static string CURRENT_TOTAL_MESSAGE_PRODUCER_QUEUE = "messages-queue-current-total";

        internal static string CURRENT_TOTAL_MESSAGE_PRODUCER_QUEUE_DESCRIPTION =
            "Current number of messages in producer queues";

        internal static string CURRENT_TOTAL_MESSAGE_SIZE_PRODUCER_QUEUE = "bytes-queue-current-total";

        internal static string CURRENT_TOTAL_MESSAGE_SIZE_PRODUCER_QUEUE_DESCRIPTION =
            "Current total size of messages in producer queues";

        internal static string MAXIMUM_MESSAGE_PRODUCER_QUEUE = "messages-queue-max";

        internal static string MAXIMUM_MESSAGE_PRODUCER_QUEUE_DESCRIPTION =
            "Threshold: maximum number of messages allowed allowed on the producer queues";

        internal static string MAXIMUM_SIZE_PRODUCER_QUEUE = "bytes-queue-max";

        internal static string MAXIMUM_SIZE_PRODUCER_QUEUE_DESCRIPTION =
            "Threshold: maximum total size of messages allowed on the producer queues";

        internal static string TOTAL_REQUEST_SENT = "request-produced-total";
        internal static string TOTAL_REQUEST_SENT_DESCRIPTION = "Total number of requests sent to Kafka brokers";

        internal static string TOTAL_REQUEST_BYTES_SENT = "request-bytes-produced-total";

        internal static string TOTAL_REQUEST_BYTES_SENT_DESCRIPTION =
            "Total number of bytes transmitted to Kafka brokers";

        //PER BROKER (add Broker NodeId as label)

        internal static string TOTAL_BROKER_REQUEST_AWAITING = "broker-request-awaiting-total";

        internal static string TOTAL_BROKER_REQUEST_AWAITING_DESCRIPTION =
            "Number of requests awaiting transmission to broker";

        internal static string TOTAL_BROKER_MESSAGE_AWAITING = "broker-message-awaiting-total";

        internal static string TOTAL_BROKER_MESSAGE_AWAITING_DESCRIPTION =
            "Number of messages awaiting transmission to broker";

        internal static string TOTAL_BROKER_REQUEST_INFLIFGHT_AWAITING_RESPONSE =
            "broker-request-in-flight-total";

        internal static string TOTAL_BROKER_REQUEST_INFLIFGHT_AWAITING_RESPONSE_DESCRIPTION =
            "Number of requests in-flight to broker awaiting response";

        internal static string TOTAL_BROKER_MESSAGE_INFLIFGHT_AWAITING_RESPONSE =
            "broker-message-in-flight-total";

        internal static string TOTAL_BROKER_MESSAGE_INFLIFGHT_AWAITING_RESPONSE_DESCRIPTION =
            "Number of messages in-flight to broker awaiting response";

        internal static string TOTAL_BROKER_REQUEST_SENT = "broker-request-sent-total";
        internal static string TOTAL_BROKER_REQUEST_SENT_DESCRIPTION = "Total number of requests sent";

        internal static string TOTAL_BROKER_BYTES_SENT = "broker-request-sent-bytes-total";
        internal static string TOTAL_BROKER_BYTES_SENT_DESCRIPTION = "Total number of bytes sent";

        internal static string TOTAL_BROKER_TRANSMISSION_ERROR = "broker-error-total";
        internal static string TOTAL_BROKER_TRANSMISSION_ERROR_DESCRIPTION = "Total number of transmission errors";

        internal static string TOTAL_BROKER_REQUEST_RETRIES = "broker-retries-total";
        internal static string TOTAL_BROKER_REQUEST_RETRIES_DESCRIPTION = "Total number of request retries";

        internal static string TOTAL_BROKER_REQUEST_TIMEOUT = "broker-timeout-total";
        internal static string TOTAL_BROKER_REQUEST_TIMEOUT_DESCRIPTION = "Total number of requests timed out";

        internal static string TOTAL_BROKER_CONNECTION = "broker-connection-total";

        internal static string TOTAL_BROKER_CONNECTION_DESCRIPTION =
            "Number of connection attempts, including successful and failed, and name resolution failures.";

        internal static string TOTAL_BROKER_DISCONNECTION = "broker-disconnection-total";

        internal static string TOTAL_BROKER_DISCONNECTION_DESCRIPTION =
            "Number of disconnects (triggered by broker, network, load-balancer, etc.).";

        internal static string BROKER_INTERNAL_QUEUE_LATENCY = "broker-internal-queue-latency-micros";

        internal static string BROKER_INTERNAL_QUEUE_LATENCY_DESCRIPTION =
            "Internal producer queue latency in microseconds. ";

        internal static string BROKER_INTERNAL_REQUEST_QUEUE_LATENCY = "broker-internal-request-queue-latency-micros";

        internal static string BROKER_INTERNAL_REQUEST_QUEUE_LATENCY_DESCRIPTION =
            "Internal request queue latency in microseconds. This is the time between a request is enqueued on the transmit (outbuf) queue and the time the request is written to the TCP socket. Additional buffering and latency may be incurred by the TCP stack and network.";

        internal static string BROKER_LATENCY_AVG = "broker-latency-avg-micros";
        internal static string BROKER_LATENCY_AVG_DESCRIPTION = "Broker latency / round-trip time in microseconds.";

        // Per Topic(add topic name as label)

        internal static string TOPIC_BATCH_SIZE_BYTES_AVG = "topic-batch-size-bytes-avg";
        internal static string TOPIC_BATCH_SIZE_BYTES_AVG_DESCRIPTION = "Batch sizes in bytes average";

        internal static string TOPIC_BATCH_COUNT_AVG = "topic-batch-count-avg";
        internal static string TOPIC_BATCH_COUNT_AVG_DESCRIPTION = "Batch message counts average";

        //  Per Partition(topic brokder id PartitionId as label)

        internal static string TOTAL_PARTITION_MESSAGE_PRODUCED = "partition-messages-produced-total";

        internal static string TOTAL_PARTITION_MESSAGE_PRODUCED_DESCRIPTION =
            "Total number of messages transmitted (produced)";

        internal static string TOTAL_PARTITION_BYTES_PRODUCED = "partition-bytes-produced-total";

        internal static string TOTAL_PARTITION_BYTES_PRODUCED_DESCRIPTION =
            "Total number of bytes transmitted for txmsgs";

        internal static string TOTAL_PARTITION_MESSAGE_INFLIGHT = "partition-messages-in-flight-total";

        internal static string TOTAL_PARTITION_MESSAGE_INFLIGHT_DESCRIPTION =
            "Current number of messages in-flight to/from broker";

        internal static string PARTITION_NEXT_EXPECTED_ACK = "partition-next-expected-ack";

        internal static string PARTITION_NEXT_EXPECTED_ACK_DESCRIPTION =
            "Next expected acked sequence (idempotent producer)";

        internal static string PARTITION_LAST_MESSAGE_ID_ACKED = "partition-last-message-id-acked";

        internal static string PARTITION_LAST_MESSAGE_ID_ACKED_DESCRIPTION =
            "Last acked internal message id (idempotent producer)";

        #region Top Metrics

        public static Sensor TotalNumberOfMessagesProducedSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_MESSAGE_PRODUCED,
                TOTAL_MESSAGE_PRODUCED_DESCRIPTION,
                (false, false, false),
                false,
                metricsRegistry);
        }

        public static Sensor TotalNumberOfMessageBytesProducedSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_BYTES_PRODUCED,
                TOTAL_BYTES_PRODUCED_DESCRIPTION,
                (false, false, false),
                false,
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
                false,
                metricsRegistry);
        }

        public static Sensor CurrentNumberOfMessagesInProducerQueuesSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor(
                threadId,
                librdKafkaClientId,
                streamAppId,
                CURRENT_TOTAL_MESSAGE_PRODUCER_QUEUE,
                CURRENT_TOTAL_MESSAGE_PRODUCER_QUEUE_DESCRIPTION,
                (false, false, false),
                false,
                metricsRegistry);
        }

        public static Sensor CurrentSizeOfMessagesInProducerQueuesSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor(
                threadId,
                librdKafkaClientId,
                streamAppId,
                CURRENT_TOTAL_MESSAGE_SIZE_PRODUCER_QUEUE,
                CURRENT_TOTAL_MESSAGE_SIZE_PRODUCER_QUEUE_DESCRIPTION,
                (false, false, false),
                false,
                metricsRegistry);
        }

        public static Sensor MaxMessagesAllowedOnProducerQueuesSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor(
                threadId,
                librdKafkaClientId,
                streamAppId,
                MAXIMUM_MESSAGE_PRODUCER_QUEUE,
                MAXIMUM_MESSAGE_PRODUCER_QUEUE_DESCRIPTION,
                (false, false, false),
                false,
                metricsRegistry);
        }

        public static Sensor MaxSizeOfMessagesAllowedOnProducerQueuesSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor(
                threadId,
                librdKafkaClientId,
                streamAppId,
                MAXIMUM_SIZE_PRODUCER_QUEUE,
                MAXIMUM_SIZE_PRODUCER_QUEUE_DESCRIPTION,
                (false, false, false),
                false,
                metricsRegistry);
        }

        public static Sensor TotalNumberOfRequestSentToKafkaSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_REQUEST_SENT,
                TOTAL_REQUEST_SENT_DESCRIPTION,
                (false, false, false),
                false,
                metricsRegistry);
        }

        public static Sensor TotalNumberOfBytesTransmittedToKafkaSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_REQUEST_BYTES_SENT,
                TOTAL_REQUEST_BYTES_SENT_DESCRIPTION,
                (false, false, false),
                false,
                metricsRegistry);
        }

        #endregion

        #region Broker Metrics

         public static LibrdKafkaSensor NumberOfRequestAwaitingTransmissionSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor<LibrdKafkaSensor>(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_BROKER_REQUEST_AWAITING,
                TOTAL_BROKER_REQUEST_AWAITING_DESCRIPTION,
                (true, false, false),
                false,
                metricsRegistry);
        }

         public static LibrdKafkaSensor NumberOfMessagesAwaitingTransmissionSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor<LibrdKafkaSensor>(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_BROKER_MESSAGE_AWAITING,
                TOTAL_BROKER_MESSAGE_AWAITING_DESCRIPTION,
                (true, false, false),
                false,
                metricsRegistry);
        }

         public static LibrdKafkaSensor NumberOfRequestInFlightSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor<LibrdKafkaSensor>(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_BROKER_REQUEST_INFLIFGHT_AWAITING_RESPONSE,
                TOTAL_BROKER_REQUEST_INFLIFGHT_AWAITING_RESPONSE_DESCRIPTION,
                (true, false, false),
                false,
                metricsRegistry);
        }

         public static LibrdKafkaSensor NumberOfMessagesInFlightSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor<LibrdKafkaSensor>(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_BROKER_MESSAGE_INFLIFGHT_AWAITING_RESPONSE,
                TOTAL_BROKER_MESSAGE_INFLIFGHT_AWAITING_RESPONSE_DESCRIPTION,
                (true, false, false),
                false,
                metricsRegistry);
        }

         public static LibrdKafkaSensor TotalNumberOfRequestSentSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor<LibrdKafkaSensor>(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_BROKER_REQUEST_SENT,
                TOTAL_BROKER_REQUEST_SENT_DESCRIPTION,
                (true, false, false),
                false,
                metricsRegistry);
        }

         public static LibrdKafkaSensor TotalNumberOfBytesSentSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor<LibrdKafkaSensor>(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_BROKER_BYTES_SENT,
                TOTAL_BROKER_BYTES_SENT_DESCRIPTION,
                (true, false, false),
                false,
                metricsRegistry);
        }

         public static LibrdKafkaSensor TotalNumberOfTransmissionErrorsSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor<LibrdKafkaSensor>(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_BROKER_TRANSMISSION_ERROR,
                TOTAL_BROKER_TRANSMISSION_ERROR_DESCRIPTION,
                (true, false, false),
                false,
                metricsRegistry);
        }

         public static LibrdKafkaSensor TotalNumberOfRequestRetriesSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor<LibrdKafkaSensor>(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_BROKER_REQUEST_RETRIES,
                TOTAL_BROKER_REQUEST_RETRIES_DESCRIPTION,
                (true, false, false),
                false,
                metricsRegistry);
        }

         public static LibrdKafkaSensor TotalNumberOfRequestTimeoutSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor<LibrdKafkaSensor>(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_BROKER_REQUEST_TIMEOUT,
                TOTAL_BROKER_REQUEST_TIMEOUT_DESCRIPTION,
                (true, false, false),
                false,
                metricsRegistry);
        }

         public static LibrdKafkaSensor NumberOfConnectionAttempsSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor<LibrdKafkaSensor>(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_BROKER_CONNECTION,
                TOTAL_BROKER_CONNECTION_DESCRIPTION,
                (true, false, false),
                false,
                metricsRegistry);
        }

         public static LibrdKafkaSensor NumberOfDisconnectsSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor<LibrdKafkaSensor>(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_BROKER_DISCONNECTION,
                TOTAL_BROKER_DISCONNECTION_DESCRIPTION,
                (true, false, false),
                false,
                metricsRegistry);
        }

         public static LibrdKafkaSensor InternalQueueProducerLatencyAverageMsSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor<LibrdKafkaSensor>(
                threadId,
                librdKafkaClientId,
                streamAppId,
                BROKER_INTERNAL_QUEUE_LATENCY,
                BROKER_INTERNAL_REQUEST_QUEUE_LATENCY_DESCRIPTION,
                (true, false, false),
                false,
                metricsRegistry);
        }

         public static LibrdKafkaSensor InternalRequestQueueLatencyAverageMsSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor<LibrdKafkaSensor>(
                threadId,
                librdKafkaClientId,
                streamAppId,
                BROKER_INTERNAL_REQUEST_QUEUE_LATENCY,
                BROKER_INTERNAL_REQUEST_QUEUE_LATENCY_DESCRIPTION,
                (true, false, false),
                false,
                metricsRegistry);
        }

         public static LibrdKafkaSensor BrokerLatencyAverageMsSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor<LibrdKafkaSensor>(
                threadId,
                librdKafkaClientId,
                streamAppId,
                BROKER_LATENCY_AVG,
                BROKER_LATENCY_AVG_DESCRIPTION,
                (true, false, false),
                false,
                metricsRegistry);
        }

        #endregion

        #region Topic Metrics

         public static LibrdKafkaSensor BatchSizeAverageBytesSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor<LibrdKafkaSensor>(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOPIC_BATCH_SIZE_BYTES_AVG,
                TOPIC_BATCH_SIZE_BYTES_AVG_DESCRIPTION,
                (false, true, false),
                false,
                metricsRegistry);
        }

         public static LibrdKafkaSensor BatchMessageCountsAverageSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor<LibrdKafkaSensor>(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOPIC_BATCH_COUNT_AVG,
                TOPIC_BATCH_COUNT_AVG_DESCRIPTION,
                (false, true, false),
                false,
                metricsRegistry);
        }

        #endregion

        #region Partition Metrics

         public static LibrdKafkaSensor PartitionTotalNumberOfMessagesProducedSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor<LibrdKafkaSensor>(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_PARTITION_MESSAGE_PRODUCED,
                TOTAL_PARTITION_MESSAGE_PRODUCED_DESCRIPTION,
                (true, true, true),
                false,
                metricsRegistry);
        }

         public static LibrdKafkaSensor PartitionTotalNumberOfBytesProducedSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor<LibrdKafkaSensor>(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_PARTITION_BYTES_PRODUCED,
                TOTAL_PARTITION_BYTES_PRODUCED_DESCRIPTION,
                (true, true, true),
                false,
                metricsRegistry);
        }

         public static LibrdKafkaSensor PartitionNumberOfMessagesInFlightSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor<LibrdKafkaSensor>(
                threadId,
                librdKafkaClientId,
                streamAppId,
                TOTAL_PARTITION_MESSAGE_INFLIGHT,
                TOTAL_PARTITION_MESSAGE_INFLIGHT_DESCRIPTION,
                (true, true, true),
                false,
                metricsRegistry);
        }

         public static LibrdKafkaSensor PartitionNextExpectedAckSequenceSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor<LibrdKafkaSensor>(
                threadId,
                librdKafkaClientId,
                streamAppId,
                PARTITION_NEXT_EXPECTED_ACK,
                PARTITION_NEXT_EXPECTED_ACK_DESCRIPTION,
                (true, true, true),
                false,
                metricsRegistry);
        }

         public static LibrdKafkaSensor PartitionLastInternalMessageIdAckedSensor(
            string threadId,
            string librdKafkaClientId,
            string streamAppId,
            StreamMetricsRegistry metricsRegistry)
        {
            return CreateSensor<LibrdKafkaSensor>(
                threadId,
                librdKafkaClientId,
                streamAppId,
                PARTITION_LAST_MESSAGE_ID_ACKED,
                PARTITION_LAST_MESSAGE_ID_ACKED_DESCRIPTION,
                (true, true, true),
                false,
                metricsRegistry);
        }

        #endregion
    }
}