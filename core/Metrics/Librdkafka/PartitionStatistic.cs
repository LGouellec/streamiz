using Newtonsoft.Json;

namespace Streamiz.Kafka.Net.Metrics.Librdkafka
{
    /// <summary>
    /// JSON Model from : https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md
    /// </summary>
    internal class PartitionStatistic
    {

        public enum PartitionState
        {
            NONE, STOPPING, STOPPED, OFFSETQUERY, OFFSETWAIT, ACTIVE
        }

        [JsonProperty(PropertyName = "partition")]
        public long PartitionId;

        [JsonProperty(PropertyName = "broker")]
        public long BrokerId;

        [JsonProperty(PropertyName = "leader")]
        public long BrokerLeaderId;

        [JsonProperty(PropertyName = "desired")]
        public bool PartitionIsDesiredByApplication;

        [JsonProperty(PropertyName = "unknown")]
        public bool IsPartitionUnknown; // not seen in topic metadata from broker

        [JsonProperty(PropertyName = "msgq_cnt")]
        public long NumberOfMessagesWaitingToBeProduced; // In first-Level queue Gauge

        [JsonProperty(PropertyName = "msgq_bytes")]
        public long NumberOfBytesWaitingToBeProduced; // Gauge

        [JsonProperty(PropertyName = "xmit_msgq_cnt")]
        public long NumberOfMessagesReadyToBeProduced; // Gauge

        [JsonProperty(PropertyName = "xmit_msgq_bytes")]
        public long NumberOfBytesReadyToBeProduced; // Gauge

        [JsonProperty(PropertyName = "fetchq_cnt")]
        public long NumberOfPreFetchedMessages; // Gauge

        [JsonProperty(PropertyName = "fetchq_size")]
        public long NumberOfBytesPreFetchedMessages; // Gauge

        [JsonProperty(PropertyName = "fetch_size")]
        public string _FetchState;

        public PartitionState FetchState;

        [JsonProperty(PropertyName = "query_offset")]
        public long CurrentOffsetQuery; // Gauge

        [JsonProperty(PropertyName = "next_offset")]
        public long NextOffsetToFetch; // Gauge

        [JsonProperty(PropertyName = "app_offset")]
        public long AppOffset; // Gauge

        [JsonProperty(PropertyName = "stored_offset")]
        public long OffsetToBeCommited; // Gauge

        [JsonProperty(PropertyName = "committed_offset")]
        public long LastOffsetCommitted; // Gauge

        [JsonProperty(PropertyName = "eof_offset")]
        public long LastPartitionEofSignaledOffset; // Gauge

        [JsonProperty(PropertyName = "lo_offset")]
        public long PartitionLowWatermarkOffset; // Gauge

        [JsonProperty(PropertyName = "hi_offset")]
        public long PartitionHighWatermarkOffset; // Gauge

        [JsonProperty(PropertyName = "ls_offset")]
        public long PartitionLastStableOffset; // Gauge

        [JsonProperty(PropertyName = "consumer_lag")]
        public long ConsumerLag; // Gauge
        
        [JsonProperty(PropertyName = "consumer_lag_stored")]
        public long ConsumerLagStored; // Gauge

        [JsonProperty(PropertyName = "txmsgs")]
        public long TotalNumberOfMessagesProduced; // Gauge

        [JsonProperty(PropertyName = "txbytes")]
        public long TotalNumberOfBytesProduced; // Gauge

        [JsonProperty(PropertyName = "rxmsgs")]
        public long TotalNumberOfMessagesconsumed; // Gauge

        [JsonProperty(PropertyName = "rxbytes")]
        public long TotalNumberOfBytesConsumed; // Gauge

        [JsonProperty(PropertyName = "msgs")]
        public long TotalNumberOfMessages; // Gauge consumed or produced

        [JsonProperty(PropertyName = "rx_ver_drops")]
        public long DroppedOutdatedMessages;

        [JsonProperty(PropertyName = "msgs_inflight")]
        public long NumberOfMessagesInFlight; // Gauge

        [JsonProperty(PropertyName = "next_ack_seq")]
        public long NextExpectedAckSequence; // Gauge idempotent producer

        [JsonProperty(PropertyName = "next_err_seq")]
        public long NextExpectedErrorSequence; // Gauge idempotent producer

        [JsonProperty(PropertyName = "acked_msgid")]
        public long LastInternalMessageIdAcked; // Gauge idempotent producer
    }
}