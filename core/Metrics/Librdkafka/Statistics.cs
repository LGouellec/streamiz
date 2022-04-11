using System.Collections.Generic;
using Newtonsoft.Json;

namespace Streamiz.Kafka.Net.Metrics.Librdkafka
{
    /// <summary>
    /// JSON Model from : https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md
    /// </summary>
    internal class Statistics
    {
        [JsonProperty(PropertyName = "name")]
        public string Name;

        [JsonProperty(PropertyName = "client_id")]
        public string ClientId;

        [JsonProperty(PropertyName = "type")]
        public string Type;

        [JsonProperty(PropertyName = "ts")]
        public long InternalMonotonicClock;

        [JsonProperty(PropertyName = "time")]
        public long TimeInSeconds;

        [JsonProperty(PropertyName = "replyq")]
        public long NumberOfOpsWaitinInQueue; // Gauge

        [JsonProperty(PropertyName = "msg_cnt")]
        public long CurrentNumberOfMessagesInProducerQueues; // Gauge

        [JsonProperty(PropertyName = "msg_size")]
        public long CurrentSizeOfMessagesInProducerQueues; // Gauge

        [JsonProperty(PropertyName = "msg_max")]
        public long MaxMessagesAllowedOnProducerQueues;

        [JsonProperty(PropertyName = "msg_size_max")]
        public long MaxSizeOfMessagesAllowedOnProducerQueues;

        [JsonProperty(PropertyName = "tx")]
        public long TotalNumberOfRequestSentToKafka;

        [JsonProperty(PropertyName = "tx_bytes")]
        public long TotalNumberOfBytesTransmittedToKafka;

        [JsonProperty(PropertyName = "rx")]
        public long TotalNumberOfResponsesReceivedFromKafka;

        [JsonProperty(PropertyName = "rx_bytes")]
        public long TotalNumberOfBytesReceivedFromKafka;

        [JsonProperty(PropertyName = "txmsgs")]
        public long TotalNumberOfMessagesProduced;

        [JsonProperty(PropertyName = "txmsg_bytes")]
        public long TotalNumberOfMessageBytesProduced;

        [JsonProperty(PropertyName = "rxmsgs")]
        public long TotalNumberOfMessagesConsumed;

        [JsonProperty(PropertyName = "rxmsg_bytes")]
        public long TotalNumberOfMessageBytesConsumed;

        [JsonProperty(PropertyName = "simple_cnt")]
        public long InternalTrackingVsConsumerApiState; // Gauge

        [JsonProperty(PropertyName = "metadata_cache_cnt")]
        public long NumberOfTopicsInMetadataCache; // Gauge

        [JsonProperty(PropertyName = "brokers")]
        public Dictionary<string, BrokerStatistic> Brokers { get; set; }

        [JsonProperty(PropertyName = "topics")]
        public Dictionary<string, TopicStatistic> Topics { get; set; }

        [JsonProperty(PropertyName = "cgrp")]
        public ConsumerGroupStatistic ConsumerGroups { get; set; }

        [JsonProperty(PropertyName = "eos")]
        public IdempotentProducerStatistic IdempotentProducerState { get; set; }


        public Statistics()
        {
            Topics = new Dictionary<string, TopicStatistic>();
            Brokers = new Dictionary<string, BrokerStatistic>();
        }
    }
}