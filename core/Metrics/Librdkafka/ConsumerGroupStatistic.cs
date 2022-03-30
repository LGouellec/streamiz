using Newtonsoft.Json;

namespace Streamiz.Kafka.Net.Metrics.Librdkafka
{
    /// <summary>
    /// 
    /// </summary>
    public class ConsumerGroupStatistic
    {
        [JsonProperty(PropertyName = "state")]
        public string State;

        [JsonProperty(PropertyName = "stateage")]
        public long StateAge; // Gauge

        [JsonProperty(PropertyName = "joinstate")]
        public string LocalConsumerGroupHandlerJoinState;

        [JsonProperty(PropertyName = "rebalance_age")]
        public long RebalanceAge; // Gauge

        [JsonProperty(PropertyName = "rebalance_cnt")]
        public long TotalNumberOfRelabalance; // Gauge assign or revoke

        [JsonProperty(PropertyName = "rebalance_reason")]
        public string RebalanceReason;

        [JsonProperty(PropertyName = "assignment_size")]
        public long CurrentAssignmentPartitionCount; // Gauge
    }
}