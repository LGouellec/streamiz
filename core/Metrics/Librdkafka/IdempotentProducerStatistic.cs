using Newtonsoft.Json;

namespace Streamiz.Kafka.Net.Metrics.Librdkafka
{
    /// <summary>
    /// 
    /// </summary>
    public class IdempotentProducerStatistic
    {
        [JsonProperty(PropertyName = "idemp_state")]
        public string State;

        [JsonProperty(PropertyName = "idemp_age")]
        public long IdempotentStateAge; //  Gauge

        [JsonProperty(PropertyName = "txn_state")]
        public string CurrentTransactionalProducerState;

        [JsonProperty(PropertyName = "txn_stateage")]
        public long TimeElapsedSinceLastTransactionalProducerStateChange; // Gauge

        [JsonProperty(PropertyName = "txn_may_enq")]
        public bool TransactionalStateAllowEnqueuing;

        [JsonProperty(PropertyName = "producer_id")]
        public long ProducerId; // Gauge

        [JsonProperty(PropertyName = "producer_epoch")]
        public long ProducerEpoch; // Gauge

        [JsonProperty(PropertyName = "epoch_cnt")]
        public long NumberOfProducerIdAssignmentSinceStarts;
    }
}