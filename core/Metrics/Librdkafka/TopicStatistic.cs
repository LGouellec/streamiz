using System.Collections.Generic;
using Newtonsoft.Json;

namespace Streamiz.Kafka.Net.Metrics.Librdkafka
{
    /// <summary>
    /// 
    /// </summary>
    public class TopicStatistic
    {
        [JsonProperty(PropertyName = "topic")]
        public string TopicName;

        [JsonProperty(PropertyName = "metadata_age")]
        public long MetadataAge; // Gauge

        [JsonProperty(PropertyName = "batchsize")]
        public WindowStatistic BatchSize; // in bytes

        [JsonProperty(PropertyName = "batchcnt")]
        public WindowStatistic BatchMessageCounts;

        [JsonProperty(PropertyName = "partitions")]
        public Dictionary<int, PartitionStatistic> Partitions;

        public TopicStatistic()
        {
            Partitions = new Dictionary<int, PartitionStatistic>();
        }
    }
}