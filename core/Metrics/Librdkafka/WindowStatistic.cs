using Newtonsoft.Json;

namespace Streamiz.Kafka.Net.Metrics.Librdkafka
{
    /// <summary>
    /// Rolling window statistics. The values are in microseconds unless otherwise stated.
    /// </summary>
    internal class WindowStatistic
    {
        [JsonProperty(PropertyName = "min")]
        public long Min;

        [JsonProperty(PropertyName = "max")]
        public long Max;

        [JsonProperty(PropertyName = "avg")]
        public long Average;

        [JsonProperty(PropertyName = "sum")]
        public long Sum;

        [JsonProperty(PropertyName = "cnt")]
        public long NumberOfValuesSampled;

        [JsonProperty(PropertyName = "stddev")]
        public long StandardDeviation;

        [JsonProperty(PropertyName = "hdrsize")]
        public long MemorySizeOfHdrHistogram;

        [JsonProperty(PropertyName = "p50")]
        public long Percentil50;

        [JsonProperty(PropertyName = "p75")]
        public long Percentil75;

        [JsonProperty(PropertyName = "p90")]
        public long Percentil90;

        [JsonProperty(PropertyName = "p95")]
        public long Percentil95;

        [JsonProperty(PropertyName = "p99")]
        public long Percentil99;

        [JsonProperty(PropertyName = "p99_99")]
        public long Percentil99_99;

        [JsonProperty(PropertyName = "outofrange")]
        public long ValuesOutOfHistogramRanges;

    }
}