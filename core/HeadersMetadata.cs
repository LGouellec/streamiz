using Confluent.Kafka;
using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net
{
    public static class HeadersMetadata
    {
        public static Headers GetCurrentMetadata()
            => StreamTask.CurrentHeaders;
    }
}
