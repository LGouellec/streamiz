using kafka_stream_core.Processors;
using kafka_stream_core.Stream;

namespace kafka_stream_core
{
    public class StreamOptions
    {
        internal ITimestampExtractor Extractor { get; private set; } = null;

        public static StreamOptions Create() => new StreamOptions();

        public static StreamOptions Create(ITimestampExtractor extractor) => new StreamOptions() { Extractor = extractor };

        public StreamOptions WithTimestrampExtractor(ITimestampExtractor extractor)
        {
            Extractor = extractor;
            return this;
        }
    }
}
