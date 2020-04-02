using kafka_stream_core.Processors;
using kafka_stream_core.Stream;

namespace kafka_stream_core
{
    public class StreamOptions
    {
        internal ITimestampExtractor Extractor { get; private set; } = null;
        internal Topology.AutoOffsetReset AutoOffsetReset { get; private set; } = Topology.AutoOffsetReset.EARLIEST;

        public static StreamOptions Create() => new StreamOptions();

        public static StreamOptions Create(ITimestampExtractor extractor) => new StreamOptions() { Extractor = extractor };

        public static StreamOptions Create(Topology.AutoOffsetReset autoOffsetReset) => new StreamOptions() { AutoOffsetReset = autoOffsetReset };

        public static StreamOptions Create(ITimestampExtractor extractor, Topology.AutoOffsetReset autoOffsetReset)
            => new StreamOptions() { Extractor = extractor, AutoOffsetReset = autoOffsetReset };

        public StreamOptions WithTimestrampExtractor(ITimestampExtractor extractor)
        {
            Extractor = extractor;
            return this;
        }

        public StreamOptions WithAutoOffsetReset(Topology.AutoOffsetReset autoOffsetReset)
        {
            AutoOffsetReset = autoOffsetReset;
            return this;
        }
    }
}
