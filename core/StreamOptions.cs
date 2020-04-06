using kafka_stream_core.Processors;
using kafka_stream_core.Stream;

namespace kafka_stream_core
{
    public class StreamOptions
    {
        internal string Named { get; private set; } = null;
        internal ITimestampExtractor Extractor { get; private set; } = null;

        public static StreamOptions Create() => Create(null, null);

        public static StreamOptions Create(string named) => Create(null, named);

        public static StreamOptions Create(ITimestampExtractor extractor) => Create(extractor, null);

        public static StreamOptions Create(ITimestampExtractor extractor, string named) 
            => new StreamOptions() { Named = named, Extractor = extractor };

        public StreamOptions WithTimestrampExtractor(ITimestampExtractor extractor)
        {
            Extractor = extractor;
            return this;
        }

        public StreamOptions WithNamed(string named)
        {
            Named = named;
            return this;
        }
    }
}
