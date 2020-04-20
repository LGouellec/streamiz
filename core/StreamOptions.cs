using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net
{
    /// <summary>
    /// NOT USED FOR MOMENT
    /// </summary>
    internal class StreamOptions
    {
        internal string Named { get; private set; } = null;
        internal ITimestampExtractor Extractor { get; private set; } = null;

        private static StreamOptions Create() => Create(null, null);

        private static StreamOptions Create(string named) => Create(null, named);

        private static StreamOptions Create(ITimestampExtractor extractor) => Create(extractor, null);

        private static StreamOptions Create(ITimestampExtractor extractor, string named) 
            => new StreamOptions() { Named = named, Extractor = extractor };

        private StreamOptions WithTimestrampExtractor(ITimestampExtractor extractor)
        {
            Extractor = extractor;
            return this;
        }

        private StreamOptions WithNamed(string named)
        {
            Named = named;
            return this;
        }
    }
}
