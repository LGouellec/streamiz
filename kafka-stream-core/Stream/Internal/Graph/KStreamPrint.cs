using kafka_stream_core.Processors;

namespace kafka_stream_core.Stream.Internal.Graph
{
    internal class KStreamPrint<K, V> : IProcessorSupplier<K, V>
    {
        public IProcessor<K, V> Get() => new KStreamPrintProcessor<K, V>();
    }
}
