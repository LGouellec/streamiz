using kafka_stream_core.Processors;
using kafka_stream_core.Stream.Internal.Graph;

namespace kafka_stream_core.Stream
{
    public class Printed<K, V>
    {
        private Printed()
        {
        }

        internal IProcessorSupplier<K, V> build(string processorName) => new KStreamPrint<K, V>();

        public static Printed<K, V> toFile(string pathFile) => null;
        public static Printed<K, V> toOut() => null;
    }
}
