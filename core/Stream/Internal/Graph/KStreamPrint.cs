using kafka_stream_core.Processors;
using System.IO;

namespace kafka_stream_core.Stream.Internal.Graph
{
    internal class PrintForeachAction<K, V>
    {
        private readonly TextWriter writer;
        private readonly IKeyValueMapper<K, V, string> mapper;
        private readonly string label;

        public PrintForeachAction(TextWriter writer, IKeyValueMapper<K, V, string> mapper, string label)
        {
            this.writer = writer;
            this.mapper = mapper;
            this.label = label;
        }

        public void Apply(K key, V value)
        {
            string data = $"[{label}]: {mapper.Apply(key, value)}";
            writer.WriteLine(data);
            writer.Flush();
        }

        public void Close()
        {
            writer.Flush();
            writer.Close();
        }
    }

    internal class KStreamPrint<K, V> : IProcessorSupplier<K, V>
    {
        private readonly PrintForeachAction<K, V> actionPrint;

        public KStreamPrint(PrintForeachAction<K, V> actionPrint)
        {
            this.actionPrint = actionPrint;
        }

        public IProcessor<K, V> Get() => new KStreamPrintProcessor<K, V>(actionPrint);
    }
}
