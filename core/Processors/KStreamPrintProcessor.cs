using Streamiz.Kafka.Net.Stream.Internal.Graph;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamPrintProcessor<K, V> : AbstractProcessor<K, V>
    {
        private readonly PrintForeachAction<K, V> actionPrint;

        public KStreamPrintProcessor(PrintForeachAction<K, V> actionPrint)
        {
            this.actionPrint = actionPrint;
        }

        public override void Process(K key, V value)
        {
            LogProcessingKeyValue(key, value);
            actionPrint.Apply(key, value);
        }

        public override void Close()
        {
            base.Close();
            actionPrint.Close();
        }
    }
}
