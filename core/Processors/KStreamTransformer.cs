using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors.Public;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamTransformer<K, V, K1, V1> : AbstractProcessor<K, V>
    {
        private readonly TransformerSupplier<K, V, K1, V1> transformerSupplier;
        private readonly bool changeKey;

        public KStreamTransformer(
            TransformerSupplier<K,V,K1,V1> transformerSupplier,
            bool changeKey)
        {
            this.transformerSupplier = transformerSupplier;
            this.changeKey = changeKey;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);
            transformerSupplier.Transformer.Init(context);
        }

        public override void Process(K key, V value)
        {
            Record<K, V> record = new Record<K, V>(
                new TopicPartitionOffset(Context.Topic, Context.Partition, Context.Offset),
                Context.RecordContext.Headers,
                new Timestamp(Context.RecordContext.Timestamp.FromMilliseconds()),
                key,
                value);
            
            var newRecord = transformerSupplier.Transformer.Process(record);
            
            if (changeKey)
                Forward(newRecord.Key, newRecord.Value);
            else
                Forward(key, newRecord.Value);
        }

        public override void Close()
        {
            transformerSupplier.Transformer.Close();
        }
    }
}