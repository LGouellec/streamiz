using System;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors.Public;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamTransformer<K, V, K1, V1> : AbstractProcessor<K, V>
    {
        private readonly ITransformer<K, V, K1, V1> transformer;
        private readonly bool changeKey;

        public KStreamTransformer(
            TransformerSupplier<K,V,K1,V1> transformerSupplier,
            bool changeKey)
        {
            this.transformer = transformerSupplier.Transformer;
            this.changeKey = changeKey;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);
            transformer.Init(context);
        }

        public override void Process(K key, V value)
        {
            if (key == null && StateStores.Count > 0)
            {
                log.LogWarning($"Skipping record due to null key because your transformer is stateful. topic=[{Context.Topic}] partition=[{Context.Partition}] offset=[{Context.Offset}]");
                droppedRecordsSensor.Record();
                return;
            }
            
            Record<K, V> record = new Record<K, V>(
                new TopicPartitionOffset(Context.Topic, Context.Partition, Context.Offset),
                Context.RecordContext.Headers,
                new Timestamp(Context.RecordContext.Timestamp.FromMilliseconds()),
                key,
                value);
            
            var newRecord = transformer.Process(record);
            
            if (newRecord != null)
            {
                if (changeKey)
                    Forward(newRecord.Key, newRecord.Value);
                else
                    Forward(key, newRecord.Value);
            }
        }

        public override void Close()
        {
            transformer.Close();
        }
    }
}