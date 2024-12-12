using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors.Public;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamTransformer<K, V, K1, V1> : KStreamPAPI<K, V>
    {
        private readonly ITransformer<K, V, K1, V1> transformer;
        private readonly bool changeKey;

        public KStreamTransformer(
            TransformerSupplier<K,V,K1,V1> transformerSupplier,
            bool changeKey)
        {
            transformer = transformerSupplier.Transformer;
            this.changeKey = changeKey;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);
            var wrappedContext = new ProcessorContext<K1,V1>(context);
            transformer.Init(wrappedContext);
        }

        public override void Process(Record<K, V> record)
        {
            var newRecord = transformer.Process(record);
            if (newRecord != null)
            {
                Context.SetHeaders(newRecord.Headers);
                
                if (changeKey)
                    Forward(newRecord.Key, newRecord.Value);
                else
                    Forward(record.Key, newRecord.Value);
            }
        }

        public override void Close()
        {
            transformer.Close();
        }
    }
}