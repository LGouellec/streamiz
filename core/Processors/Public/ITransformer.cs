using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors.Public
{
    public interface ITransformer<K, V, K1, V1>
    {
        void Init(ProcessorContext context);
        KeyValuePair<K1, V1> Process(Record<K, V> record);
        void Close();
    }
}