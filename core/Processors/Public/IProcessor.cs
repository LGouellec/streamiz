namespace Streamiz.Kafka.Net.Processors.Public
{
    public interface IProcessor<K, V>
    {
        void Init(ProcessorContext context);
        void Process(Record<K, V> record);
        void Close();
    }
}