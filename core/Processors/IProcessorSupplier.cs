namespace Streamiz.Kafka.Net.Processors
{
    internal interface IProcessorSupplier<K, V>
    {
        IProcessor<K, V> Get();
    }
}
