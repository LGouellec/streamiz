namespace Streamiz.Kafka.Net.Processors.Public
{
    /// <summary>
    ///  A public processor of key-value pair records.
    /// </summary>
    /// <typeparam name="K">type of the key</typeparam>
    /// <typeparam name="V">type of the value</typeparam>
    public interface IProcessor<K, V>
    {
        /// <summary>
        /// Initialize this processor with the given context. The framework ensures this is called once per processor when the topology
        /// that contains it is initialized.
        /// <para>
        /// This context can be used to access topology, record meta data and to access attached state stores.
        /// </para>
        /// </summary>
        /// <param name="context">the context; may not be null</param>
        void Init(ProcessorContext<K, V> context);
        
        /// <summary>
        /// Process the record with the given key and value.
        /// </summary>
        /// <param name="record">Given record with key, value and all metadata</param>
        void Process(Record<K, V> record);
        
        /// <summary>
        /// Close this processor and clean up any resources.
        /// <para>
        /// Note: Do not close any streams managed resources, like state stores here, as they are managed by the library.
        /// </para>
        /// </summary>
        void Close();
    }
}