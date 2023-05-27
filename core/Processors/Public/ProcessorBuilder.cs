using System;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Processors.Public
{
    /// <summary>
    /// Create a new <see cref="ProcessorBuilder{K,V}"/> instance
    /// </summary>
    public class ProcessorBuilder
    {
        /// <summary>
        /// Helper create method
        /// </summary>
        /// <typeparam name="K">type of the key</typeparam>
        /// <typeparam name="V">type of the value</typeparam>
        /// <returns>return a new <see cref="ProcessorBuilder{K,V}"/> instance</returns>
        public static ProcessorBuilder<K, V> New<K, V>()
            => new();
    }
    
    /// <summary>
    /// Builder's <see cref="ProcessorSupplier{K,V}"/>
    /// </summary>
    /// <typeparam name="K">type of the key</typeparam>
    /// <typeparam name="V">type of the value</typeparam>
    public class ProcessorBuilder<K, V>
    {
        private ProcessorSupplier<K, V> processorSupplier = new();
        
        /// <summary>
        /// Set the processor
        /// </summary>
        /// <typeparam name="Pr">Processor type class</typeparam>
        /// <returns></returns>
        public ProcessorBuilder<K, V> Processor<Pr>(params object[] parameters)
            where Pr : IProcessor<K, V>, new()
        {
            processorSupplier.Processor = new Pr();
            processorSupplier.ProcessorParameters = parameters;
            return this;
        }
        
        /// <summary>
        /// Set the processor
        /// </summary>
        /// <param name="processor"></param>
        /// <returns></returns>
        public ProcessorBuilder<K, V> Processor(Action<Record<K, V>> processor)
        {
            processorSupplier.Processor = new WrappedProcessor<K, V>(processor);
            return this;
        }

        /// <summary>
        /// Set the state store
        /// </summary>
        /// <param name="storeBuilder"></param>
        /// <returns></returns>
        public ProcessorBuilder<K, V> StateStore(IStoreBuilder storeBuilder)
        {
            processorSupplier.StoreBuilder = storeBuilder;
            return this;
        }
        
        /// <summary>
        /// Build this <see cref="ProcessorSupplier{K,V}"/>
        /// </summary>
        /// <returns></returns>
        public ProcessorSupplier<K, V> Build()
        {
            ProcessorSupplier<K, V> processor = processorSupplier;
            processorSupplier = new();
            return processor;
        }
    }
}