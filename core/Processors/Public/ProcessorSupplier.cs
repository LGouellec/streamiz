using System;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Processors.Public
{
    /// <summary>
    /// Materialize the processor parameters for <see cref="IKStream{K,V}.Process"/>
    /// </summary>
    /// <typeparam name="K">type of the key</typeparam>
    /// <typeparam name="V">type of the value</typeparam>
    public class ProcessorSupplier<K, V>
    {
        private IProcessor<K, V> innerProcessor;

        /// <summary>
        /// Get a copy of your processor
        /// </summary>
        public IProcessor<K, V> Processor
        {
            get
            {
                if (innerProcessor == null)
                    return null;
                if (innerProcessor is ICloneableProcessor cloneableProcessor)
                    return (IProcessor<K, V>)cloneableProcessor.Clone();
                return (IProcessor<K, V>)Activator.CreateInstance(innerProcessor.GetType(), ProcessorParameters);
            }
            internal set => innerProcessor = value;
        }
        
        /// <summary>
        /// Current state store builder (may be null)
        /// </summary>
        public IStoreBuilder StoreBuilder { get; internal set; }

        /// <summary>
        /// Processor parameters
        /// </summary>
        public object[] ProcessorParameters { get; internal set; }
    }
}