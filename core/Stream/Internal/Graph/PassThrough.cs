using Streamiz.Kafka.Net.Processors;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph
{
    internal class PassThrough<K, V> : IProcessorSupplier<K, V>
    {
        public IProcessor<K, V> Get() => new PassThroughProcessor<K, V>();
    }
}
