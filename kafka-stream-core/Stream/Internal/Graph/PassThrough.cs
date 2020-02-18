using kafka_stream_core.Processors;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Stream.Internal.Graph
{
    public class PassThrough<K, V> : IProcessorSupplier<K, V>
    {
        public IProcessor<K, V> Get() => new PassThroughProcessor<K, V>();
    }
}
