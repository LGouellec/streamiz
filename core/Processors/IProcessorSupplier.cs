using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Processors
{
    internal interface IProcessorSupplier<K,V>
    {
        IProcessor<K, V> Get();
    }
}
