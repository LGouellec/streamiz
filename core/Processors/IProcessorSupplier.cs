using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Processors
{
    internal interface IProcessorSupplier<K,V>
    {
        IProcessor<K, V> Get();
    }
}
