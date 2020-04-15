using Kafka.Streams.Net.Processors;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.State.Supplier
{
    public interface StoreSupplier<out T> 
        where T : IStateStore
    {
        string Name { get; }

        T Get();
    }
}
