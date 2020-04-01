using kafka_stream_core.Processors;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.State.Supplier
{
    public interface StoreSupplier<out T> 
        where T : IStateStore
    {
        string Name { get; }

        T Get();
    }
}
