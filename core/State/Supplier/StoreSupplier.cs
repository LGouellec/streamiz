using Streamiz.Kafka.Net.Processors;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.State.Supplier
{
    public interface StoreSupplier<out T> 
        where T : IStateStore
    {
        string Name { get; }

        T Get();
    }
}
