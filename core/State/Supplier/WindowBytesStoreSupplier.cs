using kafka_stream_core.Crosscutting;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.State.Supplier
{
    public interface WindowBytesStoreSupplier : StoreSupplier<WindowStore<Bytes, byte[]>>
    {
    }
}
