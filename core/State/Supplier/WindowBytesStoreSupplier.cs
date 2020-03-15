using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.State.Supplier
{
    public interface WindowBytesStoreSupplier : StoreSupplier<WindowStore<byte[], byte[]>>
    {
    }
}
