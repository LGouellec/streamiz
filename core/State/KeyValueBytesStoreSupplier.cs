using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.State
{
    public interface KeyValueBytesStoreSupplier : StoreSupplier<KeyValueStore<byte[], byte[]>>
    {
    }
}
