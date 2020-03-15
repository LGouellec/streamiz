using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.State.Supplier
{
    public interface SessionBytesStoreSupplier : StoreSupplier<SessionStore<byte[], byte[]>>
    {
    }
}
