using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.State
{
    public interface SessionBytesStoreSupplier : StoreSupplier<SessionStore<byte[], byte[]>>
    {
    }
}
