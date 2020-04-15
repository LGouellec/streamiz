using Kafka.Streams.Net.Crosscutting;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.State.Supplier
{
    public interface SessionBytesStoreSupplier : StoreSupplier<SessionStore<Bytes, byte[]>>
    {
    }
}
