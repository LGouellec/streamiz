using Streamiz.Kafka.Net.Crosscutting;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.State.Supplier
{
    public interface SessionBytesStoreSupplier : StoreSupplier<SessionStore<Bytes, byte[]>>
    {
    }
}
