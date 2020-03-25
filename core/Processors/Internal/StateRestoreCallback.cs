using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Processors.Internal
{
    public delegate void StateRestoreCallback(byte[] key, byte[] value);
}
