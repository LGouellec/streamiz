using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Mock.Pipes
{
    interface IPipeOutput : IDisposable
    {
        KeyValuePair<byte[], byte[]> Read();
    }
}
