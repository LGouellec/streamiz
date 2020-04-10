using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Mock.Pipes
{
    internal interface IPipeInput : IDisposable
    {
        void Pipe(byte[] key, byte[] value);
    }
}
