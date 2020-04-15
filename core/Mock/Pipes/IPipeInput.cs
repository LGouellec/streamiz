using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.Mock.Pipes
{
    internal interface IPipeInput : IDisposable
    {
        void Pipe(byte[] key, byte[] value, DateTime timestamp);
        void Flush();
    }
}
