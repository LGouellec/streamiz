using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Mock.Pipes
{
    internal interface IPipeInput : IDisposable
    {
        void Pipe(byte[] key, byte[] value, DateTime timestamp);
        void Flush();
    }
}
