using Confluent.Kafka;
using System;

namespace Streamiz.Kafka.Net.Mock.Pipes
{
    internal delegate void PipeFlushed();
    
    internal interface IPipeInput : IDisposable
    {
        string TopicName { get; }
        event PipeFlushed Flushed;
        void Pipe(byte[] key, byte[] value, DateTime timestamp, Headers headers);
        void Flush();
        
    }
}
