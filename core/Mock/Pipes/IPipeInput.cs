using Confluent.Kafka;
using System;

namespace Streamiz.Kafka.Net.Mock.Pipes
{
    internal delegate void PipeFlushed();
    
    internal interface IPipeInput : IDisposable
    {
        string TopicName { get; }
        event PipeFlushed Flushed;
        /// <summary>
        /// Pipe one record.
        /// </summary>
        /// <param name="key">serialized key</param>
        /// <param name="value">serialized value</param>
        /// <param name="timestamp">record timestamp</param>
        /// <param name="headers">record headers</param>
        /// <param name="partition">target partition, or a negative value to let the producer partitioner choose it</param>
        void Pipe(byte[] key, byte[] value, DateTime timestamp, Headers headers, int partition = -1);
        void Flush();
        
    }
}
