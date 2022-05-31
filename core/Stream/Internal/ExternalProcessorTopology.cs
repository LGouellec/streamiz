using System;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes.Internal;

namespace Streamiz.Kafka.Net.Stream.Internal
{
    internal class ExternalProcessorTopology
    {
        public IAsyncProcessor AsyncProcessor { get; }
        public RetryPolicy RetryPolicy { get; }
        public string SinkTopic { get; }

        public ExternalProcessorTopology(
            IAsyncProcessor asyncProcessor,
            RetryPolicy retryPolicy,
            String sinkTopic)
        {
            AsyncProcessor = asyncProcessor;
            RetryPolicy = retryPolicy;
            SinkTopic = sinkTopic;
        }

        public void Process(byte[] key, byte[] value, Headers header, long timestamp)
        {
            
        }
    }
}