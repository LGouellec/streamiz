using System;
using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Errors
{
    internal class StreamProducerException : Exception
    {
        public ProduceException<byte[], byte[]> OriginalProduceException { get; set; }
        public ProductionExceptionHandlerResponse Response { get; set; }

        public StreamProducerException(ProduceException<byte[], byte[]> originalProduceException, ProductionExceptionHandlerResponse response)
        {
            OriginalProduceException = originalProduceException;
            Response = response;
        }
    }
}