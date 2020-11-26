using System;

namespace Streamiz.Kafka.Net.Errors
{
    /// <summary>
    /// Production exception throw when production message is in error and <see cref="IStreamConfig.ProductionExceptionHandler"/> return <see cref="ExceptionHandlerResponse.FAIL"/>.
    /// </summary>
    public class ProductionException : Exception
    {
        /// <summary>
        /// Production exception throw when production message is in error and <see cref="IStreamConfig.ProductionExceptionHandler"/> return <see cref="ExceptionHandlerResponse.FAIL"/>.
        /// </summary>
        /// <param name="message">Exception message</param>
        public ProductionException(string message)
            : base(message)
        {
        }
    }
}
