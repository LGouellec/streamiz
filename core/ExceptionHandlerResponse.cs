
namespace Streamiz.Kafka.Net
{
    /// <summary>
    /// Enumeration that describes the response from the exception handler.
    /// </summary>
    public enum ExceptionHandlerResponse
    {
        /// <summary>
        /// Fail processing and stop it !
        /// </summary>
        FAIL = 0,
        /// <summary>
        /// Continue processing !
        /// </summary>
        CONTINUE = 1
    }

    /// <summary>
    /// Enumeration that describes the response from the production exception handler.
    /// </summary>
    public enum ProductionExceptionHandlerResponse
    {
        /// <summary>
        /// Fail processing and stop it !
        /// </summary>
        FAIL = 0,
        /// <summary>
        /// Continue processing, so skipping this message
        /// </summary>
        CONTINUE = 1,
        /// <summary>
        /// Retrying to send
        /// </summary>
        RETRY = 2
    }
}