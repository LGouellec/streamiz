
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
}