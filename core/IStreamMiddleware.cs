namespace Streamiz.Kafka.Net
{
    /// <summary>
    /// Streamiz middleware 
    /// </summary>
    public interface IStreamMiddleware
    {
        /// <summary>
        /// Middleware function called before starting streaming application
        /// </summary>
        /// <param name="config">Actual configuration</param>
        void BeforeStart(IStreamConfig config);
        /// <summary>
        /// Middleware function called after starting streaming application
        /// </summary>
        /// <param name="config">Actual configuration</param>
        void AfterStart(IStreamConfig config);
        /// <summary>
        /// Middleware function called before stopping streaming application
        /// </summary>
        /// <param name="config">Actual configuration</param>
        void BeforeStop(IStreamConfig config);
        /// <summary>
        /// Middleware function called after stopping streaming application
        /// </summary>
        /// <param name="config">Actual configuration</param>
        void AfterStop(IStreamConfig config);
    }
}