using System.Threading;

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
        /// <param name="token">Token for propagates notification that the stream should be canceled.</param>
        void BeforeStart(IStreamConfig config, CancellationToken token);
        /// <summary>
        /// Middleware function called after starting streaming application
        /// </summary>
        /// <param name="config">Actual configuration</param>
        /// <param name="token">Token for propagates notification that the stream should be canceled.</param>
        void AfterStart(IStreamConfig config, CancellationToken token);
        /// <summary>
        /// Middleware function called before stopping streaming application
        /// </summary>
        /// <param name="config">Actual configuration</param>
        /// <param name="token">Token for propagates notification that the stream should be canceled.</param>
        void BeforeStop(IStreamConfig config, CancellationToken token);
        /// <summary>
        /// Middleware function called after stopping streaming application
        /// </summary>
        /// <param name="config">Actual configuration</param>
        /// <param name="token">Token for propagates notification that the stream should be canceled.</param>
        void AfterStop(IStreamConfig config, CancellationToken token);
    }
}