using System.Threading;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class SequentiallyGracefullyShutdownHook
    {
        private readonly IThread[] _streamThreads;
        private readonly GlobalStreamThread _globalStreamThread;
        private readonly IThread _externalStreamThread;
        private readonly CancellationTokenSource _tokenSource;
        private readonly ILogger log = Logger.GetLogger(typeof(SequentiallyGracefullyShutdownHook));

        public SequentiallyGracefullyShutdownHook(
            IThread [] streamThreads,
            GlobalStreamThread globalStreamThread,
            IThread externalStreamThread,
            CancellationTokenSource tokenSource
            )
        {
            _streamThreads = streamThreads;
            _globalStreamThread = globalStreamThread;
            _externalStreamThread = externalStreamThread;
            _tokenSource = tokenSource;
        }

        public void Shutdown()
        {
            log.LogInformation($"Request shutdown gracefully");
            _tokenSource.Cancel();
            
            foreach (var t in _streamThreads)
                t.Dispose();
            
            _externalStreamThread?.Dispose();
            _globalStreamThread?.Dispose();
            
            log.LogInformation($"Shutdown gracefully successful");
        }
    }
}