using System.Reflection;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.Metrics.Internal
{
    public class GeneralClientMetrics
    {
        private static readonly ILogger log = Logger.GetLogger(typeof(GeneralClientMetrics));
        private static readonly string VERSION = "version";
        private static readonly string APPLICATION_ID = "application-id";
        private static readonly string TOPOLOGY_DESCRIPTION = "topology-description";
        private static readonly string STATE = "state";
        private static readonly string STREAM_THREADS = "stream-threads";
        private static readonly string VERSION_FROM_ASSEMBLY;
        private static readonly string DEFAULT_VALUE = "unknown";

        private static readonly string VERSION_DESCRIPTION = "The version of the Kafka Streams client";
        private static readonly string APPLICATION_ID_DESCRIPTION = "The application ID of the Kafka Streams client";
        private static readonly string TOPOLOGY_DESCRIPTION_DESCRIPTION =
        "The description of the topology executed in the Kafka Streams client";
        private static readonly string STATE_DESCRIPTION = "The state of the Kafka Streams client";
        private static readonly string STREAM_THREADS_DESCRIPTION = "The number of stream threads that are running or participating in rebalance";

        static GeneralClientMetrics()
        {
            try
            {
                VERSION_FROM_ASSEMBLY = Assembly.GetExecutingAssembly().GetName().Version.ToString();
            }
            catch
            {
                VERSION_FROM_ASSEMBLY = DEFAULT_VALUE;
            }
        }
    }
}