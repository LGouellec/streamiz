using System;
using ModelContextProtocol;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Mcp
{
    /// <summary>
    /// Provides the <see cref="TopologyModel"/> exposed by the <see cref="StreamizTopologyTools"/>.
    /// Registered by <see cref="Microsoft.Extensions.DependencyInjection.StreamizMcpServerBuilderExtensions"/>.
    /// </summary>
    public sealed class StreamizTopologyProvider
    {
        private readonly Func<IServiceProvider, TopologyModel> provider;

        /// <summary>
        /// Create a provider.
        /// </summary>
        /// <param name="provider">function returning the topology model, or null when no topology is available</param>
        public StreamizTopologyProvider(Func<IServiceProvider, TopologyModel> provider)
        {
            this.provider = Guard.NotNull(provider, nameof(provider));
        }

        /// <summary>
        /// Get the topology model.
        /// </summary>
        /// <param name="services">service provider of the current MCP request</param>
        /// <returns>the topology model</returns>
        /// <exception cref="McpException">when no topology is available</exception>
        public TopologyModel GetTopology(IServiceProvider services)
            => provider(services)
               ?? throw new McpException($"No {nameof(KafkaStream)} instance is registered in the application services.");
    }
}
