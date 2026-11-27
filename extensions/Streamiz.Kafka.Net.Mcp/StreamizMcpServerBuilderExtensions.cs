using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mcp;
using Streamiz.Kafka.Net.Stream;

namespace Microsoft.Extensions.DependencyInjection
{
    /// <summary>
    /// Extensions to register the Streamiz topology tools (<see cref="StreamizTopologyTools"/>) on an MCP server.
    /// <example>
    /// <code>
    /// builder.Services
    ///     .AddMcpServer()
    ///     .WithStdioServerTransport() // or .WithHttpTransport() + app.MapMcp() with ModelContextProtocol.AspNetCore
    ///     .WithStreamizTopologyTools();
    /// </code>
    /// </example>
    /// The tools are read-only: they only describe the topology, they never change the application state.
    /// </summary>
    public static class StreamizMcpServerBuilderExtensions
    {
        /// <summary>
        /// Register the Streamiz topology tools, exposing the topology of the <see cref="KafkaStream"/>
        /// registered in the application services. Internal topics (repartition, changelog) are resolved.
        /// A tool call returns an error when no <see cref="KafkaStream"/> is registered.
        /// </summary>
        /// <param name="builder">MCP server builder</param>
        /// <returns>the same builder, for chaining</returns>
        public static IMcpServerBuilder WithStreamizTopologyTools(this IMcpServerBuilder builder)
            => builder.WithStreamizTopologyTools(new StreamizTopologyProvider(
                sp => sp?.GetService<KafkaStream>()?.DescribeTopology()));

        /// <summary>
        /// Register the Streamiz topology tools, exposing the topology of <paramref name="stream"/>.
        /// Internal topics (repartition, changelog) are resolved.
        /// </summary>
        /// <param name="builder">MCP server builder</param>
        /// <param name="stream">running Streamiz application</param>
        /// <returns>the same builder, for chaining</returns>
        public static IMcpServerBuilder WithStreamizTopologyTools(this IMcpServerBuilder builder, KafkaStream stream)
        {
            Guard.NotNull(stream, nameof(stream));
            return builder.WithStreamizTopologyTools(new StreamizTopologyProvider(_ => stream.DescribeTopology()));
        }

        /// <summary>
        /// Register the Streamiz topology tools, exposing <paramref name="topology"/>.
        /// Internal topics are not resolved, as they depend on the application id.
        /// </summary>
        /// <param name="builder">MCP server builder</param>
        /// <param name="topology">topology to expose</param>
        /// <returns>the same builder, for chaining</returns>
        public static IMcpServerBuilder WithStreamizTopologyTools(this IMcpServerBuilder builder, Topology topology)
        {
            Guard.NotNull(topology, nameof(topology));
            return builder.WithStreamizTopologyTools(new StreamizTopologyProvider(_ => topology.Describe().ToModel()));
        }

        /// <summary>
        /// Register the Streamiz topology tools with a custom <see cref="StreamizTopologyProvider"/>.
        /// </summary>
        /// <param name="builder">MCP server builder</param>
        /// <param name="provider">topology provider</param>
        /// <returns>the same builder, for chaining</returns>
        public static IMcpServerBuilder WithStreamizTopologyTools(this IMcpServerBuilder builder, StreamizTopologyProvider provider)
        {
            Guard.NotNull(builder, nameof(builder));
            Guard.NotNull(provider, nameof(provider));

            builder.Services.AddSingleton(provider);
            return builder.WithTools<StreamizTopologyTools>();
        }
    }
}
