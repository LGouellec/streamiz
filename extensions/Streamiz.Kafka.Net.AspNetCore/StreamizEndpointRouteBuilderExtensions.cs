using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Streamiz.Kafka.Net.Stream;

[assembly: InternalsVisibleTo("Streamiz.Kafka.Net.Tests, PublicKey=00240000048000009400000006020000002400005253413100040000010001000d9d4a8e90a3b987f68f047ec499e5a3405b46fcad30f52abadefca93b5ebce094d05976950b38cc7f0855f600047db0a351ede5e0b24b9d5f1de6c59ab55dee145da5d13bb86f7521b918c35c71ca5642fc46ba9b04d4900725a2d4813639ff47898e1b762ba4ccd5838e2dd1e1664bd72bf677d872c87749948b1174bd91ad")]

namespace Streamiz.Kafka.Net.AspNetCore
{
    /// <summary>
    /// ASP.NET Core endpoint extensions to expose the topology of a Streamiz application.
    /// Nothing is exposed unless one of these methods is explicitly called. The returned
    /// <see cref="IEndpointConventionBuilder"/> can be used to secure the endpoint
    /// (e.g: <c>app.MapStreamizTopology(stream).RequireAuthorization()</c>).
    /// <para>
    /// The endpoint answers <c>GET</c> requests with the topology as JSON (default),
    /// or as text (same output as <see cref="Topology.Describe"/>) with <c>?format=text</c>
    /// or an <c>Accept: text/plain</c> header.
    /// </para>
    /// </summary>
    public static class StreamizEndpointRouteBuilderExtensions
    {
        /// <summary>
        /// Default route of the topology endpoint.
        /// </summary>
        public const string DefaultTopologyPattern = "/topology";

        /// <summary>
        /// Map a <c>GET</c> endpoint returning the topology of the <see cref="KafkaStream"/> registered in the
        /// dependency injection container.
        /// </summary>
        /// <param name="endpoints">endpoint route builder</param>
        /// <param name="pattern">route pattern, default <see cref="DefaultTopologyPattern"/></param>
        /// <returns>a builder to customize the endpoint (authorization, ...)</returns>
        public static IEndpointConventionBuilder MapStreamizTopology(
            this IEndpointRouteBuilder endpoints,
            string pattern = DefaultTopologyPattern)
        {
            ArgumentNullException.ThrowIfNull(endpoints);
            return endpoints.MapGet(pattern, (RequestDelegate)(context =>
            {
                var stream = context.RequestServices.GetService<KafkaStream>();
                return TopologyEndpoint.HandleAsync(context, stream != null ? stream.DescribeTopology : null);
            }));
        }

        /// <summary>
        /// Map a <c>GET</c> endpoint returning the topology of <paramref name="stream"/>.
        /// Internal topics (repartition and changelog topics) are resolved.
        /// </summary>
        /// <param name="endpoints">endpoint route builder</param>
        /// <param name="stream">running kafka stream instance</param>
        /// <param name="pattern">route pattern, default <see cref="DefaultTopologyPattern"/></param>
        /// <returns>a builder to customize the endpoint (authorization, ...)</returns>
        public static IEndpointConventionBuilder MapStreamizTopology(
            this IEndpointRouteBuilder endpoints,
            KafkaStream stream,
            string pattern = DefaultTopologyPattern)
        {
            ArgumentNullException.ThrowIfNull(endpoints);
            ArgumentNullException.ThrowIfNull(stream);
            return endpoints.MapGet(pattern,
                (RequestDelegate)(context => TopologyEndpoint.HandleAsync(context, stream.DescribeTopology)));
        }

        /// <summary>
        /// Map a <c>GET</c> endpoint returning the description of <paramref name="topology"/>.
        /// Internal topics are not resolved, use the <see cref="KafkaStream"/> overload for that.
        /// </summary>
        /// <param name="endpoints">endpoint route builder</param>
        /// <param name="topology">topology to describe</param>
        /// <param name="pattern">route pattern, default <see cref="DefaultTopologyPattern"/></param>
        /// <returns>a builder to customize the endpoint (authorization, ...)</returns>
        public static IEndpointConventionBuilder MapStreamizTopology(
            this IEndpointRouteBuilder endpoints,
            Topology topology,
            string pattern = DefaultTopologyPattern)
        {
            ArgumentNullException.ThrowIfNull(endpoints);
            ArgumentNullException.ThrowIfNull(topology);
            return endpoints.MapGet(pattern,
                (RequestDelegate)(context => TopologyEndpoint.HandleAsync(context, () => topology.Describe().ToModel())));
        }
    }

    internal static class TopologyEndpoint
    {
        internal const string JsonContentType = "application/json; charset=utf-8";
        internal const string TextContentType = "text/plain; charset=utf-8";

        internal static async Task HandleAsync(HttpContext context, Func<TopologyModel> topologyProvider)
        {
            if (topologyProvider == null)
            {
                context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
                context.Response.ContentType = TextContentType;
                await context.Response.WriteAsync($"No {nameof(KafkaStream)} instance is registered.", context.RequestAborted);
                return;
            }

            bool? text = WantsText(context.Request);
            if (!text.HasValue)
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                context.Response.ContentType = TextContentType;
                await context.Response.WriteAsync("Unsupported format. Supported formats are 'json' and 'text'.", context.RequestAborted);
                return;
            }

            var model = topologyProvider();
            context.Response.StatusCode = StatusCodes.Status200OK;
            if (text.Value)
            {
                context.Response.ContentType = TextContentType;
                await context.Response.WriteAsync(model.ToString(), context.RequestAborted);
            }
            else
            {
                context.Response.ContentType = JsonContentType;
                await context.Response.WriteAsync(model.ToJson(), context.RequestAborted);
            }
        }

        // true => text, false => json, null => unsupported format
        private static bool? WantsText(HttpRequest request)
        {
            if (request.Query.TryGetValue("format", out var format) && !string.IsNullOrEmpty(format.ToString()))
            {
                var f = format.ToString();
                if (string.Equals(f, "text", StringComparison.OrdinalIgnoreCase))
                    return true;
                if (string.Equals(f, "json", StringComparison.OrdinalIgnoreCase))
                    return false;
                return null;
            }

            var accept = request.Headers.Accept.ToString();
            return accept.Contains("text/plain", StringComparison.OrdinalIgnoreCase) &&
                   !accept.Contains("application/json", StringComparison.OrdinalIgnoreCase);
        }
    }
}
