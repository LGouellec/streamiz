using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using Streamiz.Kafka.Net.AspNetCore;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Tests.Public
{
    /// <summary>
    /// Tests of the <see cref="StreamizEndpointRouteBuilderExtensions.MapStreamizTopology(IEndpointRouteBuilder, string)"/> overloads.
    /// </summary>
    public class TopologyEndpointRoutingTests
    {
        private static Topology BuildTopology()
        {
            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic", new StringSerDes(), new StringSerDes(), "source")
                .To("topic-dest", "to");
            return builder.Build();
        }

        private static KafkaStream BuildStream()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-endpoint-routing",
                BootstrapServers = "127.0.0.1"
            };
            return new KafkaStream(BuildTopology(), config, new SyncKafkaSupplier());
        }

        private static WebApplication BuildApp(KafkaStream registered = null)
        {
            var builder = WebApplication.CreateBuilder();
            if (registered != null)
                builder.Services.AddSingleton(registered);
            return builder.Build();
        }

        private static RouteEndpoint SingleEndpoint(IEndpointRouteBuilder endpoints)
            => endpoints.DataSources.SelectMany(d => d.Endpoints).OfType<RouteEndpoint>().Single();

        private static async Task<(HttpContext context, string body)> InvokeAsync(WebApplication app, RouteEndpoint endpoint)
        {
            var context = new DefaultHttpContext
            {
                RequestServices = app.Services
            };
            context.Request.Method = "GET";
            context.Response.Body = new MemoryStream();

            await endpoint.RequestDelegate!(context);

            context.Response.Body.Position = 0;
            var body = await new StreamReader(context.Response.Body).ReadToEndAsync();
            return (context, body);
        }

        [Test]
        public async Task MapFromServicesUsesRegisteredKafkaStream()
        {
            using var stream = BuildStream();
            await using var app = BuildApp(stream);

            app.MapStreamizTopology();
            var endpoint = SingleEndpoint(app);
            var (context, body) = await InvokeAsync(app, endpoint);

            Assert.AreEqual("/topology", endpoint.RoutePattern.RawText);
            Assert.AreEqual(200, context.Response.StatusCode);
            Assert.AreEqual("topic-dest", JObject.Parse(body)["sinkTopics"][0].Value<string>());
        }

        [Test]
        public async Task MapFromServicesWithoutKafkaStreamReturns503()
        {
            await using var app = BuildApp();

            app.MapStreamizTopology("/custom/topology");
            var endpoint = SingleEndpoint(app);
            var (context, _) = await InvokeAsync(app, endpoint);

            Assert.AreEqual("/custom/topology", endpoint.RoutePattern.RawText);
            Assert.AreEqual(503, context.Response.StatusCode);
        }

        [Test]
        public async Task MapWithKafkaStream()
        {
            using var stream = BuildStream();
            await using var app = BuildApp();

            app.MapStreamizTopology(stream);
            var (context, body) = await InvokeAsync(app, SingleEndpoint(app));

            Assert.AreEqual(200, context.Response.StatusCode);
            Assert.AreEqual("source", JObject.Parse(body)["subTopologies"][0]["nodes"][0]["name"].Value<string>());
        }

        [Test]
        public async Task MapWithTopology()
        {
            await using var app = BuildApp();

            app.MapStreamizTopology(BuildTopology());
            var (context, body) = await InvokeAsync(app, SingleEndpoint(app));

            Assert.AreEqual(200, context.Response.StatusCode);
            Assert.AreEqual("topic", JObject.Parse(body)["sourceTopics"][0].Value<string>());
        }

        [Test]
        public async Task MapRejectsNullArguments()
        {
            await using var app = BuildApp();

            Assert.Throws<ArgumentNullException>(() => ((IEndpointRouteBuilder)null).MapStreamizTopology());
            Assert.Throws<ArgumentNullException>(() => ((IEndpointRouteBuilder)null).MapStreamizTopology(BuildTopology()));
            Assert.Throws<ArgumentNullException>(() => app.MapStreamizTopology((KafkaStream)null));
            Assert.Throws<ArgumentNullException>(() => app.MapStreamizTopology((Topology)null));
        }
    }
}
