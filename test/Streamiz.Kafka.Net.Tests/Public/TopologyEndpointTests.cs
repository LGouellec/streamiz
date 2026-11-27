using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using Streamiz.Kafka.Net.AspNetCore;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Tests.Public
{
    public class TopologyEndpointTests
    {
        private static TopologyModel BuildModel()
        {
            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic", new StringSerDes(), new StringSerDes(), "source")
                .To("topic-dest", "to");
            return builder.Build().Describe().ToModel();
        }

        private static async Task<(HttpContext context, string body)> CallAsync(
            string queryString = null,
            string accept = null,
            bool withProvider = true)
        {
            var context = new DefaultHttpContext();
            context.Request.Method = "GET";
            context.Request.Path = "/topology";
            if (queryString != null)
                context.Request.QueryString = new QueryString(queryString);
            if (accept != null)
                context.Request.Headers.Accept = accept;
            context.Response.Body = new MemoryStream();

            await TopologyEndpoint.HandleAsync(context, withProvider ? BuildModel : null);

            context.Response.Body.Position = 0;
            var body = await new StreamReader(context.Response.Body).ReadToEndAsync();
            return (context, body);
        }

        [Test]
        public async Task ReturnsJsonByDefault()
        {
            var (context, body) = await CallAsync();

            Assert.AreEqual(200, context.Response.StatusCode);
            StringAssert.StartsWith("application/json", context.Response.ContentType);
            var obj = JObject.Parse(body);
            Assert.AreEqual("source", obj["subTopologies"][0]["nodes"][0]["name"].Value<string>());
            Assert.AreEqual("topic-dest", obj["sinkTopics"][0].Value<string>());
        }

        [Test]
        public async Task ReturnsJsonWithFormatQuery()
        {
            var (context, body) = await CallAsync("?format=json", "text/plain");

            Assert.AreEqual(200, context.Response.StatusCode);
            StringAssert.StartsWith("application/json", context.Response.ContentType);
            Assert.DoesNotThrow(() => JObject.Parse(body));
        }

        [Test]
        public async Task ReturnsTextWithFormatQuery()
        {
            var (context, body) = await CallAsync("?format=text");

            Assert.AreEqual(200, context.Response.StatusCode);
            StringAssert.StartsWith("text/plain", context.Response.ContentType);
            Assert.AreEqual(BuildModel().ToString(), body);
            StringAssert.StartsWith("Topologies:", body);
        }

        [Test]
        public async Task ReturnsTextWithAcceptHeader()
        {
            var (context, body) = await CallAsync(accept: "text/plain");

            Assert.AreEqual(200, context.Response.StatusCode);
            StringAssert.StartsWith("text/plain", context.Response.ContentType);
            StringAssert.StartsWith("Topologies:", body);
        }

        [Test]
        public async Task UnsupportedFormatReturnsBadRequest()
        {
            var (context, _) = await CallAsync("?format=xml");
            Assert.AreEqual(400, context.Response.StatusCode);
        }

        [Test]
        public async Task NoStreamReturnsServiceUnavailable()
        {
            var (context, _) = await CallAsync(withProvider: false);
            Assert.AreEqual(503, context.Response.StatusCode);
        }
    }
}
