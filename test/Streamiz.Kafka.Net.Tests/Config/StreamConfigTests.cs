using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NUnit.Framework;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Tests.StreamConfigTests
{
    public class StreamConfigTests
    {
        [Test]
        public void CreateStreamConfigThroughOptionsPattern()
        {
            var configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string>()
                {
                    { "Kafka:Client:ApplicationId", "app-settings-test" },
                    { "Kafka:Client:BootstrapServers", "localhost:9092" },
                    { "Kafka:Producer:LingerMs", "10" },
                    { "Kafka:Consumer:AutoOffsetReset", "Earliest" },
                })
               .Build();


            var services = new ServiceCollection();

            services
                .AddOptions<StreamConfig>()
                .Bind(configuration.GetSection("Kafka:Client"))
                .Bind(configuration.GetSection("Kafka:Producer"))
                .Bind(configuration.GetSection("Kafka:Consumer"));

            var provider = services.BuildServiceProvider();
            var options = provider.GetRequiredService<IOptions<StreamConfig>>();
            var streamConfig = options.Value;

            Assert.That(streamConfig.ApplicationId, Is.EqualTo("app-settings-test"));
            Assert.That(streamConfig.BootstrapServers, Is.EqualTo("localhost:9092"));
            Assert.That(streamConfig.LingerMs, Is.EqualTo(10));
            Assert.That(streamConfig.AutoOffsetReset, Is.EqualTo(AutoOffsetReset.Earliest));
        }
    }
}
