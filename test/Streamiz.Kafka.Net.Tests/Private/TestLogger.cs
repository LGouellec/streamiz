using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class TestLogger
    {
        [Test]
        public void test()
        {
            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.SetMinimumLevel(LogLevel.Debug);
                builder.AddConsole();
            });
            
            var logger = loggerFactory.CreateLogger<TestLogger>();
            logger.LogInformation("Coucou test");
            logger.LogDebug("Debug Coucou test");
        }
    }
}