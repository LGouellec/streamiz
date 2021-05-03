using System;

namespace Streamiz.Kafka.Net.Tests.Helpers
{
    internal static class StreamConfigExtension
    {
        internal static StreamConfig UseRandomRocksDbConfigForTest(this StreamConfig config)
        {
            Guid guid = Guid.NewGuid();
            config.ApplicationId = $"{config.ApplicationId}-{guid}";
            config.StateDir = $".";
            return config;
        }
    }
}
