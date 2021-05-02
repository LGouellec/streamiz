using System;

namespace Streamiz.Kafka.Net.Tests.Helpers
{
    internal static class StreamConfigExtension
    {
        internal static StreamConfig UseRandomRocksDbConfigForTest(this StreamConfig config)
        {
            Random rd = new Random();
            Guid guid = Guid.NewGuid();
            config.ApplicationId = $"{config.ApplicationId}-{guid}";
            config.StateDir = $"./{rd.Next(0, int.MaxValue)}/";
            return config;
        }
    }
}
