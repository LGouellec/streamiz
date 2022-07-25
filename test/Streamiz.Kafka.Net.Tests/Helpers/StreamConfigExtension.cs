using System;
using System.IO;

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

        internal static StreamConfig RemoveRocksDbFolderForTest(this StreamConfig config)
        {
            int i = 0, maxRetry = 10;
            try
            {
                while (i < maxRetry)
                {
                    Directory.Delete(Path.Combine(config.StateDir, config.ApplicationId), true);
                }
            }catch(System.IO.IOException e)
            {
                ++i;
            }
            return config;
        }
    }
}
