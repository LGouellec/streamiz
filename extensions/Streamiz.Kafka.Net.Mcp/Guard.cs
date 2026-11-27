using System;

namespace Streamiz.Kafka.Net.Mcp
{
    internal static class Guard
    {
        internal static T NotNull<T>(T value, string paramName) where T : class
        {
#if NET8_0_OR_GREATER
            ArgumentNullException.ThrowIfNull(value, paramName);
            return value;
#else
            return value ?? throw new ArgumentNullException(paramName);
#endif
        }
    }
}
