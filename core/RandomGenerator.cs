using System;
using System.Linq;
using System.Security.Cryptography;

namespace Streamiz.Kafka.Net
{
    /// <summary>
    /// Helper random generator
    /// </summary>
    public static class RandomGenerator
    {
        /// <summary>
        /// Generates a random integer between 0 and a specified exclusive upper bound using a cryptographically strong random number generator.
        /// </summary>
        public static int GetInt32(int partitionCount)
        {
#if NETSTANDARD2_0
            byte[] rngBytes = new byte[4];
            RandomNumberGenerator.Create().GetBytes(rngBytes);
            return BitConverter.ToInt32(rngBytes, 0);
#else
            // Use this when possible as it is more memory efficient
            return RandomNumberGenerator.GetInt32(0, partitionCount);
#endif
        }

        public static string GetRandomString(int maxLength = 100)
        {
            var rdLength = GetInt32(maxLength);
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, rdLength)
                .Select(s => s[GetInt32(s.Length)]).ToArray());
        }
    }
}
