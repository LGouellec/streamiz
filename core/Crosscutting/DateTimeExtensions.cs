using System;

namespace Streamiz.Kafka.Net.Crosscutting
{
    /// <summary>
    /// Datetime extensions
    /// </summary>
    public static class DateTimeExtensions
    {
        private static readonly DateTime Jan1St1970 = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// return the total number of milliseconds since the first day of the year 1970
        /// </summary>
        /// <param name="dt"></param>
        /// <returns></returns>
        public static long GetMilliseconds(this DateTime dt)
        {
            return (long)((dt.ToUniversalTime() - Jan1St1970).TotalMilliseconds);
        }
    }
}
