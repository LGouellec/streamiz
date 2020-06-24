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

        /// <summary>
        /// Return an instance of <see cref="DateTime"/> using milliseconds from the epoch of 1970-01-01T00:00:00Z.
        /// </summary>
        /// <param name="epoch">the number of milliseconds from 1970-01-01T00:00:00Z</param>
        /// <returns>a datetime</returns>
        public static DateTime FromMilliseconds(this long epoch)
        {
            return Jan1St1970.AddMilliseconds(epoch);
        }
    }
}
