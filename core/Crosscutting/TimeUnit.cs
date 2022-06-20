using System;

namespace Streamiz.Kafka.Net.Crosscutting
{

    /// <summary>
    /// Static helper class to build <see cref="TimeUnit"/>
    /// </summary>
    public static class TimeUnits
    {
        /// <summary>
        /// Nano-second timeunit
        /// </summary>
        public static TimeUnit NANOSECONDS { get; } = new TimeUnit(TimeSpan.FromMilliseconds(1 * 1000 * 1000), (timeMs) => timeMs * 1000.0 * 1000.0);
        /// <summary>
        /// Micro-second timeunit
        /// </summary>
        public static TimeUnit MICROSECONDS { get; } = new TimeUnit(TimeSpan.FromMilliseconds(1 * 1000), (timeMs) => timeMs * 1000.0);
        /// <summary>
        /// Millisecond timeunit
        /// </summary>
        public static TimeUnit MILLISECONDS { get; } = new TimeUnit(TimeSpan.FromMilliseconds(1), (timeMs) => timeMs);
        /// <summary>
        /// Second timeunit
        /// </summary>
        public static TimeUnit SECONDS { get; } = new TimeUnit(TimeSpan.FromSeconds(1), (timeMs) => timeMs / 1000.0);
        /// <summary>
        /// Minute timeunit
        /// </summary>
        public static TimeUnit MINUTES { get; } = new TimeUnit(TimeSpan.FromMinutes(1), (timeMs) => timeMs / (60.0 * 1000.0));
        /// <summary>
        /// Hour timeunit
        /// </summary>
        public static TimeUnit HOURS { get; } = new TimeUnit(TimeSpan.FromHours(1), (timeMs) => timeMs / (60.0 * 60.0 * 1000.0));
        /// <summary>
        /// Day timeunit
        /// </summary>
        public static TimeUnit DAYS { get; } = new TimeUnit(TimeSpan.FromDays(1), (timeMs) => timeMs / (24.0 * 60.0 * 60.0 * 1000.0));
    }
    
    /// <summary>
    /// Timeunit representation
    /// </summary>
    public class TimeUnit
    {
        private readonly Func<long, double> convert;

        /// <summary>
        /// Constructor with interval time and converter function
        /// </summary>
        /// <param name="value"></param>
        /// <param name="convert"></param>
        public TimeUnit(TimeSpan value, Func<long, double> convert)
        {
            this.convert = convert;
            Value = value;
        }
    
        /// <summary>
        /// Value of timeunit
        /// </summary>
        public TimeSpan Value { get; private set; }
        
        /// <summary>
        /// Convert timeMs to milliseconds
        /// </summary>
        /// <param name="time">Time to convert in ms</param>
        /// <returns>return time to milliseconds</returns>
        public double Convert(long time) => convert(time);
    
        /// <summary>
        /// Operator *
        /// </summary>
        /// <param name="amount"></param>
        /// <param name="unit"></param>
        /// <returns></returns>
        public static TimeSpan operator *(int amount, TimeUnit unit)
        {
            return new TimeSpan(amount * unit.Value.Ticks);
        }
    
        /// <summary>
        /// Operator *
        /// </summary>
        /// <param name="amount"></param>
        /// <param name="unit"></param>
        /// <returns></returns>
        public static TimeSpan operator *(double amount, TimeUnit unit)
        {
            return TimeSpan.FromMilliseconds(amount * unit.Value.TotalMilliseconds);
        }
    }
}