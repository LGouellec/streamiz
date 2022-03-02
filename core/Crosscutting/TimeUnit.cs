using System;

namespace Streamiz.Kafka.Net.Crosscutting
{

    public static class TimeUnits
    {
        public static TimeUnit NANOSECONDS { get; } = new TimeUnit(TimeSpan.FromMilliseconds(1) * 1000 * 1000, (timeMs) => timeMs * 1000.0 * 1000.0);
        public static TimeUnit MICROSECONDS { get; } = new TimeUnit(TimeSpan.FromMilliseconds(1) * 1000, (timeMs) => timeMs * 1000.0);
        public static TimeUnit MILLISECONDS { get; } = new TimeUnit(TimeSpan.FromMilliseconds(1), (timeMs) => timeMs);
        public static TimeUnit SECONDS { get; } = new TimeUnit(TimeSpan.FromSeconds(1), (timeMs) => timeMs / 1000.0);
        public static TimeUnit MINUTES { get; } = new TimeUnit(TimeSpan.FromMinutes(1), (timeMs) => timeMs / (60.0 * 1000.0));
        public static TimeUnit HOURS { get; } = new TimeUnit(TimeSpan.FromHours(1), (timeMs) => timeMs / (60.0 * 60.0 * 1000.0));
        public static TimeUnit DAYS { get; } = new TimeUnit(TimeSpan.FromDays(1), (timeMs) => timeMs / (24.0 * 60.0 * 60.0 * 1000.0));
    }
    
    public class TimeUnit
    {
        private readonly Func<long, double> convert;

        public TimeUnit(TimeSpan value, Func<long, double> convert)
        {
            this.convert = convert;
            Value = value;
        }
    
        public TimeSpan Value { get; private set; }

        public double Convert(long timeMs) => convert(timeMs);
    
        public static TimeSpan operator *(int amount, TimeUnit unit)
        {
            return new TimeSpan(amount * unit.Value.Ticks);
        }
    
        public static TimeSpan operator *(double amount, TimeUnit unit)
        {
            return TimeSpan.FromMilliseconds(amount * unit.Value.TotalMilliseconds);
        }
    }
}