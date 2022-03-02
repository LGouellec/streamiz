using System;
using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.Metrics.Stats
{
    internal class Rate : IMeasurableStat
    {
        private readonly TimeUnit timeUnit;
        private readonly SampledStat internalStat;

        public Rate() : this(TimeUnits.SECONDS)
        { }
        public Rate(TimeUnit timeUnit) : this(timeUnit, new WindowedSum())
        { }

        public Rate(SampledStat sampledStat) : this(TimeUnits.SECONDS, sampledStat)
        { }
        public Rate(TimeUnit timeUnit, SampledStat sampledStat)
        {
            this.timeUnit = timeUnit;
            internalStat = sampledStat;
        }

        public void Record(MetricConfig config, double value, long timeMs)
            => internalStat.Record(config, value, timeMs);

        public double Measure(MetricConfig config, long now)
        {
            double value = internalStat.Measure(config, now);
            return value / timeUnit.Convert(WindowSize(config, now));
        }
        
        public long WindowSize(MetricConfig config, long now) {
            // purge old samples before we compute the window size
            internalStat.PurgeObsoleteSamples(config, now);

            /*
             * Here we check the total amount of time elapsed since the oldest non-obsolete window.
             * This give the total windowSize of the batch which is the time used for Rate computation.
             * However, there is an issue if we do not have sufficient data for e.g. if only 1 second has elapsed in a 30 second
             * window, the measured rate will be very high.
             * Hence we assume that the elapsed time is always N-1 complete windows plus whatever fraction of the final window is complete.
             *
             * Note that we could simply count the amount of time elapsed in the current window and add n-1 windows to get the total time,
             * but this approach does not account for sleeps. SampledStat only creates samples whenever record is called,
             * if no record is called for a period of time that time is not accounted for in windowSize and produces incorrect results.
             */
            long totalElapsedTimeMs = now - internalStat.Oldest(now).LastWindowMs;
            // Check how many full windows of data we have currently retained
            int numFullWindows = (int) (totalElapsedTimeMs / config.TimeWindowMs);
            int minFullWindows = config.Samples - 1;

            // If the available windows are less than the minimum required, add the difference to the totalElapsedTime
            if (numFullWindows < minFullWindows)
                totalElapsedTimeMs += (minFullWindows - numFullWindows) * config.TimeWindowMs;

            return totalElapsedTimeMs;
        }
    }
}