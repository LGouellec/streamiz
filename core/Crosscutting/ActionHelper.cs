using System;
using Streamiz.Kafka.Net.Metrics;

namespace Streamiz.Kafka.Net.Crosscutting
{
    internal static class ActionHelper
    {
        internal static long MeasureLatency(Action action)
        {
            long before = MonotonicClock.CurrentMs;
            action();
            return MonotonicClock.CurrentMs - before;
        }
        
        internal static T MeasureLatency<T>(Func<T> actionToMeasure, Sensor sensor) 
        {
            long before = MonotonicClock.CurrentMs;
            T @return = actionToMeasure.Invoke();
            long after = MonotonicClock.CurrentMs;
            sensor.Record(after - before);
            return @return;
        }
        
        internal static void MeasureLatency(Action actionToMeasure, Sensor sensor) 
        {
            long before = MonotonicClock.CurrentMs;
            actionToMeasure.Invoke();
            long after = MonotonicClock.CurrentMs;
            sensor.Record(after - before);
        }
    }
}