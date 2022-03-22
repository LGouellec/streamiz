using System;
using Streamiz.Kafka.Net.Metrics;

namespace Streamiz.Kafka.Net.Crosscutting
{
    public static class ActionHelper
    {
        internal static long MeasureLatency(Action action)
        {
            long before = DateTime.Now.GetMilliseconds();
            action();
            return DateTime.Now.GetMilliseconds() - before;
        }
        
        internal static T MeasureLatency<T>(Func<T> actionToMeasure, Sensor sensor) 
        {
            long before = DateTime.Now.GetMilliseconds();
            T @return = actionToMeasure.Invoke();
            long after = DateTime.Now.GetMilliseconds();
            sensor.Record(after - before);
            return @return;
        }
        
        internal static void MeasureLatency(Action actionToMeasure, Sensor sensor) 
        {
            long before = DateTime.Now.GetMilliseconds();
            actionToMeasure.Invoke();
            long after = DateTime.Now.GetMilliseconds();
            sensor.Record(after - before);
        }
    }
}