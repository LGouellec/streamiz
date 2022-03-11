using System;

namespace Streamiz.Kafka.Net.Crosscutting
{
    public static class ActionHelper
    {
        public static long MeasureLatency(Action action)
        {
            long before = DateTime.Now.GetMilliseconds();
            action();
            return DateTime.Now.GetMilliseconds() - before;
        }
    }
}