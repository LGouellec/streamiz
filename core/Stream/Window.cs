using Streamiz.Kafka.Net.Crosscutting;
using System;

namespace Streamiz.Kafka.Net.Stream
{
    public abstract class Window
    {
        public long StartMs { get; protected set; }
        public long EndMs { get; protected set; }

        public DateTime StartTime { get; private set; }
        public DateTime EndTime { get; private set; }

        public TimeSpan TotalTime => EndTime - StartTime;

        public Window(long startMs, long endMs)
        {
            if (startMs < 0)
                throw new ArgumentException("Window startMs time cannot be negative.");

            if (endMs < startMs)
                throw new ArgumentException("Window endMs time cannot be smaller than window startMs time.");

            StartMs = startMs;
            EndMs = endMs;

            StartTime = StartMs.FromMilliseconds();
            EndTime = EndMs.FromMilliseconds();
        }

        public abstract bool Overlap(Window other);

        public override bool Equals(object obj)
            => obj is Window &&
                obj.GetType().Equals(GetType()) &&
                (obj as Window).StartMs.Equals(StartMs) &&
                (obj as Window).EndMs.Equals(EndMs);

        public override int GetHashCode()
            => (int) (((StartMs << 32) | EndMs) % 0xFFFFFFFFL);
    }
}
