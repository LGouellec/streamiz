using Streamiz.Kafka.Net.Crosscutting;
using System;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// A single window instance, defined by its start and end timestamp.
    /// <see cref="Window"/> is agnostic if start/end boundaries are inclusive or exclusive; this is defined by concrete window implementations.
    /// To specify how <see cref="Window"/> boundaries are defined use <see cref="WindowOptions{W}"/>
    /// </summary>
    public abstract class Window
    {
        /// <summary>
        /// Start timestamp of this window.
        /// </summary>
        public long StartMs { get; protected set; }

        /// <summary>
        /// End timestamp of this window.
        /// </summary>
        public long EndMs { get; protected set; }

        /// <summary>
        /// Start datetime of this window.
        /// </summary>
        public DateTime StartTime { get; private set; }

        /// <summary>
        /// End datetime of this window.
        /// </summary>
        public DateTime EndTime { get; private set; }

        /// <summary>
        /// Total time of this window.
        /// </summary>
        public TimeSpan TotalTime => EndTime - StartTime;

        /// <summary>
        /// Create a new window for the given start and end time.
        /// </summary>
        /// <param name="startMs">the start timestamp of the window</param>
        /// <param name="endMs">the end timestamp of the window</param>
        /// <exception cref="ArgumentException">if <paramref name="startMs"/> is negative or if <paramref name="endMs"/> is smaller than or equal to <paramref name="startMs"/></exception>
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

        /// <summary>
        /// Check if the given window overlaps with this window.
        /// </summary>
        /// <param name="other">another window</param>
        /// <returns>true if other overlaps with this window, false otherwise</returns>
        public abstract bool Overlap(Window other);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
            => obj is Window &&
                obj.GetType().Equals(GetType()) &&
                (obj as Window).StartMs.Equals(StartMs) &&
                (obj as Window).EndMs.Equals(EndMs);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
            => (int) (((StartMs << 32) | EndMs) % 0xFFFFFFFFL);
    }
}
