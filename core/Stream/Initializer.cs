using System;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// The initializer interface for creating an initial value in aggregations.
    /// Initializer is used in combination with Aggregator.
    /// </summary>
    /// <typeparam name="VA">aggregate value type</typeparam>
    public interface Initializer<out VA>
    {
        /// <summary>
        /// Return the initial value for an aggregation.
        /// </summary>
        /// <returns>the initial value for an aggregation</returns>
        VA Apply();
    }

    internal class InitializerWrapper<T> : Initializer<T>
    {
        private readonly Func<T> function;

        public InitializerWrapper(Func<T> function)
        {
            this.function = function;
        }

        public T Apply() => function.Invoke();
    }
}
