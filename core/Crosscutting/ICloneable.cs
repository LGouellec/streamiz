namespace Streamiz.Kafka.Net.Crosscutting
{
    /// <summary>
    /// Supports cloning, which creates a new instance of a class with the same value as an existing instance.
    /// </summary>
    /// <typeparam name="T">Type of clone object</typeparam>
    public interface ICloneable<out T>
    {
        /// <summary>
        /// Clone current object to an another instance of <code>T</code>.
        /// </summary>
        /// <returns>Return an <typeparamref name="T"/> instance</returns>
        T Clone();
    }
}
