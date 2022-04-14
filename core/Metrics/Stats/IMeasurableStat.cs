namespace Streamiz.Kafka.Net.Metrics.Stats
{
    /// <summary>
    /// Used for <see cref="Avg"/>, <see cref="Min"/>, <see cref="Max"/> ...
    /// </summary>
    internal interface IMeasurableStat : IStat, IMeasurable
    {
        
    }
}