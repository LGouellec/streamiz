using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    /// <summary>
    /// Restoration logic for log-backed state stores upon restart, it takes one record at a time from the logs to apply to the restoring state.
    /// </summary>
    /// <param name="key">Record's key</param>
    /// <param name="value">Record's value</param>
    public delegate void StateRestoreCallback(Bytes key, byte[] value);
}
