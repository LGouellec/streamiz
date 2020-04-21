using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    /// <summary>
    /// NOT IMPLEMENTED FOR MOMENT
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    public delegate void StateRestoreCallback(Bytes key, byte[] value);
}
