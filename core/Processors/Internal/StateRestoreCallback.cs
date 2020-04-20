using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    public delegate void StateRestoreCallback(Bytes key, byte[] value);
}
