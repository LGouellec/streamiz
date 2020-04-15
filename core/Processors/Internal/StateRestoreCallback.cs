using Kafka.Streams.Net.Crosscutting;

namespace Kafka.Streams.Net.Processors.Internal
{
    public delegate void StateRestoreCallback(Bytes key, byte[] value);
}
