using kafka_stream_core.Crosscutting;

namespace kafka_stream_core.Processors.Internal
{
    public delegate void StateRestoreCallback(Bytes key, byte[] value);
}
