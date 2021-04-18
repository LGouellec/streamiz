using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal interface ISegment : IKeyValueStore<Bytes, byte[]>
    {
        void Destroy();
    }
}
