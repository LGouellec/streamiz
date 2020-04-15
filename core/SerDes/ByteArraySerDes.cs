namespace Kafka.Streams.Net.SerDes
{
    public class ByteArraySerDes : AbstractSerDes<byte[]>
    {
        public override byte[] Deserialize(byte[] data) => data;

        public override byte[] Serialize(byte[] data) => data;
    }
}
