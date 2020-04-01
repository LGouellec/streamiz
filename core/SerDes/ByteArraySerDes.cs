namespace kafka_stream_core.SerDes
{
    public class ByteArraySerDes : AbstractSerDes<byte[]>
    {
        public override byte[] Deserialize(byte[] data) => data;

        public override byte[] Serialize(byte[] data) => data;
    }
}
