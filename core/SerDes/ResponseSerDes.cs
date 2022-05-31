namespace Streamiz.Kafka.Net.SerDes
{
    public class ResponseSerDes<K, V>
    {
        internal ISerDes<K> ResponseKeySerDes { get; private set; }
        internal ISerDes<V> ResponseValueSerDes { get; private set; }

        public ResponseSerDes(
            ISerDes<K> responseKeySerDes,
            ISerDes<V> responseValueSerDes)
        {
            ResponseKeySerDes = responseKeySerDes;
            ResponseValueSerDes = responseValueSerDes;
        }
    }
}