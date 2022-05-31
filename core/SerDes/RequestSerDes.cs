namespace Streamiz.Kafka.Net.SerDes
{
    public class RequestSerDes<K, V>
    {
        internal ISerDes<K> RequestKeySerDes { get; private set; }
        internal ISerDes<V> RequestValueSerDes { get; private set; }

        public RequestSerDes(
            ISerDes<K> requestKeySerDes,
            ISerDes<V> requestValueSerDes)
        {
            RequestKeySerDes = requestKeySerDes;
            RequestValueSerDes = requestValueSerDes;
        }
    }
}