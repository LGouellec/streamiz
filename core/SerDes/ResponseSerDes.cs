namespace Streamiz.Kafka.Net.SerDes
{
    public class ResponseSerDes<K, V>
    {
        internal ISerDes<K> ResponseKeySerDes { get; private set; }
        internal ISerDes<V> ResponseValueSerDes { get; private set; }

        internal static ResponseSerDes<K, V> Empty => new ResponseSerDes<K, V>(null, null);
        
        public ResponseSerDes(
            ISerDes<K> responseKeySerDes,
            ISerDes<V> responseValueSerDes)
        {
            ResponseKeySerDes = responseKeySerDes;
            ResponseValueSerDes = responseValueSerDes;
        }
    }
}