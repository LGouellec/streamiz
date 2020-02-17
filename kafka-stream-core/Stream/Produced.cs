using kafka_stream_core.SerDes;

namespace kafka_stream_core.Stream
{
    public class Produced<K, V>
    {
        internal ISerDes<K> KeySerdes { get; }
        internal ISerDes<V> ValueSerdes { get; }


        internal Produced(ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
        {
            KeySerdes = keySerdes;
            ValueSerdes = valueSerdes;
        }

        public static Produced<K, V> with(ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
        {
            return new Produced<K, V>(keySerdes, valueSerdes);
        }
    }
}
