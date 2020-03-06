using kafka_stream_core.SerDes;

namespace kafka_stream_core.Stream
{
    public class Consumed<K,V>
    {
        internal ISerDes<K> KeySerdes { get; }
        internal ISerDes<V> ValueSerdes { get; }


        internal Consumed(ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
        {
            KeySerdes = keySerdes;
            ValueSerdes = valueSerdes;
        }

        public static Consumed<K,V> with(ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
        {
            return new Consumed<K, V>(keySerdes, valueSerdes);
        }
    }
}
