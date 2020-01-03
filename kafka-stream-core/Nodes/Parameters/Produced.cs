using kafka_stream_core.SerDes;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Nodes.Parameters
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
