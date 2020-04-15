using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.State
{
    public interface ReadOnlyKeyValueStore<K, V>
    {
        V Get(K key);

        // TODO : 
        //KeyValueIterator<K, V> range(K from, K to);

        //KeyValueIterator<K, V> all();

        long ApproximateNumEntries();
    }
}
