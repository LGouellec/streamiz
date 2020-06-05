using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.State
{
    public class Windowed<K>
    {
        
        public Windowed(K key, Window window)
        {
            Key = key;
            Window = window;
        }

        public K Key { get; }
        public Window Window { get; }
    }
}
