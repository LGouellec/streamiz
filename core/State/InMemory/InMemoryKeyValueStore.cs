using System;
using System.Collections.Generic;
using System.Text;
using kafka_stream_core.Crosscutting;
using kafka_stream_core.Processors;

namespace kafka_stream_core.State.InMemory
{
    internal class InMemoryKeyValueStore : KeyValueStore<byte[], byte[]>
    {
        private int size = 0;
        private readonly IDictionary<byte[], byte[]> map = new Dictionary<byte[], byte[]>();

        public InMemoryKeyValueStore(string name)
        {
            this.Name = name;
        }

        public string Name { get; }

        public bool Persistent => false;

        public bool IsOpen { get; private set; } = false;

        public long approximateNumEntries() => size;

        public void close() {
            map.Clear();
            size = 0;
            IsOpen = false;
        }

        public byte[] delete(byte[] key)
        {
            byte[] v = map.ContainsKey(key) ? map[key] : null;
            size -= map.Remove(key) ? 1 : 0;
            return v;
        }

        public void flush(){ /* Nothing => IN MEMORY */ }

        public byte[] get(byte[] key) => map[key];

        public void init(ProcessorContext context, StateStore root)
        {
            size = 0;
            if (root != null)
            {
                // register the store
                context.Register(root, (key, value) => put(key, value));
            }

            IsOpen = true;
        }

        public void put(byte[] key, byte[] value)
        {
            if (value == null)
                size -= map.Remove(key) ? 1 : 0;
            else
                size += map.AddOrUpdate(key, value) ? 1 : 0;
        }

        public void putAll(IEnumerable<KeyValuePair<byte[], byte[]>> entries)
        {
            foreach (var kp in entries)
                this.put(kp.Key, kp.Value);
        }

        public byte[] putIfAbsent(byte[] key, byte[] value)
        {
            if(!map.ContainsKey(key))
            {
                this.put(key, value);
            }
            // TODO : 
            return null;
        }
    }
}
