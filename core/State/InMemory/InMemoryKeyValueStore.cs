using System;
using System.Collections.Generic;
using System.Text;
using kafka_stream_core.Crosscutting;
using kafka_stream_core.Processors;

namespace kafka_stream_core.State.InMemory
{
    internal class InMemoryKeyValueStore : KeyValueStore<Bytes, byte[]>
    {
        private int size = 0;
        private readonly IDictionary<Bytes, byte[]> map = new Dictionary<Bytes, byte[]>(new BytesComparer());

        public InMemoryKeyValueStore(string name)
        {
            this.Name = name;
        }

        public string Name { get; }

        public bool Persistent => false;

        public bool IsOpen { get; private set; } = false;

        public long approximateNumEntries() => size;

        public void Close() {
            map.Clear();
            size = 0;
            IsOpen = false;
        }

        public byte[] delete(Bytes key)
        {
            byte[] v = map.ContainsKey(key) ? map[key] : null;
            size -= map.Remove(key) ? 1 : 0;
            return v;
        }

        public void Flush(){ /* Nothing => IN MEMORY */ }

        public byte[] get(Bytes key) => map.ContainsKey(key) ? map[key] : null;

        public void Init(ProcessorContext context, IStateStore root)
        {
            size = 0;
            if (root != null)
            {
                // register the store
                context.Register(root, (key, value) => put(key, value));
            }

            IsOpen = true;
        }

        public void put(Bytes key, byte[] value)
        {
            if (value == null)
                size -= map.Remove(key) ? 1 : 0;
            else
                size += map.AddOrUpdate(key, value) ? 1 : 0;
        }

        public void putAll(IEnumerable<KeyValuePair<Bytes, byte[]>> entries)
        {
            foreach (var kp in entries)
                this.put(kp.Key, kp.Value);
        }

        public byte[] putIfAbsent(Bytes key, byte[] value)
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
