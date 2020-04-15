using System;
using System.Collections.Generic;
using System.Text;
using Kafka.Streams.Net.Crosscutting;
using Kafka.Streams.Net.Processors;

namespace Kafka.Streams.Net.State.InMemory
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

        public long ApproximateNumEntries() => size;

        public void Close() {
            map.Clear();
            size = 0;
            IsOpen = false;
        }

        public byte[] Delete(Bytes key)
        {
            byte[] v = map.ContainsKey(key) ? map[key] : null;
            size -= map.Remove(key) ? 1 : 0;
            return v;
        }

        public void Flush(){ /* Nothing => IN MEMORY */ }

        public byte[] Get(Bytes key) => map.ContainsKey(key) ? map[key] : null;

        public void Init(ProcessorContext context, IStateStore root)
        {
            size = 0;
            if (root != null)
            {
                // register the store
                context.Register(root, (key, value) => Put(key, value));
            }

            IsOpen = true;
        }

        public void Put(Bytes key, byte[] value)
        {
            if (value == null)
                size -= map.Remove(key) ? 1 : 0;
            else
                size += map.AddOrUpdate(key, value) ? 1 : 0;
        }

        public void PutAll(IEnumerable<KeyValuePair<Bytes, byte[]>> entries)
        {
            foreach (var kp in entries)
                this.Put(kp.Key, kp.Value);
        }

        public byte[] PutIfAbsent(Bytes key, byte[] value)
        {
            if(!map.ContainsKey(key))
            {
                this.Put(key, value);
            }
            // TODO : 
            return null;
        }
    }
}
