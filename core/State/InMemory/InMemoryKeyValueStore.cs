using System;
using System.Collections.Generic;
using System.Text;
using Kafka.Streams.Net.Crosscutting;
using Kafka.Streams.Net.Processors;

namespace Kafka.Streams.Net.State.InMemory
{
    /// <summary>
    /// <see cref="InMemoryKeyValueStore"/> implements <see cref="KeyValueStore{K, V}"/>. 
    /// This store can be used for development phase. It's not persistent, so be carefull.
    /// </summary>
    public class InMemoryKeyValueStore : KeyValueStore<Bytes, byte[]>
    {
        private int size = 0;
        private readonly IDictionary<Bytes, byte[]> map = new Dictionary<Bytes, byte[]>(new BytesComparer());

        /// <summary>
        /// Constructor with the store name
        /// </summary>
        /// <param name="name">Store name</param>
        public InMemoryKeyValueStore(string name)
        {
            this.Name = name;
        }

        /// <summary>
        /// Name of the store
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Return always false in <see cref="InMemoryKeyValueStore"/>
        /// </summary>
        public bool Persistent => false;

        /// <summary>
        /// Is open
        /// </summary>
        public bool IsOpen { get; private set; } = false;

        /// <summary>
        /// Calculate approximate the number of entries in the state store
        /// </summary>
        /// <returns></returns>
        public long ApproximateNumEntries() => size;

        /// <summary>
        /// Close the storage engine.
        /// Note that this function needs to be idempotent since it may be called
        /// several times on the same state store.
        /// Users only need to implement this function but should NEVER need to call this api explicitly
        /// as it will be called by the library automatically when necessary
        /// </summary>
        public void Close() {
            map.Clear();
            size = 0;
            IsOpen = false;
        }

        /// <summary>
        /// Delete the value from the store (if there is one).
        /// </summary>
        /// <param name="key">The key</param>
        /// <returns>Return old value or null if key not found</returns>
        public byte[] Delete(Bytes key)
        {
            byte[] v = map.ContainsKey(key) ? map[key] : null;
            size -= map.Remove(key) ? 1 : 0;
            return v;
        }

        /// <summary>
        /// Flush any cached data. Nothing for moment in <see cref="InMemoryKeyValueStore"/>
        /// </summary>
        public void Flush(){ /* Nothing => IN MEMORY */ }

        /// <summary>
        /// Get the value corresponding to this key.
        /// </summary>
        /// <param name="key">The key to fetch</param>
        /// <returns>The value or null if no value is found</returns>
        public byte[] Get(Bytes key) => map.ContainsKey(key) ? map[key] : null;

        /// <summary>
        /// Initialize state store.
        /// </summary>
        /// <param name="context">Processor context, used for register this store via <see cref="ProcessorContext.Register(IStateStore, Processors.Internal.StateRestoreCallback)"/></param>
        /// <param name="root">Root store</param>
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

        /// <summary>
        /// Update the value associated with this key.
        /// </summary>
        /// <param name="key">The key to associate the value to</param>
        /// <param name="value">The value to update, it can be null.if the serialized bytes are also null it is interpreted as deletes</param>
        public void Put(Bytes key, byte[] value)
        {
            if (value == null)
                size -= map.Remove(key) ? 1 : 0;
            else
                size += map.AddOrUpdate(key, value) ? 1 : 0;
        }

        /// <summary>
        /// Update all the given key/value pairs.
        /// </summary>
        /// <param name="entries">A list of entries to put into the store</param>
        public void PutAll(IEnumerable<KeyValuePair<Bytes, byte[]>> entries)
        {
            foreach (var kp in entries)
                this.Put(kp.Key, kp.Value);
        }

        /// <summary>
        /// Update the value associated with this key, unless a value is already associated with the key.
        /// </summary>
        /// <param name="key">The key to associate the value to</param>
        /// <param name="value">The value to update, it can be null.if the serialized bytes are also null it is interpreted as deletes</param>
        /// <returns>The old value or null if there is no such key.</returns>
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
