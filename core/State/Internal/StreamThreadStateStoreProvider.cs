using Streamiz.Kafka.Net.Processors;
using System.Linq;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Errors;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class StreamThreadStateStoreProvider : IStateStoreProvider
    {
        private readonly IThread streamThread;

        public StreamThreadStateStoreProvider(IThread streamThread)
        {
            this.streamThread = streamThread;
        }

        public IEnumerable<T> Stores<T>(string storeName, IQueryableStoreType<T> queryableStoreType) where T: class//, IStateStore
        {
            if (this.streamThread.State == ThreadState.DEAD)
            {
                return Enumerable.Empty<T>();
            }
            if (!this.streamThread.IsRunningAndNotRebalancing)
            {
                throw new InvalidStateStoreException($"Cannot get state store {storeName} because " +
                    $"the stream thread is {streamThread.State}, not RUNNING");
            }

            List<T> stores = new List<T>();
            foreach (var streamTask in streamThread.Tasks)
            {
                IStateStore store = streamTask.GetStore(storeName);
                if (store != null && queryableStoreType.Accepts(store))
                {
                    if (!store.IsOpen)
                    {
                        throw new InvalidStateStoreException($"Cannot get state store {storeName} for task {streamTask} because the " +
                            $"store is not open. The state store may have migrated to another instances.");
                    }

                    // TODO: handle TimestampedKeyValueStore and TimestampedWindowStore 

                    stores.Add(store as T);
                }
            }
            return stores;
        }
    }
}
