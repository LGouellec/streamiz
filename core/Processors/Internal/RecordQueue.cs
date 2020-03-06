using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Processors.Internal
{
    internal class RecordQueue<T>
    {
        private readonly int maxSize;
        private readonly Queue<T> queue;

        public int Size => queue.Count;
        public int MaxSize => maxSize;

        public RecordQueue(int maxSize)
        {
            this.maxSize = maxSize;
            this.queue = new Queue<T>(this.maxSize);
        }

        public bool AddRecord(T record)
        {
            if (maxSize >= queue.Count)
            {
                queue.Enqueue(record);
                return true;
            }
            else
                return false;
        }

        public T GetNextRecord() => queue.Peek();

        public void Commit() => queue.Dequeue();
    }
}
