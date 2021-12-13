using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Mock.Kafka
{
    public class ThreadSafeList<T> : IList<T>
    {
        protected readonly List<T> _internalList = null;
        protected static object _lock = new ();

        public ThreadSafeList()
        : this(new List<T>())
        {
        }

        public ThreadSafeList(IEnumerable<T> items)
        {
            _internalList = new List<T>(items);
        }

        // Other Elements of IList implementation

        public IEnumerator<T> GetEnumerator()
        {
            return Clone().GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return Clone().GetEnumerator();
        }
        
        public List<T> Clone()
        {
            List<T> newList = new List<T>();

            lock (_lock)
            {
                _internalList.ForEach(x => newList.Add(x));
            }

            return newList;
        }

        public void Add(T item)
        {
            lock(_lock)
                _internalList.Add(item);
        }

        public void Clear()
        {
            lock(_lock)
                _internalList.Clear();
        }

        public bool Contains(T item)
        {
            lock(_lock)
                return _internalList.Contains(item);
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            lock(_lock)
                _internalList.CopyTo(array, arrayIndex);
        }

        public bool Remove(T item)
        {
            lock (_lock)
                return _internalList.Remove(item);
        }

        public int Count => _internalList.Count;
        public bool IsReadOnly => false;
        
        public int IndexOf(T item)
        {
            lock (_lock)
                return _internalList.IndexOf(item);
        }

        public void Insert(int index, T item)
        {
            lock(_lock)
                _internalList.Insert(index, item);
        }

        public void RemoveAt(int index)
        {
            lock (_lock)
                _internalList.RemoveAt(index);
        }

        public T this[int index]
        {
            get
            {
                lock (_lock)
                    return _internalList[index];
            } 
            set
            {
                lock (_lock)
                    _internalList[index] = value;
            } 
        }
    }
}