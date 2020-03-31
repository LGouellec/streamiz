using System;

namespace kafka_stream_core.SerDes
{
    public abstract class AbstractSerDes<T> : ISerDes<T>
    {
        public object DeserializeObject(byte[] data) => this.Deserialize(data);
        public byte[] SerializeObject(object data)
        {
            if (data is T)
                return this.Serialize((T)data);
            else
                throw new InvalidOperationException($"Impossible to serialize data type {data.GetType().Name} with {this.GetType().Name}<{typeof(T).Name}>");
        }


        public abstract byte[] Serialize(T data);

        public abstract T Deserialize(byte[] data);
    }
}
