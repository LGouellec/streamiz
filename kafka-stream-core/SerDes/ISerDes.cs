using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.SerDes
{
    public interface ISerDes
    {
        object DeserializeObject(byte[] data);
        byte[] SerializeObject(object data);
    }

    public interface ISerDes<T> : ISerDes
    {
        T Deserialize(byte[] data);
        byte[] Serialize(T data);
    }
}