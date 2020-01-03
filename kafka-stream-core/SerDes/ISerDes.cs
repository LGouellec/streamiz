using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.SerDes
{
    public interface ISerDes<T>
    {
        T Deserialize(byte[] data);
        byte[] Serialize(T data);
    }
}
