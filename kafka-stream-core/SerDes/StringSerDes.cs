using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.SerDes
{
    public class StringSerDes : ISerDes<String>
    {
        public string Deserialize(byte[] data)
        {
            return Encoding.UTF8.GetString(data);
        }

        public byte[] Serialize(string data)
        {
            return Encoding.UTF8.GetBytes(data);
        }
    }
}
