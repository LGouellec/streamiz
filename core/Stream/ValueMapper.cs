using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Stream
{
    public interface IValueMapper<V, VR>
    {
        VR apply(V value);
    }
}
