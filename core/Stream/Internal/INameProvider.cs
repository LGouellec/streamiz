using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Stream.Internal
{
    internal interface INameProvider
    {
        string NewProcessorName(string prefix);

        string NewStoreName(string prefix);
    }
}
