using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Stream.Internal
{
    internal interface InternalNameProvider
    {
        string newProcessorName(string prefix);

        string newStoreName(string prefix);
    }
}
