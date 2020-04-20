using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Stream.Internal
{
    internal interface INameProvider
    {
        string NewProcessorName(string prefix);

        string NewStoreName(string prefix);
    }
}
