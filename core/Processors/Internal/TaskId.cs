using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Processors.Internal
{
    internal class TaskId
    {
        public int Id { get; set; }
        public int Partition { get; set; }

        public override string ToString() => $"{Id}-{Partition}";
    }
}
