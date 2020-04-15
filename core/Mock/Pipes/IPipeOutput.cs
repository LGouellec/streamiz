using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.Mock.Pipes
{
    internal class PipeOutputInfo
    {
        public string Topic { get; set; }
        public int Partition { get; set; }
        public long Offset { get; set; }
        public long Low { get; set; }
        public long High { get; set; }
    }

    interface IPipeOutput : IDisposable
    {
        KeyValuePair<byte[], byte[]> Read();
        IEnumerable<KeyValuePair<byte[], byte[]>> ReadList();
        List<PipeOutputInfo> GetInfos();
        int Size { get; }
        bool IsEmpty { get; }
    }
}
