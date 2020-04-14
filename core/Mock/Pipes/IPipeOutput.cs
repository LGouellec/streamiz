using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Mock.Pipes
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

        List<PipeOutputInfo> GetInfos();
        int Size { get; }
    }
}
