using FASTER.core;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Faster
{
    public class FasterKeyValueStore : IKeyValueStore<Bytes, byte[]>
    {
        public FasterKeyValueStore()
        {
            this.Path = System.IO.Path.Join(System.IO.Path.GetTempPath(), "partition_" + (int)0);
            this.ObjectLog = Devices.CreateLogDevice(this.Path + "_obj.log");
            this.Log = Devices.CreateLogDevice(this.Path + ".log");
            this.Store = new FasterKV<string, int>(
                size: 1L << 20, // 1M cache lines of 64 bytes each = 64MB hash table
                logSettings: new LogSettings
                {
                    LogDevice = Log,
                    ObjectLogDevice = ObjectLog,
                    PreallocateLog = true,
                    MemorySizeBits = null,
                    PageSizeBits = null,
                    ReadCacheSettings = new ReadCacheSettings(),
                    SegmentSizeBits = null
                }
            );
            var funcs = new SimpleFunctions<string, int>((a, b) => a + b); // function used for read-modify-write (RMW).
            this.Session = this.Store.NewSession(funcs);
        }

        public FasterKV<string,int> Store { get; set; }

        public IDevice Log { get; set; }

        public IDevice ObjectLog { get; set; }

        public string Path { get; set; }
    }
}