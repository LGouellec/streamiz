using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using FASTER.core;

namespace test_faster
{
    internal class Program
    {
        public static async Task Main(string[] args)
        {
            Directory.CreateDirectory($"{Path.GetTempPath()}/streamiz");
            var log = Devices.CreateLogDevice($"{Path.GetTempPath()}/streamiz/hlog.log");
            var objlog = Devices.CreateLogDevice($"{Path.GetTempPath()}/streamiz/hlog.obj.log");

            var settings = new FasterKVSettings<byte[], byte[]>
            {
                LogDevice = log,
                ObjectLogDevice = objlog,
                IndexSize = Utility.ParseSize("1MB"),
                MemorySize = Utility.ParseSize("16MB"),
                PageSize = Utility.ParseSize("4KB"),
                ReadCacheEnabled = true,
                ReadCacheMemorySize = Utility.ParseSize("32MB"),
                ReadCachePageSize = Utility.ParseSize("8KB"),
                SegmentSize = Utility.ParseSize("100MB"),
                TryRecoverLatest = true,
                RemoveOutdatedCheckpoints = true
            };
            
            var store = new FasterKV<byte[], byte[]>(settings);
            // store.Recover();
            
            var funcs = new SimpleFunctions<byte[], byte[]>();
            
            var session = store.For(funcs).NewSession(funcs);
            
            byte [] key = Encoding.UTF8.GetBytes("sylvain");
            byte [] value = Encoding.UTF8.GetBytes("sylvain2");

            var re = session.Read(key);
            
            session.Upsert(key, value);
            session.CompletePending(true);
            
            using var iter = session.Iterate();
            while (iter.GetNext(out var recordInfo))
            {
                 byte[] keyI = iter.GetKey();
                 byte[] valueI = iter.GetValue();
            }           
            
            await store.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver);
            await session.WaitForCommitAsync();
            
            session.Dispose();
            store.Dispose();
            log.Dispose();
            objlog.Dispose();
        }
        
        internal static long PreviousPowerOf2(long v)
        {
            v |= v >> 1;
            v |= v >> 2;
            v |= v >> 4;
            v |= v >> 8;
            v |= v >> 16;
            v |= v >> 32;
            return v - (v >> 1);
        }
    }
}