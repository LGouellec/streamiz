using Streamiz.Kafka.Net.Crosscutting;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.State.RocksDb
{
    internal class WindowKeyBytesComparer : IEqualityComparer<WindowKeyBytes>, IComparer<WindowKeyBytes>
    {
        public bool Equals(WindowKeyBytes x, WindowKeyBytes y)
        {
            return x.Equals(y);
            
        }

        public int GetHashCode(WindowKeyBytes obj)
        {
            return obj.GetHashCode();
        }

        public int Compare(WindowKeyBytes x, WindowKeyBytes y)
        {
            var bytes1 = x.Get;
            var bytes2 = x.Get;
            using (var buffer1 = ByteBuffer.Build(bytes1))
            {
                using(var buffer2 = ByteBuffer.Build(bytes2))
                {
                    var sizeKey1 = buffer1.GetInt(0);
                    var sizeKey2 = buffer2.GetInt(0);
                    var key1 = buffer1.GetBytes(sizeof(int), sizeKey1);
                    var key2 = buffer1.GetBytes(sizeof(int), sizeKey1);
                    int compareKey = BytesComparer.Compare(key1, key2);
                    if (compareKey == 0)
                    {
                        long ts1 = buffer1.GetLong(sizeof(int) + sizeKey1);
                        long ts2 = buffer2.GetLong(sizeof(int) + sizeKey2);
                        int compareTs = ts1.CompareTo(ts2);
                        if (compareTs == 0)
                        {
                            int seq1 = buffer1.GetInt(sizeof(int) + sizeKey1 + sizeof(long));
                            int seq2 = buffer2.GetInt(sizeof(int) + sizeKey2 + sizeof(long));
                            return seq1.CompareTo(seq2);
                        }
                        else
                            return compareTs;
                    }
                    else
                        return compareKey;
                }
            }
        }
    }

    public class WindowKeyBytes : Bytes
    {
        private WindowKeyBytes(byte[] bytes)
            : base(bytes)
        {

        }

        public static WindowKeyBytes Wrap(byte[] key, long time, int seqnum)
        {
            int totalSize = key.Length + sizeof(long) + sizeof(int);
            using (var buffer = ByteBuffer.Build(totalSize))
            {
                buffer.PutInt(key.Length);
                buffer.Put(key);
                buffer.PutLong(time);
                buffer.PutInt(seqnum);
                return new WindowKeyBytes(buffer.ToArray());
            }
        }

        public static WindowKeyBytes Wrap(byte[] key)
        {
            if (key == null)
                return null;
            return new WindowKeyBytes(key);
        }
    }
}
