using RocksDbSharp;
using Streamiz.Kafka.Net.Crosscutting;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.State
{
    internal class RocksDbRangeEnumerator : RocksDbEnumerator
    {
        private readonly byte[] rawLastKey;
        private readonly Func<byte[], byte[], int> keyComparator;

        public RocksDbRangeEnumerator(Iterator iterator, string name, Bytes from, Bytes to, Func<byte[], byte[], int> keyComparator, bool forward)
            : base(iterator, name, forward)
        {
            if (forward)
            {
                iterator.Seek(from.Get);
                rawLastKey = to.Get;
                if (rawLastKey == null)
                {
                    throw new NullReferenceException($"RocksDbRangeEnumerator: rawLastKey is null for key {to}");
                }
            }
            else
            {
                iterator.SeekForPrev(to.Get);
                rawLastKey = from.Get;
                if (rawLastKey == null)
                {
                    throw new NullReferenceException($"RocksDbRangeEnumerator: rawLastKey is null for key {from}");
                }
            }

            this.keyComparator = keyComparator;
        }

        public override bool MoveNext()
        {
            if (iterator.Valid())
            {
                if (forward)
                {
                    if (keyComparator(iterator.Key(), rawLastKey) <= 0)
                    {
                        Current = new KeyValuePair<Bytes, byte[]>(new Bytes(iterator.Key()), iterator.Value());
                    }
                    else
                    {
                        return false;
                    }
                }
                else
                {
                    if (keyComparator(iterator.Key(), rawLastKey) >= 0)
                    {
                        Current = new KeyValuePair<Bytes, byte[]>(new Bytes(iterator.Key()), iterator.Value());
                    }
                    else
                    {
                        return false;
                    }
                }

                iterator = forward ? iterator.Next() : iterator.Prev();
                return true;
            }
            else
            {
                Current = null;
                return false;
            }
        }
    }
}