using RocksDbSharp;
using Streamiz.Kafka.Net.Crosscutting;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.State.RocksDb
{
    internal class RocksDbRangeEnumerator : RocksDbEnumerator
    {
        private readonly byte[] rawLastKey;

        public RocksDbRangeEnumerator(Iterator iterator, string name, Bytes from, Bytes to, bool forward) 
            : base(iterator, name, forward)
        {
            if(forward)
            {
                iterator.Seek(from.Get);
                rawLastKey = to.Get;
                if (rawLastKey == null)
                    throw new NullReferenceException($"RocksDbRangeEnumerator: rawLastKey is null for key {to}");
            }
            else
            {
                iterator.SeekForPrev(to.Get);
                rawLastKey = from.Get;
                if (rawLastKey == null)
                    throw new NullReferenceException($"RocksDbRangeEnumerator: rawLastKey is null for key {from}");

            }
        }

        public override bool MoveNext()
        {
            if (iterator.Valid())
            {
                if (forward)
                {
                    if (BytesComparer.Compare(iterator.Key(), rawLastKey) <= 0)
                        Current = new KeyValuePair<Bytes, byte[]>(new Bytes(iterator.Key()), iterator.Value());
                    else
                        return false;
                }
                else
                {
                    if (BytesComparer.Compare(iterator.Key(), rawLastKey) >= 0)
                        Current = new KeyValuePair<Bytes, byte[]>(new Bytes(iterator.Key()), iterator.Value());
                    else
                        return false;
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
