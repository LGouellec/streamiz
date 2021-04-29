using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.State.Internal;
using System;
using System.Collections;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State.Enumerator
{
    internal class SegmentEnumerator<S> : IKeyValueEnumerator<Bytes, byte[]>
        where S : ISegment
    {
        private readonly IEnumerator<S> segmentsEnumerator;
        private readonly Func<IKeyValueEnumerator<Bytes, byte[]>, bool> nextCondition;
        private readonly Bytes from;
        private readonly Bytes to;
        private readonly bool forward;

        private S currentSegment;
        private IKeyValueEnumerator<Bytes, byte[]> currentEnumerator;

        public SegmentEnumerator(
            IEnumerable<S> segments,
            Func<IKeyValueEnumerator<Bytes, byte[]>, bool> nextCondition, 
            Bytes from, 
            Bytes to, 
            bool forward)
        {
            segmentsEnumerator = segments.GetEnumerator();
            this.nextCondition = nextCondition;
            this.from = from;
            this.to = to;
            this.forward = forward;
        }

        #region IKeyValueEnumerator Impl

        public KeyValuePair<Bytes, byte[]>? Current => currentEnumerator?.Current;

        object IEnumerator.Current => Current;

        public void Dispose()
            => CloseCurrentEnumerator();

        public bool MoveNext()
        {
            while ((currentEnumerator == null || !nextCondition(currentEnumerator) || !currentSegment.IsOpen)
                    && segmentsEnumerator.MoveNext())
            {
                CloseCurrentEnumerator();
                currentSegment = segmentsEnumerator.Current;

                try
                {
                    if (from == null || to == null)
                        currentEnumerator = forward ? currentSegment.All().ToWrap() : currentSegment.ReverseAll().ToWrap();
                    else
                        currentEnumerator = forward ? currentSegment.Range(from, to) : currentSegment.ReverseRange(from, to);
                }
                catch (InvalidStateStoreException)
                {
                    // segment may have been close, ignore, next segment
                    currentEnumerator = null;
                }
            }

            return currentEnumerator != null && currentEnumerator.MoveNext();
        }

        public Bytes PeekNextKey()
            => currentEnumerator?.PeekNextKey();

        public void Reset()
        {
            CloseCurrentEnumerator();
            segmentsEnumerator.Reset();
        }

        #endregion

        private void CloseCurrentEnumerator()
        {
            currentEnumerator?.Dispose();
            currentEnumerator = null;
        }
    }
}
