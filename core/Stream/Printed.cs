using kafka_stream_core.Errors;
using kafka_stream_core.Processors;
using kafka_stream_core.Stream.Internal.Graph;
using System;
using System.IO;

namespace kafka_stream_core.Stream
{
    public class Printed<K, V>
    {
        private Printed(TextWriter writer)
        {
            this.Writer = writer;
        }

        internal TextWriter Writer { get; }

        internal string Name { get; private set; }
        internal string Label { get; private set; }
        internal IKeyValueMapper<K, V, string> Mapper { get; private set; } = new WrappedKeyValueMapper<K, V, string>((k, v) => $"{k} {v}");

        internal IProcessorSupplier<K, V> Build(string processorName) => 
            new KStreamPrint<K, V>(
                new PrintForeachAction<K, V>(this.Writer, this.Mapper, this.Label == null ? this.Name : this.Label));

        #region Static

        public static Printed<K, V> ToOut() => new Printed<K, V>(Console.Out);

        #endregion

        public Printed<K, V> WithLabel(string label)
        {
            this.Label = label;
            return this;
        }

        public Printed<K, V> WithKeyValueMapper(Func<K, V, string> mapper)
        {
            this.Mapper = new WrappedKeyValueMapper<K, V, string>(mapper);
            return this;
        }

        public Printed<K, V> WithName(string name)
        {
            this.Name = name;
            return this;
        }
    }
}
