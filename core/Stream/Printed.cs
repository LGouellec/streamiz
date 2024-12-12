using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Stream.Internal.Graph;
using System;
using System.IO;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// An object to define the options used when printing a <see cref="IKStream{K, V}"/>
    /// </summary>
    /// <typeparam name="K">key type</typeparam>
    /// <typeparam name="V">value type</typeparam>
    public class Printed<K, V>
    {
        private Printed(TextWriter writer)
        {
            this.Writer = writer;
        }

        internal TextWriter Writer { get; }

        internal string Name { get; private set; }
        internal string Label { get; private set; }
        internal IKeyValueMapper<K, V, string> Mapper { get; private set; } 
            = new WrappedKeyValueMapper<K, V, string>((k, v, c) => $"{k} {v}");

        internal IProcessorSupplier<K, V> Build(string processorName) => 
            new KStreamPrint<K, V>(
                new PrintForeachAction<K, V>(this.Writer, this.Mapper, this.Label == null ? this.Name : this.Label));

        #region Static

        /// <summary>
        /// Print the records of a <see cref="IKStream{K, V}"/> to <see cref="Console.Out"/>
        /// </summary>
        /// <returns>A new <see cref="Printed{K, V}"/> instance. </returns>
        public static Printed<K, V> ToOut() => new Printed<K, V>(Console.Out);

        /// <summary>
        /// Print the records of a <see cref="IKStream{K, V}"/> to <see cref="TextWriter"/> writer
        /// </summary>
        /// <param name="writer">A writer instance</param>
        /// <returns>A new <see cref="Printed{K, V}"/> instance.</returns>
        public static Printed<K, V> ToWriter(TextWriter writer) => new Printed<K, V>(writer);

        #endregion

        /// <summary>
        /// Print the records of a <see cref="IKStream{K, V}"/> with the provided label.
        /// </summary>
        /// <param name="label">Label to use</param>
        /// <returns>Itself</returns>
        public Printed<K, V> WithLabel(string label)
        {
            this.Label = label;
            return this;
        }

        /// <summary>
        /// Print the records of a <see cref="IKStream{K, V}"/> with the provided function mapper.
        /// </summary>
        /// <param name="mapper">Mapper to use</param>
        /// <returns>Itself</returns>
        public Printed<K, V> WithKeyValueMapper(Func<K, V, IRecordContext, string> mapper)
        {
            this.Mapper = new WrappedKeyValueMapper<K, V, string>(mapper);
            return this;
        }

        /// <summary>
        /// Print the records of a <see cref="IKStream{K, V}"/> with provided processor name.
        /// </summary>
        /// <param name="name">Processor name</param>
        /// <returns>Itself</returns>
        public Printed<K, V> WithName(string name)
        {
            this.Name = name;
            return this;
        }
    }
}
