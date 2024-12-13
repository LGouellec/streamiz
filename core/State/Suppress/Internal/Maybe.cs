using Streamiz.Kafka.Net.Errors;

namespace Streamiz.Kafka.Net.State.Suppress.Internal
{
    internal sealed class Maybe<T>
        where T : class
    {
        private T nullableValue;

        public bool IsDefined { get; private set; }

        public T Value
        {
            get
            {
                if (IsDefined)
                    return nullableValue;
                
                throw new IllegalStateException("Value is not defined");
            }
            private set => nullableValue = value;
        }
        
        public Maybe(T nullableValue, bool isDefined)
        {
            Value = nullableValue;
            IsDefined = isDefined;
        }

        public static Maybe<T> Defined(T value) => new Maybe<T>(value, true);
        public static Maybe<T> Undefined() => new Maybe<T>(null, false);
    }
}