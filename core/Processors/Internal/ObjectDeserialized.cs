namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class ObjectDeserialized
    {
        public object Bean { get; private set; }
        public bool MustBeSkipped { get; private set; }

        public static ObjectDeserialized ObjectSkipped => new ObjectDeserialized(true);

        public ObjectDeserialized(object bean, bool mustBeSkipped)
        {
            Bean = bean;
            MustBeSkipped = mustBeSkipped;
        }

        private ObjectDeserialized(bool mustBeSkipped)
        {
            MustBeSkipped = mustBeSkipped;
        }
    }
}
