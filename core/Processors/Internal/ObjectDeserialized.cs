namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class ObjectDeserialized
    {
        public object Bean { get; private set; } = null;
        public bool MustBeSkipped { get; private set; } = false;

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
