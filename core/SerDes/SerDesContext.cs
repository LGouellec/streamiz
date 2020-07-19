namespace Streamiz.Kafka.Net.SerDes
{
    public class SerDesContext
    {
        public IStreamConfig Config { get; private set; }

        public SerDesContext(IStreamConfig config)
        {
            Config = config;
        }
    }
}
