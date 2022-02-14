namespace Streamiz.Kafka.Net
{
    public interface IStreamMiddleware
    {
        void BeforeStart(IStreamConfig config);
        void AfterStart(IStreamConfig config);
        void BeforeStop(IStreamConfig config);
        void AfterStop(IStreamConfig config);
    }
}