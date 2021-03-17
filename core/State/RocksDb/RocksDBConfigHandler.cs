namespace Streamiz.Kafka.Net.State.RocksDb
{
    public delegate void RocksDBConfigHandler(string storeName, RocksDbOptions options, IStreamConfig config);
}
