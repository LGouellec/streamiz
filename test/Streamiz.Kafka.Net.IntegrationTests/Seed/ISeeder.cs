namespace Streamiz.Kafka.Net.IntegrationTests.Seed;

public interface ISeeder<T>
{
    public T SeedOnce();
}