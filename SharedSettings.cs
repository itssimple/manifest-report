using StackExchange.Redis;

namespace Manifest.Report
{
    public static class SharedSettings
    {
        private static ConnectionMultiplexer _redisClient;

        public static ConnectionMultiplexer RedisClient => _redisClient ??= ConnectionMultiplexer.Connect("127.0.0.1:6379");

        private static ISubscriber _redisPubsubSubscriber;
        public static ISubscriber RedisPubsubSubscriber = _redisPubsubSubscriber ??= RedisClient.GetSubscriber();
    }
}
