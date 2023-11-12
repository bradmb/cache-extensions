using System.Diagnostics.CodeAnalysis;
using StackExchange.Redis;

namespace TakeThree.CachingHelpers.Redis;

/// <summary>
/// Provides extension methods for Redis cache.
/// </summary>
[SuppressMessage("ReSharper", "UnusedMember.Global")]
public static class RedisCacheExtensions
{
    /// <summary>
    /// Creates a new instance of RedisCacheBuilder.
    /// </summary>
    /// <param name="redisDb">The Redis database instance.</param>
    /// <returns>A new instance of RedisCacheBuilder.</returns>
    public static RedisCacheBuilder FromCache(this IDatabase redisDb)
    {
        return new RedisCacheBuilder(redisDb);
    }
}