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
    /// <param name="options">The options you can set to default certain settings.</param>
    /// <returns>A new instance of RedisCacheBuilder.</returns>
    public static RedisCacheBuilder<TItemType> FromCache<TItemType>(this IDatabase redisDb, RedisCacheCollectionOptions? options = null) where TItemType : class
    {
        return new RedisCacheBuilder<TItemType>(redisDb, options);
    }
}