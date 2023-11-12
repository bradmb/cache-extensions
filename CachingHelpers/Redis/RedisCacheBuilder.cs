using StackExchange.Redis;
using System.Diagnostics.CodeAnalysis;
using TakeThree.CachingHelpers.Enums;

namespace TakeThree.CachingHelpers.Redis;

/// <summary>
/// Redis cache builder class.
/// </summary>
[SuppressMessage("ReSharper", "UnusedMember.Global")]
public class RedisCacheBuilder
{
    private readonly IDatabase _redisDb;

    /// <summary>
    /// Constructor for RedisCacheBuilder.
    /// </summary>
    /// <param name="redisDb">The Redis database.</param>
    public RedisCacheBuilder(IDatabase redisDb)
    {
        _redisDb = redisDb;
    }

    /// <summary>
    /// Method to read from a Redis cache collection.
    /// </summary>
    /// <typeparam name="TItemType">The type of item in the collection.</typeparam>
    /// <returns>A RedisCacheCollectionBuilder instance for reading from the collection.</returns>
    public RedisCacheCollectionBuilder<TItemType> ReadFromCollection<TItemType>() where TItemType : class
    {
        return new RedisCacheCollectionBuilder<TItemType>(_redisDb, OperationType.Read);
    }

    /// <summary>
    /// Method to add to a Redis cache collection.
    /// </summary>
    /// <typeparam name="TItemType">The type of item in the collection.</typeparam>
    /// <returns>A RedisCacheCollectionBuilder instance for adding to the collection.</returns>
    public RedisCacheCollectionBuilder<TItemType> AddToCollection<TItemType>() where TItemType : class
    {
        return new RedisCacheCollectionBuilder<TItemType>(_redisDb, OperationType.Add);
    }

    /// <summary>
    /// Method to update a Redis cache collection.
    /// </summary>
    /// <typeparam name="TItemType">The type of item in the collection.</typeparam>
    /// <returns>A RedisCacheCollectionBuilder instance for updating the collection.</returns>
    public RedisCacheCollectionBuilder<TItemType> UpdateCollection<TItemType>() where TItemType : class
    {
        return new RedisCacheCollectionBuilder<TItemType>(_redisDb, OperationType.Update);
    }

    /// <summary>
    /// Method to delete from a Redis cache collection.
    /// </summary>
    /// <typeparam name="TItemType">The type of item in the collection.</typeparam>
    /// <returns>A RedisCacheCollectionBuilder instance for deleting from the collection.</returns>
    public RedisCacheCollectionBuilder<TItemType> DeleteFromCollection<TItemType>() where TItemType : class
    {
        return new RedisCacheCollectionBuilder<TItemType>(_redisDb, OperationType.Delete);
    }
}