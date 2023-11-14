using System;
using System.Collections.Generic;
using StackExchange.Redis;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using TakeThree.CachingHelpers.Enums;

namespace TakeThree.CachingHelpers.Redis;

/// <summary>
/// Redis cache builder class.
/// </summary>
[SuppressMessage("ReSharper", "UnusedMember.Global")]
public class RedisCacheBuilder<TItemType> where TItemType : class
{
    private readonly IDatabase _redisDb;
    private readonly RedisCacheCollectionOptions? _options;

    /// <summary>
    /// Constructor for RedisCacheBuilder.
    /// </summary>
    /// <param name="redisDb">The Redis database.</param>
    /// <param name="options">The options you can set to default certain settings.</param>
    public RedisCacheBuilder(IDatabase redisDb, RedisCacheCollectionOptions? options = null)
    {
        _redisDb = redisDb;
        _options = options;
    }

    /// <summary>
    /// Method to read from a Redis cache collection.
    /// </summary>
    /// <typeparam name="TItemType">The type of item in the collection.</typeparam>
    /// <returns>A RedisCacheCollectionBuilder instance for reading from the collection.</returns>
    public RedisCacheCollectionBuilder<TItemType> ReadFromCollection()
    {
        return new RedisCacheCollectionBuilder<TItemType>(_redisDb, OperationType.Read, _options);
    }

    /// <summary>
    /// Method to replace a Redis cache collection.
    /// </summary>
    /// <typeparam name="TItemType">The type of item in the collection.</typeparam>
    /// <returns>A RedisCacheCollectionBuilder instance for replacing the collection.</returns>
    public RedisCacheCollectionModifier<TItemType> ReplaceCollection(Func<Task<IEnumerable<TItemType>?>> replaceFunction)
    {
        var collectionModifier = new RedisCacheCollectionModifier<TItemType>(_redisDb, OperationType.Replace, _options);
        collectionModifier.WithFallback(replaceFunction);

        return collectionModifier;
    }

    /// <summary>
    /// Method to add to a Redis cache collection.
    /// </summary>
    /// <typeparam name="TItemType">The type of item in the collection.</typeparam>
    /// <returns>A RedisCacheCollectionBuilder instance for adding to the collection.</returns>
    public RedisCacheCollectionModifier<TItemType> AddToCollection()
    {
        return new RedisCacheCollectionModifier<TItemType>(_redisDb, OperationType.Add, _options);
    }

    /// <summary>
    /// Method to update a Redis cache collection.
    /// </summary>
    /// <typeparam name="TItemType">The type of item in the collection.</typeparam>
    /// <returns>A RedisCacheCollectionBuilder instance for updating the collection.</returns>
    public RedisCacheCollectionModifier<TItemType> UpdateCollection()
    {
        return new RedisCacheCollectionModifier<TItemType>(_redisDb, OperationType.Update, _options);
    }

    /// <summary>
    /// Method to delete from a Redis cache collection.
    /// </summary>
    /// <typeparam name="TItemType">The type of item in the collection.</typeparam>
    /// <returns>A RedisCacheCollectionBuilder instance for deleting from the collection.</returns>
    public RedisCacheCollectionModifier<TItemType> DeleteFromCollection()
    {
        return new RedisCacheCollectionModifier<TItemType>(_redisDb, OperationType.Delete, _options);
    }
}