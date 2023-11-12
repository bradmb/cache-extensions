using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using StackExchange.Redis;
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks;
using FluentResults;
using TakeThree.CachingHelpers.Enums;

namespace TakeThree.CachingHelpers.Redis;

/// <summary>
/// Redis cache collection builder.
/// </summary>
/// <typeparam name="T">The type of the collection.</typeparam>
[SuppressMessage("ReSharper", "UnusedMember.Global")]
public class RedisCacheCollectionBuilder<T> where T : class
{
    private readonly IDatabase _redisDb;
    private readonly OperationType _operationType;

    private string? _collectionKey;
    private TimeSpan? _expiration;

    private T? _item;
    private Func<Task<IEnumerable<T>?>>? _fallbackAsyncFunction;

    internal PropertyInfo? IdentifierProperty;
    internal string? Identifier;
    internal Func<T, string>? IdentifierSelector;

    /// <summary>
    /// Initializes a new instance of the <see cref="RedisCacheCollectionBuilder{T}"/> class.
    /// </summary>
    /// <param name="redisDb">The Redis database.</param>
    /// <param name="operationType">The operation type.</param>
    public RedisCacheCollectionBuilder(IDatabase redisDb, OperationType operationType)
    {
        _redisDb = redisDb;
        _operationType = operationType;
        _collectionKey = typeof(T).Name + ":Collection";
    }

    /// <summary>
    /// Sets the collection key that will be used to identify the collection of data.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <returns>The Redis cache collection builder.</returns>
    public virtual RedisCacheCollectionBuilder<T> WithCollectionKey(string key)
    {
        _collectionKey = key;
        return this;
    }

    /// <summary>
    /// Sets the fallback function.
    /// </summary>
    /// <param name="fallbackAsyncFunction">The fallback async function.</param>
    /// <returns>The Redis cache collection builder.</returns>
    public virtual RedisCacheCollectionBuilder<T> WithFallback(Func<Task<IEnumerable<T>?>> fallbackAsyncFunction)
    {
        _fallbackAsyncFunction = fallbackAsyncFunction;
        return this;
    }

    /// <summary>
    /// Sets the expiration.
    /// </summary>
    /// <param name="expiration">The expiration.</param>
    /// <returns>The Redis cache collection builder.</returns>
    public virtual RedisCacheCollectionBuilder<T> WithExpiration(TimeSpan expiration)
    {
        _expiration = expiration;
        return this;
    }

    /// <summary>
    /// Sets the item.
    /// </summary>
    /// <param name="item">The item.</param>
    /// <returns>The Redis cache collection builder.</returns>
    public virtual RedisCacheCollectionBuilder<T> WithItem(T item)
    {
        _item = item;
        return this;
    }

    /// <summary>
    /// Executes the async operation.
    /// </summary>
    /// <returns>The result of the operation.</returns>
    public async Task<Result<IEnumerable<T>>> ExecuteAsync()
    {
        // First, check to see if the cache is initialized. If it is not, we need to initialize it.
        var initializationResult = await InitializeCacheFromFallbackAsync();
        if (initializationResult.IsFailed)
        {
            return Result.Fail(initializationResult.Errors);
        }

        return _operationType switch
        {
            OperationType.Read => await ReadAsync(),
            OperationType.Add => await AddAsync(),
            OperationType.Update => await UpdateAsync(),
            OperationType.Delete => await DeleteAsync(),
            _ => Result.Fail("Unexpected operation type"),
        };
    }

    /// <summary>
    /// Performs a read operation asynchronously.
    /// </summary>
    /// <returns>A <see cref="Result"/> containing the operation outcome and the collection of items.</returns>
    private async Task<Result<IEnumerable<T>>> ReadAsync()
    {
        RedisValue[] itemIds;
        var items = new List<T>();

        try
        {
            itemIds = await _redisDb.SetMembersAsync(_collectionKey);
        }
        catch (Exception ex)
        {
            return Result.Fail(new Error("Error when attempting to read the record").CausedBy(ex));
        }

        foreach (var id in itemIds)
        {
            RedisValue cachedData;

            try
            {
                cachedData = await _redisDb.StringGetAsync(_collectionKey + ":" + id);
            }
            catch (Exception ex)
            {
                return Result.Fail(new Error("Error when attempting to read the record").CausedBy(ex));
            }

            if (cachedData.IsNullOrEmpty)
            {
                continue;
            }

            var addResult = Result.Try(() => items.Add(Deserialize<T>(cachedData)));
            if (addResult.IsFailed)
            {
                return addResult;
            }
        }

        return items;
    }

    /// <summary>
    /// Adds a new item to the collection asynchronously.
    /// </summary>
    /// <returns>A <see cref="Result"/> containing the operation outcome.</returns>
    private async Task<Result> AddAsync()
    {
        var idValueResult = GetItemIdentifier(_item);
        if (!idValueResult.IsSuccess)
        {
            return Result.Fail(idValueResult.Errors);
        }

        var itemKey = _collectionKey + ":" + idValueResult.Value;

        try
        {
            await _redisDb.StringSetAsync(itemKey, Serialize(_item));
            await _redisDb.SetAddAsync(_collectionKey, idValueResult.Value);
        }
        catch (Exception ex)
        {
            return Result.Fail(new Error("Error when attempting to add the record").CausedBy(ex));
        }

        // Now let's set the expiration if the value was passed
        return await UpdateCollectionExpirationAsync();
    }

    /// <summary>
    /// Updates an existing item in the collection asynchronously.
    /// </summary>
    /// <returns>A <see cref="Result"/> containing the operation outcome.</returns>
    private async Task<Result> UpdateAsync()
    {
        var idValueResult = GetItemIdentifier(_item);
        if (!idValueResult.IsSuccess)
        {
            return Result.Fail(idValueResult.Errors);
        }

        var itemKey = _collectionKey + ":" + idValueResult.Value;

        RedisValue existingData;

        try
        {
            existingData = await _redisDb.StringGetAsync(itemKey);
        }
        catch (Exception ex)
        {
            return Result.Fail(new Error("Error when attempting to update the record").CausedBy(ex));
        }

        if (existingData.IsNullOrEmpty)
        {
            return Result.Fail("Item not found in collection.");
        }

        var existingItemResult = Result.Try(() => Deserialize<T>(existingData));
        if (existingItemResult.IsFailed)
        {
            return Result.Fail(existingItemResult.Errors);
        }

        var existingItem = existingItemResult.Value;

        foreach (var prop in typeof(T).GetProperties())
        {
            if (!prop.CanWrite)
            {
                continue;
            }

            var newValue = prop.GetValue(_item);
            prop.SetValue(existingItem, newValue);
        }

        try
        {
            await _redisDb.StringSetAsync(itemKey, Serialize(existingItem));
        }
        catch (Exception ex)
        {
            return Result.Fail(new Error("Error when attempting to update the record").CausedBy(ex));
        }

        // Now let's set the expiration if the value was passed
        return await UpdateCollectionExpirationAsync();
    }

    /// <summary>
    /// Deletes an item from the collection asynchronously.
    /// </summary>
    /// <returns>A <see cref="Result"/> containing the operation outcome.</returns>
    private async Task<Result> DeleteAsync()
    {
        var idValueResult = GetItemIdentifier(_item);
        if (!idValueResult.IsSuccess)
        {
            return Result.Fail(idValueResult.Errors);
        }

        var itemKey = _collectionKey + ":" + idValueResult.Value;

        try
        {
            await _redisDb.StringSetAsync(itemKey, RedisValue.Null);
            await _redisDb.SetRemoveAsync(_collectionKey, idValueResult.Value);
        }
        catch (Exception ex)
        {
            return Result.Fail(new Error("Error when attempting to remove the record").CausedBy(ex));
        }

        // Now let's set the expiration if the value was passed
        return await UpdateCollectionExpirationAsync();
    }

    /// <summary>
    /// Serializes the given data to a RedisValue.
    /// </summary>
    /// <typeparam name="TValue">The type of data to serialize.</typeparam>
    /// <param name="data">The data to serialize.</param>
    /// <returns>The serialized data as a <see cref="RedisValue"/>.</returns>
    private RedisValue Serialize<TValue>(TValue data)
    {
        return JsonSerializer.Serialize(data);
    }

    /// <summary>
    /// Deserializes the given RedisValue to the specified type.
    /// </summary>
    /// <typeparam name="TValue">The type to deserialize to.</typeparam>
    /// <param name="cachedData">The cached data to deserialize.</param>
    /// <returns>The deserialized object of type <typeparamref name="TValue"/>.</returns>
    private TValue Deserialize<TValue>(RedisValue cachedData)
    {
        return JsonSerializer.Deserialize<TValue>(cachedData!)!;
    }

    /// <summary>
    /// Initializes the cache from a fallback function asynchronously if necessary.
    /// </summary>
    /// <returns>A <see cref="Result"/> indicating success or failure of the operation.</returns>
    private async Task<Result> InitializeCacheFromFallbackAsync()
    {
        // First we are going to see if the cache is already initialized. If
        // it is, then we are going to skip initialization.
        try
        {
            var itemIds = await _redisDb.SetMembersAsync(_collectionKey);
            if (itemIds is { Length: > 0 })
            {
                return Result.Ok();
            }
        }
        catch (Exception ex)
        {
            return Result.Fail(new Error("Error when attempting to read the record").CausedBy(ex));
        }

        if (_fallbackAsyncFunction is null)
        {
            return Result.Fail("No fallback function set.");
        }

        var fallbackResult = await _fallbackAsyncFunction();
        if (fallbackResult is null)
        {
            return Result.Fail("Fallback function returned null.");
        }

        foreach (var item in fallbackResult)
        {
            var idValueResult = GetItemIdentifier(item);
            if (!idValueResult.IsSuccess)
            {
                return Result.Fail(idValueResult.Errors);
            }

            var itemKey = _collectionKey + ":" + idValueResult.Value;

            try
            {
                _redisDb.StringSet(itemKey, Serialize(item));
                _redisDb.SetAdd(_collectionKey, idValueResult.Value);
            }
            catch (Exception ex)
            {
                return Result.Fail(new Error("Error when attempting to add the record").CausedBy(ex));
            }
        }

        // Now let's set the expiration if the value was passed
        return await UpdateCollectionExpirationAsync();
    }

    /// <summary>
    /// Updates the expiration of the entire collection asynchronously.
    /// </summary>
    /// <returns>A <see cref="Result"/> indicating success or failure of the operation.</returns>
    private async Task<Result> UpdateCollectionExpirationAsync()
    {
        if (_expiration is null)
        {
            return Result.Ok();
        }

        try
        {
            await _redisDb.KeyExpireAsync(_collectionKey, _expiration);
        }
        catch (Exception ex)
        {
            return Result.Fail(new Error("Error when attempting to set the expiration").CausedBy(ex));
        }

        return Result.Ok();
    }

    /// <summary>
    /// Retrieves the identifier for the current item.
    /// </summary>
    /// <param name="item">The item to identify.</param>
    /// <returns>A <see cref="Result"/> containing the identifier or an error.</returns>
    private Result<string?> GetItemIdentifier(T? item)
    {
        if (item is null)
        {
            return Result.Fail("Item not set. Please use WithItem to set a value.");
        }

        // If more than one of the three identifier options are set, fail and let the user know to only use one.
        var identifierCount = 0;
        if (IdentifierSelector != null)
        {
            identifierCount++;
        }

        if (IdentifierProperty is not null)
        {
            identifierCount++;
        }

        if (Identifier is not null)
        {
            identifierCount++;
        }

        if (identifierCount > 1)
        {
            return Result.Fail("More than one identifier option set. Please only use one.");
        }

        // Get the ID from either _identifierSelector or _identifierProperty
        if (IdentifierSelector != null)
        {
            return IdentifierSelector(item);
        }

        if (IdentifierProperty is not null)
        {
            return IdentifierProperty.GetValue(item)?.ToString();
        }

        if (Identifier is not null)
        {
            return Identifier;
        }

        return Result.Fail("Identifier not set. Please use WithIdentifier to set a value.");
    }
}