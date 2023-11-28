using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using StackExchange.Redis;
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks;
using FluentResults;
using Polly;
using Polly.Retry;
using TakeThree.CachingHelpers.Attributes;
using TakeThree.CachingHelpers.Enums;

namespace TakeThree.CachingHelpers.Redis;

/// <summary>
/// Redis cache collection builder.
/// </summary>
/// <typeparam name="T">The type of the collection.</typeparam>
[SuppressMessage("ReSharper", "UnusedMember.Global")]
public class RedisCacheCollectionBuilder<T> where T : class
{
    /// <summary>
    /// The redis database connection
    /// </summary>
    private readonly IDatabase _redisDb;

    /// <summary>
    /// The type of operation to perform.
    /// </summary>
    private readonly OperationType _operationType;

    /// <summary>
    /// The options you can set to default certain settings.
    /// </summary>
    private readonly RedisCacheCollectionOptions _options;

    private Func<Task<IEnumerable<T>?>>? _fallbackAsyncFunction;

    /// <summary>
    /// The resilience pipeline to use for the operations (Polly).
    /// </summary>
    private readonly ResiliencePipeline _resiliencePipeline = new ResiliencePipelineBuilder()
        .AddRetry(new RetryStrategyOptions())
        .AddTimeout(TimeSpan.FromSeconds(10))
        .Build();

    /// <summary>
    /// Add Operations: The item to add to the collection.
    /// </summary>
    internal T? Item;

    /// <summary>
    /// Add/Update/Delete Operations: The identifier property to use to identify the item.
    /// </summary>
    internal PropertyInfo? IdentifierProperty;

    /// <summary>
    /// Add/Update/Delete Operations: The identifier to use to identify the item.
    /// </summary>
    internal string? Identifier;

    /// <summary>
    /// Add/Update/Delete Operations: The identifier selector function to use to identify the item.
    /// </summary>
    internal Func<T, string>? IdentifierSelector;

    /// <summary>
    /// Update Operations: The changes to make to the item.
    /// </summary>
    internal Action<T>? ChangesFunction;

    /// <summary>
    /// Initializes a new instance of the <see cref="RedisCacheCollectionBuilder{T}"/> class.
    /// </summary>
    /// <param name="redisDb">The Redis database.</param>
    /// <param name="operationType">The operation type.</param>
    /// <param name="options">The options you can set to default certain settings.</param>
    public RedisCacheCollectionBuilder(IDatabase redisDb, OperationType operationType, RedisCacheCollectionOptions? options = null)
    {
        _redisDb = redisDb;
        _operationType = operationType;

        _options = options ?? new RedisCacheCollectionOptions();
        _options.CollectionKey ??= "TakeThree:Caching:" + typeof(T).Name + ":Collection";
    }

    /// <summary>
    /// Sets the collection key that will be used to identify the collection of data.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <returns>The Redis cache collection builder.</returns>
    public virtual RedisCacheCollectionBuilder<T> WithCollectionKey(string key)
    {
        _options.CollectionKey = key;
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
        _options.Expiration = expiration;
        return this;
    }

    /// <summary>
    /// Executes the async operation.
    /// </summary>
    /// <returns>The result of the operation.</returns>
    public async Task<Result<IEnumerable<T>>> ExecuteAsync()
    {
        // First, check to see if the cache is initialized. If it is not, we need to initialize it.
        if (_operationType != OperationType.Replace)
        {
            var initializationResult = await InitializeCacheFromFallbackAsync();
            if (initializationResult.IsFailed)
            {
                return Result.Fail(initializationResult.Errors);
            }
        }

        return _operationType switch
        {
            OperationType.Read => await ReadAsync(),
            OperationType.Add => await AddAsync(),
            OperationType.Update => await UpdateAsync(),
            OperationType.Delete => await DeleteAsync(),
            OperationType.Replace => await ReplaceAsync(),
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
            itemIds = await _redisDb.SetMembersAsync(_options.CollectionKey);
        }
        catch (Exception ex)
        {
            return Result.Fail(new Error("Error when attempting to read the set members").CausedBy(ex));
        }

        // We are going to loop through the items in batches to avoid
        // a large amount of individual requests to Redis.
        var remainingCount = itemIds.Length;
        var completedCount = 0;

        var redisCacheKeys = itemIds
            .Select(itemId => new RedisKey($"{_options.CollectionKey}:{itemId}"))
            .ToList();

        while (remainingCount > 0)
        {
            var batch = redisCacheKeys.Skip(completedCount).Take(_options.BatchOperationThresholdLimit);

            RedisValue[] cachedDataResult;
            try
            {
                cachedDataResult = await _resiliencePipeline.ExecuteAsync(async _ => await _redisDb.StringGetAsync(batch.ToArray()));
            }
            catch (Exception ex)
            {
                return Result.Fail(new Error("Error when attempting to read the records").CausedBy(ex));
            }

            var addResult = Result.Try(() => items.AddRange(cachedDataResult.Select(Deserialize<T>)));
            if (addResult.IsFailed)
            {
                return addResult;
            }

            remainingCount -= _options.BatchOperationThresholdLimit;
            completedCount += _options.BatchOperationThresholdLimit;
        }

        return items;
    }

    /// <summary>
    /// Adds a new item to the collection asynchronously.
    /// </summary>
    /// <returns>A <see cref="Result"/> containing the operation outcome.</returns>
    private async Task<Result> AddAsync()
    {
        var idValueResult = GetItemIdentifier(Item);
        if (!idValueResult.IsSuccess)
        {
            return Result.Fail(idValueResult.Errors);
        }

        var itemKey = _options.CollectionKey + ":" + idValueResult.Value;

        try
        {
            await _redisDb.StringSetAsync(itemKey, Serialize(Item));
            await _redisDb.SetAddAsync(_options.CollectionKey, idValueResult.Value);
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
        var idValueResult = GetItemIdentifier(Item);
        if (!idValueResult.IsSuccess)
        {
            return Result.Fail(idValueResult.Errors);
        }

        var itemKey = _options.CollectionKey + ":" + idValueResult.Value;

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

        if (ChangesFunction is not null)
        {
            ChangesFunction(existingItem);
        }
        else if (Item is not null)
        {
            foreach (var prop in typeof(T).GetProperties())
            {
                if (!prop.CanWrite)
                {
                    continue;
                }

                var newValue = prop.GetValue(Item);
                prop.SetValue(existingItem, newValue);
            }
        }
        else
        {
            return Result.Fail("No changes were made to the item. Please pass either WithItem or WithChanges to update a cached value.");
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
        var idValueResult = GetItemIdentifier(Item);
        if (!idValueResult.IsSuccess)
        {
            return Result.Fail(idValueResult.Errors);
        }

        var itemKey = _options.CollectionKey + ":" + idValueResult.Value;

        try
        {
            await _redisDb.StringSetAsync(itemKey, RedisValue.Null);
            await _redisDb.SetRemoveAsync(_options.CollectionKey, idValueResult.Value);
        }
        catch (Exception ex)
        {
            return Result.Fail(new Error("Error when attempting to remove the record").CausedBy(ex));
        }

        // Now let's set the expiration if the value was passed
        return await UpdateCollectionExpirationAsync();
    }

    /// <summary>
    /// Replaces the entire collection asynchronously.
    /// </summary>
    /// <returns>A <see cref="Result"/> containing the operation outcome.</returns>
    private async Task<Result> ReplaceAsync()
    {
        RedisValue[] itemIds;

        // Get all of the keys of the items in the collection
        try
        {
            itemIds = await _redisDb.SetMembersAsync(_options.CollectionKey);
        }
        catch (Exception ex)
        {
            return Result.Fail(new Error("Error when attempting to read the record").CausedBy(ex));
        }

        // Next delete all of the individual items
        foreach (var id in itemIds)
        {
            try
            {
                await _redisDb.KeyDeleteAsync(_options.CollectionKey + ":" + id);
            }
            catch (Exception ex)
            {
                return Result.Fail(new Error("Error when attempting to replace the collection").CausedBy(ex));
            }
        }

        // Now delete the set
        try
        {
            await _redisDb.KeyDeleteAsync(_options.CollectionKey);
        }
        catch (Exception ex)
        {
            return Result.Fail(new Error("Error when attempting to replace the collection").CausedBy(ex));
        }

        // Now let's re-initialize the cache
        return await InitializeCacheFromFallbackAsync();
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
            var itemIds = await _redisDb.SetMembersAsync(_options.CollectionKey);
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

        IEnumerable<T>? fallbackResult;
        try
        {
            fallbackResult = await _fallbackAsyncFunction();
            if (fallbackResult is null)
            {
                return Result.Fail("Fallback function returned null.");
            }
        }
        catch (Exception ex)
        {
            return Result.Fail(new Error("Error when attempting to read the record").CausedBy(ex));
        }

        foreach (var item in fallbackResult)
        {
            var idValueResult = GetItemIdentifier(item);
            if (!idValueResult.IsSuccess)
            {
                return Result.Fail(idValueResult.Errors);
            }

            var itemKey = _options.CollectionKey + ":" + idValueResult.Value;

            try
            {
                _redisDb.StringSet(itemKey, Serialize(item));
                _redisDb.SetAdd(_options.CollectionKey, idValueResult.Value);
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
        if (_options.Expiration is null)
        {
            return Result.Ok();
        }

        try
        {
            await _redisDb.KeyExpireAsync(_options.CollectionKey, _options.Expiration);
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

        if (IdentifierSelector != null)
        {
            if (item is null)
            {
                return Result.Fail("Item not set. Please use WithItem.");
            }

            return IdentifierSelector(item);
        }

        if (IdentifierProperty is not null)
        {
            if (item is null)
            {
                return Result.Fail("Item not set. Please use WithItem.");
            }

            return IdentifierProperty.GetValue(item)?.ToString();
        }

        if (Identifier is not null)
        {
            return Identifier;
        }

        var attributeResult = GetItemIdentifierFromAttribute(item);
        if (attributeResult.IsSuccess)
        {
            return attributeResult.Value;
        }

        return Result.Fail("Identifier not set. Please use WithRecordIdentifier to set a value or use the CacheRecordIdentifierAttribute on the class property.");
    }

    /// <summary>
    /// Retrieves the identifier for the current item from the attribute.
    /// </summary>
    /// <param name="item">The item to identify.</param>
    /// <returns>A <see cref="Result"/> containing the identifier or an error.</returns>
    [SuppressMessage("Minor Code Smell", "S6602:\"Find\" method should be used instead of the \"FirstOrDefault\" extension", Justification = "Find is not available for the array")]
    private Result<string?> GetItemIdentifierFromAttribute(T? item)
    {
        var identifierProperty = typeof(T)
            .GetProperties()
            .FirstOrDefault(prop => Attribute.IsDefined(prop, typeof(CacheRecordIdentifierAttribute)));

        if (identifierProperty == null)
        {
            return Result.Fail("Attribute was not found");
        }

        var identifierValue = identifierProperty.GetValue(item)?.ToString();
        if (string.IsNullOrEmpty(identifierValue))
        {
            return Result.Fail("Attribute was not found");
        }

        return identifierValue;
    }
}