using FluentResults;
using K4os.Compression.LZ4;
using K4os.Compression.LZ4.Streams;
using Polly;
using Polly.Retry;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
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

    /// <summary>
    /// Our standard fallback function
    /// </summary>
    private Func<Task<IEnumerable<T>?>>? _fallbackAsyncFunction;

    /// <summary>
    /// Our Mediator-based fallback function
    /// </summary>
    private Func<ValueTask<Result<IEnumerable<T>?>>>? _fallbackMediatorFunction;

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
        _options.CollectionKey ??= $"TakeThree:Caching{(_options.UseCompression ? "LZ4" : string.Empty)}:{typeof(T).Name}:Collection";
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
    /// <param name="fallbackMediatorFunction">The fallback mediator function.</param>
    /// <returns>The Redis cache collection builder.</returns>
    public virtual RedisCacheCollectionBuilder<T> WithFallback(Func<ValueTask<Result<IEnumerable<T>?>>> fallbackMediatorFunction)
    {
        _fallbackMediatorFunction = fallbackMediatorFunction;
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

            string[] cachedDataValues;
            try
            {
                var cachedDataResult = await GetCacheValuesAsync(batch.ToArray());
                if (cachedDataResult.IsFailed)
                {
                    return Result.Fail(cachedDataResult.Errors);
                }

                cachedDataValues = cachedDataResult.Value;
            }
            catch (Exception ex)
            {
                return Result.Fail(new Error("Error when attempting to read the records").CausedBy(ex));
            }

            var addResult = Result.Try(() => items.AddRange(cachedDataValues.Select(Deserialize<T>)));
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
            // Cache the value and ensure it did not run into any issues before adding to the set
            var cacheSetResult = await SetCacheValueAsync(itemKey, Serialize(Item));
            if (cacheSetResult.IsFailed)
            {
                return cacheSetResult;
            }

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

        string existingData;

        try
        {
            var existingDataResult = await GetCacheValueAsync(itemKey);
            if (existingDataResult.IsFailed)
            {
                return existingDataResult.ToResult();
            }

            existingData = existingDataResult.Value;
        }
        catch (Exception ex)
        {
            return Result.Fail(new Error("Error when attempting to update the record").CausedBy(ex));
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
            // Cache the value and ensure it did not run into any issues
            var cacheSetResult = await SetCacheValueAsync(itemKey, Serialize(existingItem));
            if (cacheSetResult.IsFailed)
            {
                return cacheSetResult;
            }
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
            await _redisDb.KeyDeleteAsync(itemKey);
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
    /// Serializes the given data to a string.
    /// </summary>
    /// <typeparam name="TValue">The type of data to serialize.</typeparam>
    /// <param name="data">The data to serialize.</param>
    /// <returns>The serialized data as a <see cref="string"/>.</returns>
    private static string Serialize<TValue>(TValue data)
    {
        return JsonSerializer.Serialize(data);
    }

    /// <summary>
    /// Deserializes the given string to the specified type.
    /// </summary>
    /// <typeparam name="TValue">The type to deserialize to.</typeparam>
    /// <param name="cachedData">The cached data to deserialize.</param>
    /// <returns>The deserialized object of type <typeparamref name="TValue"/>.</returns>
    private static TValue Deserialize<TValue>(string cachedData)
    {
        return JsonSerializer.Deserialize<TValue>(cachedData)!;
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

        if (_fallbackAsyncFunction is null && _fallbackMediatorFunction is null)
        {
            return Result.Fail("No fallback function set.");
        }

        IEnumerable<T>? fallbackResult = null!;
        try
        {
            if (_fallbackAsyncFunction is not null)
            {
                fallbackResult = await _fallbackAsyncFunction();
            }
            else if (_fallbackMediatorFunction is not null)
            {
                var mediatorResult = await _fallbackMediatorFunction();
                fallbackResult = mediatorResult.Value;
            }

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
                // Cache the value and ensure it did not run into any issues before adding to the set
                var cacheSetResult = await SetCacheValueAsync(itemKey, Serialize(item));
                if (cacheSetResult.IsFailed)
                {
                    return cacheSetResult;
                }

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
    private static Result<string?> GetItemIdentifierFromAttribute(T? item)
    {
        var identifierProperty = Array.Find(typeof(T).GetProperties(), prop => Attribute.IsDefined(prop, typeof(CacheRecordIdentifierAttribute)));

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

    /// <summary>
    /// Sets the cache value. Adds compression if enabled.
    /// </summary>
    /// <param name="key">The redis key</param>
    /// <param name="value">The value to store</param>
    /// <returns>The result of the operation</returns>
    private async Task<Result> SetCacheValueAsync(string key, string value)
    {
        RedisValue cacheValue = value;

        // Check to see if we have compression enabled. If we do, then we will compress the data before storage
        if (_options.UseCompression)
        {
            cacheValue = CompressString(value);
        }

        try
        {
            await _resiliencePipeline.ExecuteAsync(async _ => await _redisDb.StringSetAsync(key, cacheValue));
        }
        catch (Exception ex)
        {
            return Result.Fail(new Error($"Error when attempting to set the cache value{(_options.UseCompression ? " with compression" : string.Empty)}").CausedBy(ex));
        }

        return Result.Ok();
    }

    /// <summary>
    /// Gets the cache values. Decompresses if compression is enabled.
    /// </summary>
    /// <param name="keys">The redis keys to read from</param>
    /// <returns>The values from the cache</returns>
    private async Task<Result<string[]>> GetCacheValuesAsync(RedisKey[] keys)
    {
        RedisValue[] values;

        try
        {
            values = await _resiliencePipeline.ExecuteAsync(async _ => await _redisDb.StringGetAsync(keys));
        }
        catch (Exception ex)
        {
            return Result.Fail(new Error("Error when attempting to read the cache values").CausedBy(ex));
        }

        // Check to see if we have compression enabled. If we do, then we will compress the data before storage
        if (!_options.UseCompression)
        {
            return values
                .Select(v => v.ToString())
                .ToArray();
        }

        var decompressedValues = values.Select(rv => DecompressString((byte[])rv!));
        return decompressedValues.ToArray();
    }

    /// <summary>
    /// Gets the cache value. Decompresses if compression is enabled.
    /// </summary>
    /// <param name="key">The redis key to read from</param>
    /// <returns>The value from the cache</returns>
    private async Task<Result<string>> GetCacheValueAsync(RedisKey key)
    {
        RedisValue value;

        try
        {
            value = await _resiliencePipeline.ExecuteAsync(async _ => await _redisDb.StringGetAsync(key));
        }
        catch (Exception ex)
        {
            return Result.Fail(new Error("Error when attempting to read the cache value").CausedBy(ex));
        }

        if (value.IsNullOrEmpty)
        {
            return Result.Fail("Value not found in cache");
        }

        // Check to see if we have compression enabled. If we do, then we will compress the data before storage
        if (!_options.UseCompression)
        {
            return value.ToString();
        }

        var decompressedValue = DecompressString(value!);
        return decompressedValue;
    }

    /// <summary>
    /// Compresses the given string using LZ4.
    /// </summary>
    /// <param name="value">The value to compress</param>
    /// <returns>The compressed value</returns>
    private static byte[] CompressString(string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        var compressed = LZ4Pickler.Pickle(bytes);

        return compressed;
    }

    /// <summary>
    /// Decompresses the given byte array using LZ4.
    /// </summary>
    /// <param name="compressedBytes">The compressed data to decompress</param>
    /// <returns>The decompressed value</returns>
    private static string DecompressString(byte[] compressedBytes)
    {
        var uncompressedBytes = LZ4Pickler.Unpickle(compressedBytes);
        var value = Encoding.UTF8.GetString(uncompressedBytes);

        return value;
    }
}