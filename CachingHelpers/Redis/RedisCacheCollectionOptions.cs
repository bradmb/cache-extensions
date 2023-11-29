using System;

namespace TakeThree.CachingHelpers.Redis;

public class RedisCacheCollectionOptions
{
    /// <summary>
    /// The unique identifier of the collection to cache. If not set, defaults to the type name of the collection.
    /// </summary>
    public string? CollectionKey { get; set; }

    /// <summary>
    /// The limit on batch size for query operations to the cache. If not set, defaults to 2500.
    /// </summary>
    public int BatchOperationThresholdLimit { get; set; } = 2500;

    /// <summary>
    /// Enable compression of data when storing in the cache. If not set, defaults to true.
    /// </summary>
    public bool UseCompression { get; set; } = true;

    /// <summary>
    /// When the data should expire. If not set, the data will not expire.
    /// </summary>
    public TimeSpan? Expiration { get; set; }
}