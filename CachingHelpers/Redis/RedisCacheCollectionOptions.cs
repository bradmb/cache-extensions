using System;

namespace TakeThree.CachingHelpers.Redis;

public class RedisCacheCollectionOptions
{
    /// <summary>
    /// The unique identifier of the collection to cache. If not set, defaults to the type name of the collection.
    /// </summary>
    public string? CollectionKey { get; set; }

    /// <summary>
    /// When the data should expire. If not set, the data will not expire.
    /// </summary>
    public TimeSpan? Expiration { get; set; }
}