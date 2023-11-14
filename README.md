![Take Three Technologies](https://i0.wp.com/take3tech.com/wp-content/uploads/2022/02/Take3Tech-LOGO-transparent-no-slogan.png?resize=300%2C89&ssl=1)

# Redis Caching Helpers

These helpers are designed to simplify your interactions with Redis, including fine tuned caching for lists, allowing you to create/edit/update individual items in a list without replacing the entire cache in a way that helps prevent collisions.

## About Take Three Technologies
Take Three Technologies was born from the idea that mortgage technology should be simple but powerful. Our data-centric platform gives you total transparency as the data flows seamlessly between our mortgage CRM, POS, and LOS. Combined with our complete integrations with third-party providers, no longer do you need complex implementations or multiple systems for each part of the origination process.

[Contact Us](https://take3tech.com/contact/)

## How To Initialize

The caching helper is designed to be initalized per object/collection. Out of the box, it uses this object type as part of the unique key that is stored in redis.

```
var myObjectCache = redisDatabase.GetDatabase()
.FromCache<MyObjectType>(new RedisCacheCollectionOptions
{
    Expiration = TimeSpan.FromDays(7), // when the object/collection will expire
    CollectionKey = "MyCustomKey" // use this only if you want to override the default key
});
```

## How To Use

Once your helper has been initalized, you can perform create/read/update/delete operations on the object/collection. On first run, the cache will not have any data in it, so it will fill the cache using the `.WithFallback` method. When results are returned, they will be returned using [FluentResults](https://github.com/altmann/FluentResults). Please review the documentation for that library to learn more detail on how to interact with a fluent result.

Before you begin, your class unique identifier must be decorated with the `CacheRecordIdentifier` attribute. This will be used as part of the cache initialization operation to create the keys in Redis.

```
public class MyObjectType
{
    /// <summary>
    /// The unique identifier of the record
    /// </summary>
    [CacheRecordIdentifier]
    public int Id { get; set; }

    /// <summary>
    /// A value we are caching
    /// </summary>
    public string Value { get; set; }
}
```

### Reading

```
var myObjectResult = await myObjectCache.ReadFromCollection()
    // pass your method into WithFallBack to populate the cache when the cache is not initalized
    .WithFallback(_dapperDbFs.FlowSheetAdministration.GetFlowSheetCategoriesAsync)
    .ExecuteAsync();

// Reading FluentResult object
if (myObjectResult.IsFailed) {
  return myObjectResut.Errors;
}

// Accessing the underlying data
var myObjectData = myObjectResult.Value;
```

### Updating

When it comes to create/update/delete operations, they require you to pass in a `RecordIdentifier` that will identify the individual record that is being updated. It is also recommended to pass `WithFallback` in all scenarios you are interacting with the cache, as it will allow you handle any scenario where the cache may not be initalized.

```
await myObjectCache.UpdateCollection()
        .WithRecordIdentifier(myCollectionItem.Id)
        .WithFallback(_myDependencyLayer.MyDatabaseCallForCachedData)
        .WithChanges(sheetCategory => sheetCategory.CategoryName = updatedCategoryName)
        .ExecuteAsync();
```

### Creating

Creating an item has you populate `WithItem`, which allows you to pass in the object you wish to insert into the cache collection.

```
await myObjectCache.AddToCollection()
    .WithFallback(_dapperDbFs.FlowSheetAdministration.GetFlowSheetCategoriesAsync)
    .WithItem(new MyObjectType
    {
        Id = 123,
        Value = "Hello World"
    })
    .ExecuteAsync();
```


### Deleting

Deleting is a simple operation where you just need to pass `WithRecordIdentifier`, which you will pass the unique identifier of the individual item you want to remove from the collection.

```
await myObjectCache.DeleteFromCollection()
    .WithRecordIdentifier(myCollectionItem.Id)
    .ExecuteAsync();
```
