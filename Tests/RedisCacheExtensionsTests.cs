using Moq;
using NUnit.Framework;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using TakeThree.CachingHelpers.Redis;

namespace TakeThree.CachingHelpers.Tests;


/// <summary>
/// Tests for RedisCacheExtension class
/// </summary>
[TestFixture]
public class RedisCacheExtensionTests
{
    private Mock<IDatabase> _mockDatabase = null!;

    private const string CollectionKey = "MyCollection";
    private const string RecordIdentifier = "MyKey";
    private const string Item = "HelloWorld";

    [SetUp]
    public void Setup()
    {
        _mockDatabase = new Mock<IDatabase>();
    }

    /// <summary>
    /// Test that FromCache method initializes builder correctly
    /// </summary>
    [Test]
    public void FromCache_InitializesBuilder()
    {
        var builder = _mockDatabase.Object.FromCache();
        Assert.IsNotNull(builder);
    }

    /// <summary>
    /// Test that AddToCollection method reads item correctly
    /// </summary>
    [Test]
    public async Task ReadFromCollection_ReadsItemCorrectly()
    {
        var itemJson = JsonSerializer.Serialize(Item);

        _mockDatabase.Setup(db => db.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>())).ReturnsAsync(itemJson);
        _mockDatabase.Setup(db => db.SetMembersAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>())).ReturnsAsync(new List<RedisValue> { CollectionKey }.ToArray());

        var verifyResult = await _mockDatabase.Object
            .FromCache()
            .ReadFromCollection<string>()
            .WithCollectionKey(CollectionKey)
            .WithRecordIdentifier(RecordIdentifier)
            .ExecuteAsync();

        Assert.IsTrue(verifyResult.IsSuccess);
        Assert.IsNotNull(verifyResult.Value);
        Assert.AreEqual(Item, verifyResult.Value.First());
    }

    /// <summary>
    /// Test that AddToCollection method gracefully fails when there are no results
    /// </summary>
    [Test]
    public async Task ReadFromCollection_FindsNoResults()
    {
        var verifyResult = await _mockDatabase.Object
            .FromCache()
            .ReadFromCollection<string>()
            .WithCollectionKey(CollectionKey)
            .ExecuteAsync();

        Assert.IsFalse(verifyResult.IsSuccess);
        Assert.AreEqual("No fallback function set.", verifyResult.Errors[0].Message);
    }

    /// <summary>
    /// Test that AddToCollection method uses fallback on failure
    /// </summary>
    [Test]
    public async Task AddToCollection_UsesFallbackOnFailure()
    {
        var result = await _mockDatabase.Object
            .FromCache()
            .AddToCollection<string>()
            .WithCollectionKey(CollectionKey)
            .WithRecordIdentifier(RecordIdentifier)
            .WithItem(Item)
            .WithExpiration(TimeSpan.FromHours(1))
            .WithFallback(() => Task.FromResult<IEnumerable<string>?>(new List<string> { Item }))
            .ExecuteAsync();

        Assert.IsTrue(result.IsSuccess);
    }
}