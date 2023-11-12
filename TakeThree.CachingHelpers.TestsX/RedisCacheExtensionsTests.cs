using Moq;
using NUnit.Framework;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TakeThree.CachingHelpers.Redis;

namespace TakeThree.CachingHelpers.Tests;

[TestFixture]
public class RedisCacheExtensionTests
{
    private Mock<IDatabase> _mockDatabase = null!;
    private RedisCacheBuilder _cacheBuilder = null!;

    [SetUp]
    public void Setup()
    {
        _mockDatabase = new Mock<IDatabase>();
        _cacheBuilder = new RedisCacheBuilder(_mockDatabase.Object);
    }

    [Test]
    public void FromCache_InitializesBuilder()
    {
        var builder = _mockDatabase.Object.FromCache();
        Assert.IsNotNull(builder);
    }

    [Test]
    public async Task AddToCollection_AddsItemCorrectly()
    {
        const string key = "MyKey";
        const string item = "HelloWorld";

        _mockDatabase.Setup(db => db.StringSetAsync(key, It.IsAny<RedisValue>(), It.IsAny<TimeSpan?>(),
            It.IsAny<When>(), It.IsAny<CommandFlags>())).ReturnsAsync(true);

        var result = await _cacheBuilder.AddToCollection<string>()
            .WithExpiration(TimeSpan.FromHours(1))
            .WithItem(item)
            .WithKey(key)
            .WithIdentifier("MyCollection")
            .WithFallback(() => Task.FromResult<IEnumerable<string>?>(new List<string> { item }))
            .ExecuteAsync();

        _mockDatabase.Verify(db => db.StringSetAsync(key, It.IsAny<RedisValue>(),
            TimeSpan.FromHours(1), When.Always, CommandFlags.None), Times.Once);

        Assert.IsNotNull(result);
        Assert.IsTrue(result.Value.Contains(item));
    }

    [Test]
    public async Task AddToCollection_UsesFallbackOnFailure()
    {
        const string key = "MyKey";
        const string item = "HelloWorld";

        _mockDatabase.Setup(db => db.StringSetAsync(key, It.IsAny<RedisValue>(), It.IsAny<TimeSpan?>(),
            It.IsAny<When>(), It.IsAny<CommandFlags>())).ReturnsAsync(false);

        var result = await _cacheBuilder.AddToCollection<string>()
            .WithExpiration(TimeSpan.FromHours(1))
            .WithItem(item)
            .WithKey(key)
            .WithIdentifier("MyCollection")
            .WithFallback(() => Task.FromResult<IEnumerable<string>?>(new List<string> { item }))
            .ExecuteAsync();

        Assert.IsNotNull(result);
        Assert.IsTrue(result.Value.Contains(item));
    }
}
