using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using StackExchange.Redis;
using TakeThree.CachingHelpers.Enums;

namespace TakeThree.CachingHelpers.Redis
{
    public class RedisCacheCollectionModifier<T> : RedisCacheCollectionBuilder<T> where T : class
    {
        public RedisCacheCollectionModifier(IDatabase redisDb, OperationType operationType) : base(redisDb, operationType)
        {
        }

        /// <summary>
        /// Sets the item that you will create, update, or delete.
        /// </summary>
        /// <param name="item">The item.</param>
        /// <returns>The Redis cache collection builder.</returns>
        public RedisCacheCollectionModifier<T> WithItem(T item)
        {
            Item = item;
            return this;
        }

        /// <summary>
        /// Sets the identifier that will be used to perform create/update/delete operations on an item.
        /// </summary>
        /// <param name="propertyName">The property name.</param>
        /// <returns>The Redis cache collection builder.</returns>
        public RedisCacheCollectionModifier<T> WithPropertyRecordIdentifier(string propertyName)
        {
            IdentifierProperty = typeof(T).GetProperty(propertyName);
            if (IdentifierProperty == null)
            {
                throw new ArgumentException($"Property '{propertyName}' not found on type '{typeof(T).Name}'.");
            }

            return this;
        }

        /// <summary>
        /// Sets the identifier that will be used to perform create/update/delete operations on an item.
        /// </summary>
        /// <param name="identifierSelector">The identifier selector.</param>
        /// <returns>The Redis cache collection builder.</returns>
        public RedisCacheCollectionModifier<T> WithRecordIdentifier(Func<T, string> identifierSelector)
        {
            IdentifierSelector = identifierSelector;
            return this;
        }

        /// <summary>
        /// Sets the identifier that will be used to perform create/update/delete operations on an item.
        /// </summary>
        /// <param name="identifier">The identifier.</param>
        /// <returns>The Redis cache collection builder.</returns>
        public RedisCacheCollectionModifier<T> WithRecordIdentifier(string identifier)
        {
            Identifier = identifier;
            return this;
        }

        /// <summary>
        /// Sets the identifier that will be used to perform create/update/delete operations on an item.
        /// </summary>
        /// <param name="identifier">The identifier.</param>
        /// <returns>The Redis cache collection builder.</returns>
        public RedisCacheCollectionModifier<T> WithRecordIdentifier(int identifier)
        {
            Identifier = identifier.ToString();
            return this;
        }

        /// <summary>
        /// Sets the identifier that will be used to perform create/update/delete operations on an item.
        /// </summary>
        /// <param name="identifier">The identifier.</param>
        /// <returns>The Redis cache collection builder.</returns>
        public RedisCacheCollectionModifier<T> WithRecordIdentifier(Guid identifier)
        {
            Identifier = identifier.ToString("N");
            return this;
        }

        /// <summary>
        /// Sets the collection key that will be used to identify the collection of data.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns>The Redis cache collection builder.</returns>
        public new RedisCacheCollectionModifier<T> WithCollectionKey(string key)
        {
            base.WithCollectionKey(key);
            return this;
        }

        /// <summary>
        /// Sets the fallback function.
        /// </summary>
        /// <param name="fallbackAsyncFunction">The fallback async function.</param>
        /// <returns>The Redis cache collection builder.</returns>
        public new RedisCacheCollectionModifier<T> WithFallback(Func<Task<IEnumerable<T>?>> fallbackAsyncFunction)
        {
            base.WithFallback(fallbackAsyncFunction);
            return this;
        }

        /// <summary>
        /// Sets the expiration.
        /// </summary>
        /// <param name="expiration">The expiration.</param>
        /// <returns>The Redis cache collection builder.</returns>
        public new RedisCacheCollectionModifier<T> WithExpiration(TimeSpan expiration)
        {
            base.WithExpiration(expiration);
            return this;
        }
    }
}