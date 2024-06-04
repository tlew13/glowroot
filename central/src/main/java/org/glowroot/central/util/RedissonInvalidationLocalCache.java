package org.glowroot.central.util;

import org.redisson.api.RLocalCachedMap;

import org.redisson.api.RedissonClient;
import org.redisson.api.options.LocalCachedMapOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;


public class RedissonInvalidationLocalCache<K extends Serializable, V> implements Cache<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(RedissonInvalidationLocalCache.class);
    private final String cacheName;
    private final CacheLoader<K, V> loader;

    private final RedissonClient redissonClient;
    private final RLocalCachedMap<K, V> cache;


    public RedissonInvalidationLocalCache(CacheLoader<K, V> loader, String cacheName, RedissonClient redissonClient, int size) {
        this.redissonClient = redissonClient;
        this.loader = loader;
        this.cacheName = cacheName;
        LocalCachedMapOptions<K,V> options = LocalCachedMapOptions.name(cacheName);
        //LOCALCACHE - store data in local cache only and use Redis only for data update/invalidation.
        //https://github.com/redisson/redisson/wiki/7.-distributed-collections#711-map-eviction-local-cache-and-data-partitioning
        //                       .cacheMode(CacheMode.INVALIDATION_ASYNC)
//                    .expiration()
//                    .maxIdle(30, MINUTES)
//                    .memory()
//                    .maxCount(size)
//                    .whenFull(EvictionStrategy.REMOVE)
//                    .statistics()
//                    .enable();
        options.storeMode(LocalCachedMapOptions.StoreMode.LOCALCACHE);
        options.syncStrategy(LocalCachedMapOptions.SyncStrategy.INVALIDATE);
        options.cacheSize(size);
        options.maxIdle(java.time.Duration.ofMinutes(30));
        options.evictionPolicy(LocalCachedMapOptions.EvictionPolicy.LRU);
        this.cache = redissonClient.getLocalCachedMap(options);

    }

    public RedissonInvalidationLocalCache(CacheLoader<K, V> loader, String cacheName, RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
        this.loader = loader;
        this.cacheName = cacheName;
        LocalCachedMapOptions<K,V> options = LocalCachedMapOptions.name(cacheName);
        options.storeMode(LocalCachedMapOptions.StoreMode.LOCALCACHE);
        options.syncStrategy(LocalCachedMapOptions.SyncStrategy.INVALIDATE);
        this.cache = redissonClient.getLocalCachedMap(options);

    }

    @Override
    public V get(K key) {
        V value = this.cache.get(key);
        if (value == null) {
            value = this.loader.load(key);
            this.cache.put(key, value);
        }
        return value;
    }

    @Override
    public void invalidate(K key) {
        logger.debug("invalidating cache for key: " + key + " and cache name: " + this.cacheName);
        this.cache.remove(key);
    }

}