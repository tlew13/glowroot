package org.glowroot.central.util;

import org.redisson.api.RLocalCachedMap;
import org.redisson.api.RedissonClient;
import org.redisson.api.options.LocalCachedMapOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;


public class RedissonAsyncInvalidationLocalCache<K extends /*@NonNull*/ Serializable, V extends /*@NonNull*/ Object> implements AsyncCache<K, V>{

    private static final Logger logger = LoggerFactory.getLogger(RedissonAsyncInvalidationLocalCache.class);
    private final String cacheName;
    private final AsyncCacheLoader<K, V> loader;

    private final RedissonClient redissonClient;

    private final RLocalCachedMap<K, V> cache;

    private final Executor executor;

    public RedissonAsyncInvalidationLocalCache(AsyncCacheLoader<K, V> loader, String cacheName, RedissonClient redissonClient, Executor executor) {
        this.redissonClient = redissonClient;
        this.loader = loader;
        this.cacheName = cacheName;
        LocalCachedMapOptions<K,V> options = LocalCachedMapOptions.name(cacheName);
        options.storeMode(LocalCachedMapOptions.StoreMode.LOCALCACHE);
        options.syncStrategy(LocalCachedMapOptions.SyncStrategy.INVALIDATE);
        this.cache = redissonClient.getLocalCachedMap(options);
        this.executor = executor;
    }

    public RedissonAsyncInvalidationLocalCache(AsyncCacheLoader<K, V> loader, String cacheName, RedissonClient redissonClient, Executor executor, int size) {
        this.redissonClient = redissonClient;
        this.loader = loader;
        this.cacheName = cacheName;
        LocalCachedMapOptions<K,V> options = LocalCachedMapOptions.name(cacheName);
        options.storeMode(LocalCachedMapOptions.StoreMode.LOCALCACHE);
        options.syncStrategy(LocalCachedMapOptions.SyncStrategy.INVALIDATE);
        options.cacheSize(size);
        options.maxIdle(java.time.Duration.ofMinutes(30));
        options.evictionPolicy(LocalCachedMapOptions.EvictionPolicy.LRU);
        this.cache = redissonClient.getLocalCachedMap(options);
        this.executor = executor;

    }



    @Override
    public CompletableFuture<V> get(K key) {
        logger.debug("getting async key: " + key + " and cache name: " + this.cacheName);
        V value = cache.get(key);
        if (value != null) {
            return CompletableFuture.completedFuture(value);
        }
        CompletableFuture<V> future = loader.load(key)
                // FIXME there's a race condition if invalidation is received at this point
                .thenApplyAsync(v -> {
                    cache.put(key, v);
                    return v;
                }, executor);
        return future;
    }

    @Override
    public void invalidate(K key) {
        logger.debug("invalidating async cache for key: " + key + " and cache name: " + this.cacheName);
        this.cache.remove(key);
    }

}