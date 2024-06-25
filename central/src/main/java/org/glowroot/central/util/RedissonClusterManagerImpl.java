package org.glowroot.central.util;

import org.glowroot.common2.repo.util.LockSet;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.codec.SerializationCodec;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class RedissonClusterManagerImpl extends ClusterManager {

    private static final Logger logger = LoggerFactory.getLogger(RedissonClusterManagerImpl.class);
    private final RedissonClient redisson;

    private final Executor executor;


    public RedissonClusterManagerImpl(File redisConfigurationFile) {


        Config config = null;
        try {
            config = Config.fromYAML(redisConfigurationFile);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Could create Redisson client from: " + redisConfigurationFile);
        }
        config.setCodec(new SerializationCodec());
        this.redisson = Redisson.create(config);


        this.executor = MoreExecutors2.newCachedThreadPool("Cluster-Manager-Worker");
    }

    @Override
    public <K extends /*@NonNull*/ Serializable, V extends /*@NonNull*/ Object> Cache<K, V> createPerAgentCache(
            String cacheName, int size, Cache.CacheLoader<K, V> loader) {
        return new RedissonInvalidationLocalCache<K, V>(loader, cacheName, this.redisson, size);


    }

    @Override
    public <K extends /*@NonNull*/ Serializable, V extends /*@NonNull*/ Object> AsyncCache<K, V> createPerAgentAsyncCache(
            String cacheName, int size, AsyncCache.AsyncCacheLoader<K, V> loader) {
        return new RedissonAsyncInvalidationLocalCache<K, V>(loader, cacheName, this.redisson, this.executor, size);
    }

    @Override
    public <K extends /*@NonNull*/ Serializable, V extends /*@NonNull*/ Object> Cache<K, V> createSelfBoundedCache(
            String cacheName, Cache.CacheLoader<K, V> loader) {
        return new RedissonInvalidationLocalCache<K, V>(loader, cacheName, this.redisson);
    }

    @Override
    public <K extends /*@NonNull*/ Serializable, V extends /*@NonNull*/ Object> DistributedExecutionMap<K, V> createDistributedExecutionMap(
            String cacheName) {
        return new RedissonDistributedExececutionImpl<K, V>(cacheName, this.redisson);
    }

    @Override
    public <K extends /*@NonNull*/ Serializable> LockSet<K> createReplicatedLockSet(
            String mapName, long expirationTime, TimeUnit expirationUnit) {
        return new LockSet.LockSetImpl<>(createReplicatedMap(mapName, expirationTime, expirationUnit));
    }

    @Override
    public <K extends /*@NonNull*/ Serializable, V extends /*@NonNull*/ Serializable> ConcurrentMap<K, V> createReplicatedMap(
            String mapName) {
        return redisson.getMapCache(mapName);
    }

    @Override
    public <K extends /*@NonNull*/ Serializable, V extends /*@NonNull*/ Serializable> ConcurrentMap<K, V> createReplicatedMap(
            String mapName, long expirationTime, TimeUnit expirationUnit) {
        return new RedissonReplicatedCache<>(mapName, this.redisson, expirationTime, expirationUnit);
    }


    @Override
    public void close() throws InterruptedException {
        this.redisson.shutdown();
    }
}