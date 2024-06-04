package org.glowroot.central.util;

import org.redisson.Redisson;
import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class RedissonReplicatedCache<K, V> implements ConcurrentMap<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(RedissonReplicatedCache.class);
    private final RedissonClient redisson;
    private final RMapCache<K, V> cache;
    
    private final String cacheName;
    private long expirationTime = -1;

    private TimeUnit expirationUnit;

    public RedissonReplicatedCache(String cacheName, RedissonClient redisson) {
        this.redisson = redisson;
        this.cache = redisson.getMapCache(cacheName);
        this.cacheName = cacheName;
    }

    public RedissonReplicatedCache(String cacheName, RedissonClient redisson, long expirationTime, TimeUnit expirationUnit) {
        this.redisson = redisson;
        this.cache = redisson.getMapCache(cacheName);
        this.cacheName = cacheName;
        this.expirationTime = expirationTime;
        this.expirationUnit = expirationUnit;
    }

    @Override
    public V put(K key, V value) {
       // Set the TTL
        if(this.expirationTime >= 0) {
            this.cache.put(key, value, this.expirationTime, this.expirationUnit);
        }else{
            this.cache.put(key, value);
        }
        return value;
    }

    @Override
    public V get(Object key) {
        return this.cache.get(key);
    }

    @Override
    public V remove(Object key) {
        return this.cache.remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        if(this.expirationTime >= 0) {
            this.cache.putAll(m, this.expirationTime, this.expirationUnit);
        }else{
            this.cache.putAll(m);
        }
    }

    @Override
    public void clear() {
        this.cache.clear();
    }

    @Override
    public Set<K> keySet() {
        return this.cache.keySet();
    }

    @Override
    public Collection<V> values() {
        return this.cache.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return this.cache.entrySet();
    }

    @Override
    public int size() {
        return this.cache.size();
    }

    @Override
    public boolean isEmpty() {
        return this.cache.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return this.cache.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return this.cache.containsValue(value);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        if(this.expirationTime >= 0) {
            return this.cache.putIfAbsent(key, value, this.expirationTime, this.expirationUnit);
        }else{
            return this.cache.putIfAbsent(key, value);
        }
    }

    @Override
    public boolean remove(Object key, Object value) {
        return this.cache.remove(key, value);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
       return this.cache.replace(key, oldValue, newValue);
    }

    @Override
    public V replace(K key, V value) {
        return this.cache.replace(key, value);
    }
}