package org.glowroot.central.util;

import org.infinispan.util.function.SerializableFunction;
import org.redisson.api.*;
import org.redisson.api.annotation.RRemoteAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.*;

import static com.google.common.base.Preconditions.checkNotNull;

public class RedissonDistributedExececutionImpl<K extends Serializable, V extends Object> implements DistributedExecutionMap<K, V> {


    public interface TaskService<K extends Serializable, V, R extends Serializable> {
        <R extends Serializable> Serializable executeTask(K key, SerializableFunction<V, R> task);
    }

    @RRemoteAsync(TaskService.class)
    public interface TaskServiceAsync<K extends Serializable, V, R extends Serializable> {
        RFuture <R> executeTask(K key, SerializableFunction<V, R> task);
    }


    public static class TaskServiceImpl<K extends Serializable, V ,R extends Serializable> implements TaskService<K, V, R> {

        private final RedissonDistributedExececutionImpl<K,V> distributionImpl;

        public TaskServiceImpl(RedissonDistributedExececutionImpl<K,V> distributionImpl){
            this.distributionImpl = distributionImpl;
        }
        @Override
        public <R extends Serializable> Serializable executeTask(K key, SerializableFunction<V, R> task) {
            logger.debug("remote execute task");
            try {
                Serializable result = this.distributionImpl.executeLocal(key, task);
                return result;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(RedissonDistributedExececutionImpl.class);
    private final RedissonClient redissonClient;
    private final ConcurrentMap<K, V> cache;
    private final RMap<String, String> keyToServiceMap;
    private final TaskService<K, V, Serializable> taskService;
    private final String mapName;

    private final String serviceName;


    public RedissonDistributedExececutionImpl( String mapName, RedissonClient redissonClient) {
        this.mapName = mapName;
        this.serviceName = mapName + UUID.randomUUID();
        this.redissonClient = redissonClient;
        this.cache = new ConcurrentHashMap<>();
        this.keyToServiceMap = redissonClient.getMap(mapName + "KeyToServiceMap");
        this.taskService = new TaskServiceImpl<>(this);
        ExecutorService executor = Executors.newFixedThreadPool(5);
        RRemoteService remoteService = redissonClient.getRemoteService(this.serviceName);
        remoteService.register(TaskService.class, this.taskService, 2, executor);
    }

    private String getRemoteServiceName(String key){
        return this.keyToServiceMap.get(key);
    }

    @Override
    public V get(K key) {
        return cache.get(key);
    }

    @Override
    public void put(K key, V value) {
        System.out.println("putting key: " + key + " and value: " + value);
        cache.put(key, value);
        this.keyToServiceMap.put(key.toString(), this.serviceName);

    }

    @Override
    public void remove(K key, V value) {
        System.out.println("removing key: " + key + " and value: " + value);
        cache.remove(key, value);
        this.keyToServiceMap.remove(key.toString());
    }



    public <R extends Serializable> Serializable  executeLocal(K key, SerializableFunction<V, R> task) throws Exception {

        V value = this.cache.get(key);
        if (value == null) {
           System.out.println("agent not in local cache");
            return null;
        }
        return task.apply(value);
    }


    @Override
    public <R extends Serializable> Optional<R> execute(String key, int timeoutSeconds, SerializableFunction<V, R> task) throws Exception {

        //call only the remote service for the node we're looking for
        String serviceName = getRemoteServiceName(key);
        System.out.println("service name: " + serviceName);
        //if agent no longer registered
        if(serviceName == null){
            return Optional.empty();
        }

        RRemoteService remoteService = this.redissonClient.getRemoteService(serviceName);

        TaskServiceAsync<K, V, R> taskService = remoteService.get(TaskServiceAsync.class);

        RFuture<R> result = taskService.executeTask((K)key, task);


        if(result == null){
            System.out.println("result is null: " + serviceName);
            return Optional.empty();
        }
        R myResult = result.get(timeoutSeconds, TimeUnit.SECONDS);

        return Optional.ofNullable(myResult);


    }

}