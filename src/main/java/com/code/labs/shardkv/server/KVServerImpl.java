package com.code.labs.shardkv.server;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.code.labs.shardkv.server.storage.KVStore;
import com.code.labs.shardkv.server.storage.MemoryStore;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.util.ExecutorServiceFuturePool;
import com.twitter.util.Function0;
import com.twitter.util.Future;

public class KVServerImpl implements com.code.labs.shardkv.KVServer.ServiceIface {

  private static final Logger LOG = LoggerFactory.getLogger(KVServerImpl.class);
  ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("server-thread-%d").build();
  ThreadPoolExecutor executorService = new ThreadPoolExecutor(1, 1, 10, TimeUnit.SECONDS,
      new LinkedBlockingQueue<Runnable>(1000), threadFactory, new ThreadPoolExecutor.DiscardOldestPolicy());
  ExecutorServiceFuturePool futurePool = new ExecutorServiceFuturePool(executorService, true);

  private int shardId;
  private KVStore kvStore = new MemoryStore();

  public KVServerImpl(int shardId) {
    this.shardId = shardId;
    Thread checkupThread = new Thread() {
      @Override
      public void run() {
        while (true) {
          try {
            Thread.sleep(5000);
            LOG.info("Executor service status {}", executorService);
          } catch (InterruptedException e) {
            break;
          }
        }
      }
    };
    checkupThread.setDaemon(true);
    checkupThread.start();
  }

  @Override
  public Future<String> get(final String key) {
    return futurePool.apply(new Function0<String>() {
      @Override
      public String apply() {
        return String.format("shard %d's value for key %s is %s", shardId, key, kvStore.get(key));
      }
    });
  }

  @Override
  public Future<Boolean> put(final String key, final String value) {
    return futurePool.apply(new Function0<Boolean>() {
      @Override
      public Boolean apply() {
        return kvStore.put(key, value);
      }
    });
  }

  public void close() {
    executorService.shutdown();
  }
}