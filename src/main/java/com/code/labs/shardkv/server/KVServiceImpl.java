package com.code.labs.shardkv.server;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.util.ExecutorServiceFuturePool;
import com.twitter.util.Function0;
import com.twitter.util.Future;

public class KVServiceImpl implements com.code.labs.shardkv.KVService.ServiceIface {

  ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("server-pool-thread%d").build();
  ThreadPoolExecutor executorService = new ThreadPoolExecutor(1, 1, 10, TimeUnit.SECONDS,
      new LinkedBlockingQueue<Runnable>(1000), threadFactory, new ThreadPoolExecutor.DiscardOldestPolicy());
  ExecutorServiceFuturePool futurePool = new ExecutorServiceFuturePool(executorService, true);

  private int shardId;

  public KVServiceImpl(int shardId) {
    this.shardId = shardId;
    Thread checkupThread = new Thread() {
      @Override
      public void run() {
        while (true) {
          try {
            Thread.sleep(500);
            System.out.println(executorService);
          } catch (InterruptedException e) {
            break;
          }
        }
      }
    };
    checkupThread.setDaemon(true);
    checkupThread.start();
  }

  public void close() {
    executorService.shutdown();
  }

  @Override
  public Future<String> get(final String key) {
    return futurePool.apply(new Function0<String>() {
      @Override
      public String apply() {
        return String.valueOf(shardId);
      }
    });
  }

  @Override
  public Future<Boolean> put(String key, String value) {
    return futurePool.apply(new Function0<Boolean>() {
      @Override
      public Boolean apply() {
        return true;
      }
    });
  }
}
