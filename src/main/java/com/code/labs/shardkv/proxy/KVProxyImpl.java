package com.code.labs.shardkv.proxy;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.util.ExecutorServiceFuturePool;
import com.twitter.util.Function0;
import com.twitter.util.Future;

public class KVProxyImpl implements com.code.labs.shardkv.KVProxy.ServiceIface {

  private static final Logger LOG = LoggerFactory.getLogger(KVProxyImpl.class);
  ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("proxy-thread-%d").build();
  ThreadPoolExecutor executorService = new ThreadPoolExecutor(1, 1, 10, TimeUnit.SECONDS,
      new LinkedBlockingQueue<Runnable>(1000), threadFactory, new ThreadPoolExecutor.DiscardOldestPolicy());
  ExecutorServiceFuturePool futurePool = new ExecutorServiceFuturePool(executorService, true);

  @Override
  public Future<String> get(final String key) {
    return futurePool.apply(new Function0<String>() {
      @Override
      public String apply() {
        return key;
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

  public void close() {
    executorService.shutdown();
  }
}