package com.code.labs.shardkv.proxy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.code.labs.shardkv.GetResponse;
import com.code.labs.shardkv.ProxyGetResponse;
import com.code.labs.shardkv.ProxyPutResponse;
import com.code.labs.shardkv.PutResponse;
import com.code.labs.shardkv.common.Config;
import com.code.labs.shardkv.proxy.route.ConsistentHashRoute;
import com.code.labs.shardkv.proxy.route.RouteRule;
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

  private Map<Integer,ShardClient> shardClients = new ConcurrentHashMap<>();
  private RouteRule routeRule = new ConsistentHashRoute(Config.SHARD_SIZE);

  public KVProxyImpl() {
    for (int shardId = 0; shardId < Config.SHARD_SIZE; shardId++) {
      shardClients.put(shardId, new ShardClient(Config.ZK, shardId));
    }
  }

  @Override
  public Future<ProxyGetResponse> get(final String key) {
    return futurePool.apply(new Function0<ProxyGetResponse>() {
      @Override
      public ProxyGetResponse apply() {
        int shardId = routeRule.route(key);
        try {
          GetResponse response = shardClients.get(shardId).read(key);
          if (response.isSuccess()) {
            return new ProxyGetResponse(shardId, true).setValue(response.getValue());
          } else {
            return new ProxyGetResponse(shardId, false).setMsg(response.getMsg());
          }
        } catch (Exception e) {
          return new ProxyGetResponse(shardId, false).setMsg(e.toString());
        }
      }
    });
  }

  @Override
  public Future<ProxyPutResponse> put(final String key, final String value) {
    return futurePool.apply(new Function0<ProxyPutResponse>() {
      @Override
      public ProxyPutResponse apply() {
        int shardId = routeRule.route(key);
        try {
          PutResponse response = shardClients.get(shardId).write(key, value);
          if (response.isSuccess()) {
            return new ProxyPutResponse(shardId, true);
          } else {
            return new ProxyPutResponse(shardId, false).setMsg(response.getMsg());
          }
        } catch (Exception e) {
          return new ProxyPutResponse(shardId, false).setMsg(e.toString());
        }
      }
    });
  }

  public void close() {
    executorService.shutdown();
  }
}