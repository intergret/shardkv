package com.code.labs.shardkv.client;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.code.labs.shardkv.KVProxy;
import com.code.labs.shardkv.ProxyGetResponse;
import com.code.labs.shardkv.ProxyPutResponse;
import com.code.labs.shardkv.common.Config;
import com.twitter.util.Await;
import com.twitter.util.Future;

public class ShardKVClient {

  private static final Logger LOG = LoggerFactory.getLogger(ShardKVClient.class);
  private ProxyClient shardKVProxy;

  public ShardKVClient() {
    shardKVProxy = new ProxyClient(Config.ZK);
  }

  public ProxyGetResponse get(String key) throws Exception {
    long start = System.currentTimeMillis();
    Map.Entry<String,KVProxy.ServiceIface> proxy = shardKVProxy.getProxy();
    Future<ProxyGetResponse> future = proxy.getValue().get(key);
    try {
      return Await.result(future);
    } catch (Exception e) {
      if (future != null) {
        future.cancel();
      }
      throw e;
    } finally {
      long elapse = System.currentTimeMillis() - start;
      if (elapse > 500) {
        LOG.warn("Slow request to {}, cost: {}ms", proxy.getKey(), elapse);
      }
    }
  }

  public ProxyPutResponse put(String key, String value) throws Exception {
    long start = System.currentTimeMillis();
    Map.Entry<String,KVProxy.ServiceIface> proxy = shardKVProxy.getProxy();
    Future<ProxyPutResponse> future = proxy.getValue().put(key, value);
    try {
      return Await.result(future);
    } catch (Exception e) {
      if (future != null) {
        future.cancel();
      }
      throw e;
    } finally {
      long elapse = System.currentTimeMillis() - start;
      if (elapse > 500) {
        LOG.warn("Slow request to {}, cost: {}ms", proxy.getKey(), elapse);
      }
    }
  }

  public void close() {
    if (shardKVProxy != null) {
      shardKVProxy.close();
    }
  }
}
