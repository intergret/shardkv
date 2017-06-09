package com.code.labs.shardkv.client;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.code.labs.shardkv.KVProxy;
import com.code.labs.shardkv.common.Env;
import com.code.labs.shardkv.common.zk.ZKConfig;
import com.code.labs.shardkv.common.zk.ZkEventListener;
import com.github.zkclient.ZkClient;
import com.twitter.finagle.Thrift;
import com.twitter.thrift.ServiceInstance;
import com.twitter.util.Await;
import com.twitter.util.Future;

public class ShardKVClient {

  private static final Logger LOG = LoggerFactory.getLogger(ShardKVClient.class);

  private volatile List<Map.Entry<String,KVProxy.ServiceIface>> proxyList;

  public ShardKVClient(Env env) {
    String zkAddress;
    switch (env) {
      case DEBUG:
        zkAddress = ZKConfig.DEBUG;
        break;
      default:
        throw new RuntimeException("Env " + env + " not support.");
    }
    initClient(zkAddress);

    if (CollectionUtils.isEmpty(proxyList)) {
      throw new RuntimeException("Can't find proxy!");
    }
  }

  private void initClient(final String zkAddress) {
    final ZkClient zkClient = new ZkClient(zkAddress);
    zkClient.waitUntilConnected();

    List<String> clientNodes = zkClient.getChildren(ZKConfig.PROXY_PATH);
    final Map<String,Map.Entry<String,KVProxy.ServiceIface>> nodes = new ConcurrentHashMap<>();
    zkClient.subscribeChildChanges(ZKConfig.PROXY_PATH, new ZkEventListener(ZKConfig.PROXY_PATH, clientNodes) {
      @Override
      public void onChildChange(String parent, List<String> children, List<String> newAdded, List<String> deleted) {
        for (String node : newAdded) {
          String fullPath = FilenameUtils.separatorsToUnix(FilenameUtils.concat(parent, node));
          byte[] bytes = zkClient.readData(fullPath);
          ServiceInstance serviceInstance = JSONObject.parseObject(new String(bytes), ServiceInstance.class);
          String schema = String.format("%s:%s", serviceInstance.getServiceEndpoint().getHost(),
              serviceInstance.getServiceEndpoint().getPort());
          KVProxy.ServiceIface iface = Thrift.newIface(schema, KVProxy.ServiceIface.class);
          nodes.put(node, new AbstractMap.SimpleEntry<>(schema, iface));
          LOG.info("Proxy node {} {} joined!", node, schema);
        }
        for (String node : deleted) {
          nodes.remove(node);
          LOG.info("Proxy node {} left!", node);
        }

        // ensure the new node overrides the old node.
        List<String> sortedNodes = new ArrayList<>();
        for (String node : nodes.keySet()) {
          sortedNodes.add(node);
        }
        Collections.sort(sortedNodes, Collections.reverseOrder());

        Set<String> uniqueClients = new HashSet<>();
        for (String node : sortedNodes) {
          String schema = nodes.get(node).getKey();
          if (uniqueClients.contains(schema)) {
            nodes.remove(node);
            LOG.warn("Proxy node {} {} duplicate, removed!", node, schema);
          } else {
            uniqueClients.add(schema);
          }
        }

        for (String node : nodes.keySet()) {
          LOG.info("Proxy node {} {} on service!", node, nodes.get(node).getKey());
        }
        ShardKVClient.this.proxyList = new ArrayList<>(nodes.values());
      }
    });
  }

  private Map.Entry<String,KVProxy.ServiceIface> getProxy() {
    return proxyList.get(ThreadLocalRandom.current().nextInt(proxyList.size()));
  }

  public String get(String key) throws Exception {
    long start = System.currentTimeMillis();
    Map.Entry<String,KVProxy.ServiceIface> proxy = getProxy();
    Future<String> future = proxy.getValue().get(key);
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

  public boolean put(String key, String value) throws Exception {
    long start = System.currentTimeMillis();
    Map.Entry<String,KVProxy.ServiceIface> proxy = getProxy();
    Future<Boolean> future = proxy.getValue().put(key, value);
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
    if (proxyList != null) {
      proxyList.clear();
    }
  }
}
