package com.code.labs.shardkv.client;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.code.labs.shardkv.common.ZkEventListener;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.code.labs.shardkv.KVService;
import com.github.zkclient.ZkClient;
import com.twitter.finagle.Thrift;
import com.twitter.thrift.ServiceInstance;
import com.twitter.util.Await;
import com.twitter.util.Future;

public class ShardKVClient {

  private static final Logger LOG = LoggerFactory.getLogger(ShardKVClient.class);

  private static final String ZK_DEBUG = "127.0.0.1:2181";
  private static final String ZK_PATH = "/nodes";
  private static final Random random = new Random();

  private volatile List<Map.Entry<String,KVService.ServiceIface>> clients;

  public enum Env {
    DEBUG,
  }

  public ShardKVClient(Env env) {
    String zkAddress;
    switch (env) {
      case DEBUG:
        zkAddress = ZK_DEBUG;
        break;
      default:
        throw new RuntimeException("Env " + env + " not support.");
    }
    initClient(zkAddress);

    if (CollectionUtils.isEmpty(clients)) {
      throw new RuntimeException("Can't find client!");
    }
  }

  private void initClient(final String zkAddress) {
    final ZkClient zkClient = new ZkClient(zkAddress);
    zkClient.waitUntilConnected();

    List<String> clientNodes = zkClient.getChildren(ZK_PATH);
    final Map<String,Map.Entry<String,KVService.ServiceIface>> clients = new ConcurrentHashMap<>();
    zkClient.subscribeChildChanges(ZK_PATH, new ZkEventListener(ZK_PATH, clientNodes) {
      @Override
      public void onChildChange(String parent, List<String> children, List<String> newAdded, List<String> deleted) {
        for (String node : newAdded) {
          String fullPath = FilenameUtils.separatorsToUnix(FilenameUtils.concat(parent, node));
          byte[] bytes = zkClient.readData(fullPath);
          ServiceInstance serviceInstance = JSONObject.parseObject(new String(bytes), ServiceInstance.class);
          String schema = String.format("%s:%s", serviceInstance.getServiceEndpoint().getHost(),
              serviceInstance.getServiceEndpoint().getPort());
          KVService.ServiceIface iface = Thrift.newIface(schema, KVService.ServiceIface.class);
          clients.put(node, new AbstractMap.SimpleEntry<>(schema, iface));
          LOG.info("Client node {} {} joined!", node, schema);
        }
        for (String node : deleted) {
          clients.remove(node);
          LOG.info("Client node {} left!", node);
        }

        // ensure the new node overrides the old node.
        List<String> sortedNodes = new ArrayList<>();
        for (String node : clients.keySet()) {
          sortedNodes.add(node);
        }
        Collections.sort(sortedNodes, Collections.reverseOrder());

        Set<String> uniqueClients = new HashSet<>();
        for (String node : sortedNodes) {
          String schema = clients.get(node).getKey();
          if (uniqueClients.contains(schema)) {
            clients.remove(node);
            LOG.warn("Client node {} {} duplicate, removed!", node, schema);
          } else {
            uniqueClients.add(schema);
          }
        }

        for (String node : clients.keySet()) {
          LOG.info("Client node {} {} on service!", node, clients.get(node).getKey());
        }
        ShardKVClient.this.clients = new ArrayList<>(clients.values());
      }
    });
  }

  private Map.Entry<String,KVService.ServiceIface> getClient() {
    return clients.get(random.nextInt(clients.size()));
  }

  public String get(String key) throws Exception {
    long start = System.currentTimeMillis();
    Map.Entry<String,KVService.ServiceIface> client = getClient();
    Future<String> future = client.getValue().get(key);
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
        LOG.warn("Slow add request to {}, cost: {}ms", client.getKey(), elapse);
      }
    }
  }

  public boolean put(String key, String value) throws Exception {
    long start = System.currentTimeMillis();
    Map.Entry<String,KVService.ServiceIface> client = getClient();
    Future<Boolean> future = client.getValue().put(key, value);
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
        LOG.warn("Slow add request to {}, cost: {}ms", client.getKey(), elapse);
      }
    }
  }

  public void close() {
    if (clients != null) {
      clients.clear();
    }
  }
}
