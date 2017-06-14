package com.code.labs.shardkv.proxy;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.code.labs.shardkv.GetResponse;
import com.code.labs.shardkv.KVServer;
import com.code.labs.shardkv.PutResponse;
import com.code.labs.shardkv.common.Config;
import com.code.labs.shardkv.common.Role;
import com.code.labs.shardkv.common.zk.ZkEventListener;
import com.github.zkclient.ZkClient;
import com.twitter.finagle.Thrift;
import com.twitter.thrift.ServiceInstance;
import com.twitter.util.Await;
import com.twitter.util.Future;
import com.twitter.util.FutureTransformer;

public class ShardClient {

  private static final Logger LOG = LoggerFactory.getLogger(ShardClient.class);
  private volatile List<Map.Entry<String,KVServer.ServiceIface>> masterList;
  private volatile List<Map.Entry<String,KVServer.ServiceIface>> slaveList;

  private int shardId;

  public ShardClient(int shardId) {
    this.shardId = shardId;

    ZkClient zkClient = new ZkClient(Config.ZK);
    zkClient.waitUntilConnected();
    initMasterView(zkClient);
    initSlaveView(zkClient);
  }

  private void initMasterView(final ZkClient zkClient) {
    String zkMasterPath = String.format(Config.ZK_SERVER_PATH, shardId, Role.MASTER.name().toLowerCase());
    List<String> clientNodes = zkClient.getChildren(zkMasterPath);
    final Map<String,Map.Entry<String,KVServer.ServiceIface>> nodes = new ConcurrentHashMap<>();
    zkClient.subscribeChildChanges(zkMasterPath, new ZkEventListener(zkMasterPath, clientNodes) {
      @Override
      public void onChildChange(String parent, List<String> children, List<String> newAdded, List<String> deleted) {
        for (String node : newAdded) {
          String fullPath = FilenameUtils.separatorsToUnix(FilenameUtils.concat(parent, node));
          String instanceInfo = new String(zkClient.readData(fullPath));
          ServiceInstance serviceInstance = JSONObject.parseObject(instanceInfo, ServiceInstance.class);
          String schema = String.format("%s:%s", serviceInstance.getServiceEndpoint().getHost(),
              serviceInstance.getServiceEndpoint().getPort());
          KVServer.ServiceIface iface = Thrift.newIface(schema, KVServer.ServiceIface.class);
          nodes.put(node, new AbstractMap.SimpleEntry<>(schema, iface));
          LOG.info("Shard {} Master node {} {} joined!", shardId, node, schema);
        }
        for (String node : deleted) {
          nodes.remove(node);
          LOG.info("Shard {} Master node {} left!", shardId, node);
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
            LOG.warn("Shard {} Master node {} {} duplicate, removed!", shardId, node, schema);
          } else {
            uniqueClients.add(schema);
          }
        }

        for (String node : nodes.keySet()) {
          LOG.info("Shard {} Master node {} {} on service!", shardId, node, nodes.get(node).getKey());
        }
        masterList = new ArrayList<>(nodes.values());
      }
    });

    if (CollectionUtils.isEmpty(masterList)) {
      throw new RuntimeException("Can't find Master in Shard " + shardId + "!");
    }
    if (masterList.size() != 1) {
      throw new RuntimeException("Find multi Master in Shard " + shardId + "!");
    }
  }

  private void initSlaveView(final ZkClient zkClient) {
    String zkSlavePath = String.format(Config.ZK_SERVER_PATH, shardId, Role.SLAVE.name().toLowerCase());
    List<String> clientNodes = zkClient.getChildren(zkSlavePath);
    final Map<String,Map.Entry<String,KVServer.ServiceIface>> nodes = new ConcurrentHashMap<>();
    zkClient.subscribeChildChanges(zkSlavePath, new ZkEventListener(zkSlavePath, clientNodes) {
      @Override
      public void onChildChange(String parent, List<String> children, List<String> newAdded, List<String> deleted) {
        for (String node : newAdded) {
          String fullPath = FilenameUtils.separatorsToUnix(FilenameUtils.concat(parent, node));
          String instanceInfo = new String(zkClient.readData(fullPath));
          ServiceInstance serviceInstance = JSONObject.parseObject(instanceInfo, ServiceInstance.class);
          String schema = String.format("%s:%s", serviceInstance.getServiceEndpoint().getHost(),
              serviceInstance.getServiceEndpoint().getPort());
          KVServer.ServiceIface iface = Thrift.newIface(schema, KVServer.ServiceIface.class);
          nodes.put(node, new AbstractMap.SimpleEntry<>(schema, iface));
          LOG.info("Shard {} Slave node {} {} joined!", shardId, node, schema);
        }
        for (String node : deleted) {
          nodes.remove(node);
          LOG.info("Shard {} Slave node {} left!", shardId, node);
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
            LOG.warn("Shard {} Slave node {} {} duplicate, removed!", shardId, node, schema);
          } else {
            uniqueClients.add(schema);
          }
        }

        for (String node : nodes.keySet()) {
          LOG.info("Shard {} Slave node {} {} on service!", shardId, node, nodes.get(node).getKey());
        }
        slaveList = new ArrayList<>(nodes.values());
      }
    });

    if (CollectionUtils.isEmpty(slaveList)) {
      throw new RuntimeException("Can't find Slave in Shard " + shardId + "!");
    }
  }

  public GetResponse read(String key) throws Exception {
    long start = System.currentTimeMillis();
    Map.Entry<String,KVServer.ServiceIface> master = masterList.get(0);
    Future<GetResponse> future = master.getValue().get(key);
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
        LOG.warn("Slow request to {}, cost: {}ms", master.getKey(), elapse);
      }
    }
  }

  private FutureTransformer<PutResponse,Boolean> futureTransformer = new FutureTransformer<PutResponse,Boolean>() {
    @Override
    public Boolean map(PutResponse response) {
      return response.success;
    }

    @Override
    public Boolean handle(Throwable t) {
      return false;
    }
  };

  public PutResponse write(String key, String value) throws Exception {
    List<Future<Boolean>> futures = new ArrayList<>();
    Map.Entry<String,KVServer.ServiceIface> master = masterList.get(0);
    futures.add(master.getValue().put(key, value).transformedBy(futureTransformer));
    for (final Map.Entry<String,KVServer.ServiceIface> slave : slaveList) {
      futures.add(slave.getValue().put(key, value).transformedBy(futureTransformer));
    }

    Future<List<Boolean>> collected = Future.collect(futures);
    try {
      int successCount = 0;
      List<Boolean> resultList = Await.result(collected);
      for (boolean success : resultList) {
        if (success) {
          successCount += 1;
        }
      }
      boolean finalSuccess = successCount == resultList.size();

      PutResponse putResponse = new PutResponse(finalSuccess);
      if (!finalSuccess) {
        putResponse.setMsg("Write value to both master and slave failed!");
      }
      return putResponse;
    } catch (Exception e) {
      return new PutResponse(false).setMsg(e.toString());
    }
  }
}
