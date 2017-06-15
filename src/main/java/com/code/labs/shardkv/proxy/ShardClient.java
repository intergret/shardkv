package com.code.labs.shardkv.proxy;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import com.alibaba.fastjson.JSONObject;
import com.code.labs.shardkv.GetResponse;
import com.code.labs.shardkv.KVServer;
import com.code.labs.shardkv.PutResponse;
import com.code.labs.shardkv.common.Config;
import com.code.labs.shardkv.common.zk.ZkEventListener;
import com.github.zkclient.ZkClient;
import com.twitter.finagle.Thrift;
import com.twitter.thrift.ServiceInstance;
import com.twitter.util.Await;
import com.twitter.util.Future;
import com.twitter.util.FutureTransformer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShardClient {

  private static final Logger LOG = LoggerFactory.getLogger(ShardClient.class);
  private volatile List<Map.Entry<String,KVServer.ServiceIface>> replicaList;

  private int shardId;

  public ShardClient(int shardId) {
    this.shardId = shardId;
    initShardClient();
  }

  private void initShardClient() {
    final ZkClient zkClient = new ZkClient(Config.ZK);
    zkClient.waitUntilConnected();

    String zkSlavePath = String.format(Config.ZK_SERVER_PATH, shardId);
    List<String> clientNodes = zkClient.getChildren(zkSlavePath);
    final Map<String,Map.Entry<String,KVServer.ServiceIface>> replicas = new ConcurrentHashMap<>();
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
          replicas.put(node, new AbstractMap.SimpleEntry<>(schema, iface));
          LOG.info("Shard {} replica {} {} joined!", shardId, node, schema);
        }
        for (String node : deleted) {
          replicas.remove(node);
          LOG.info("Shard {} replica {} left!", shardId, node);
        }

        // ensure the new node overrides the old node.
        List<String> sortedReplica = new ArrayList<>();
        for (String replica : replicas.keySet()) {
          sortedReplica.add(replica);
        }
        Collections.sort(sortedReplica, Collections.reverseOrder());

        Set<String> uniqueClients = new HashSet<>();
        for (String replica : sortedReplica) {
          String schema = replicas.get(replica).getKey();
          if (uniqueClients.contains(schema)) {
            replicas.remove(replica);
            LOG.warn("Shard {} replica {} {} duplicate, removed!", shardId, replica, schema);
          } else {
            uniqueClients.add(schema);
          }
        }

        for (String replica : replicas.keySet()) {
          LOG.info("Shard {} replica {} {} on service!", shardId, replica, replicas.get(replica).getKey());
        }
        replicaList = new ArrayList<>(replicas.values());
      }
    });

    if (CollectionUtils.isEmpty(replicaList)) {
      throw new RuntimeException("No replica in Shard " + shardId + "!");
    }
  }

  public Map.Entry<String,KVServer.ServiceIface> getReplica() {
    return replicaList.get(ThreadLocalRandom.current().nextInt(replicaList.size()));
  }

  public GetResponse read(String key) throws Exception {
    long start = System.currentTimeMillis();
    Map.Entry<String,KVServer.ServiceIface> replica = getReplica();
    Future<GetResponse> future = replica.getValue().get(key);
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
        LOG.warn("Slow request to {}, cost: {}ms", replica.getKey(), elapse);
      }
    }
  }

  public PutResponse write(String key, String value) throws Exception {
    List<Future<Boolean>> futures = new ArrayList<>();
    for (final Map.Entry<String,KVServer.ServiceIface> replica : replicaList) {
      futures.add(replica.getValue().put(key, value).transformedBy(new FutureTransformer<PutResponse,Boolean>() {
        @Override
        public Boolean map(PutResponse response) {
          return response.success;
        }

        @Override
        public Boolean handle(Throwable t) {
          return false;
        }
      }));
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
        putResponse.setMsg("Write value to all replicas failed!");
      }
      return putResponse;
    } catch (Exception e) {
      return new PutResponse(false).setMsg(e.toString());
    }
  }
}
