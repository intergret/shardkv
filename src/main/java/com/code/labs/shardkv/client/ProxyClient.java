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
import com.code.labs.shardkv.common.Config;
import com.code.labs.shardkv.common.zk.ZkEventListener;
import com.github.zkclient.ZkClient;
import com.twitter.finagle.Thrift;
import com.twitter.thrift.ServiceInstance;

public class ProxyClient {

  private static final Logger LOG = LoggerFactory.getLogger(ProxyClient.class);
  private volatile List<Map.Entry<String,KVProxy.ServiceIface>> proxyList;

  public ProxyClient(final String zkAddress) {
    final ZkClient zkClient = new ZkClient(zkAddress);
    zkClient.waitUntilConnected();

    List<String> clientNodes = zkClient.getChildren(Config.ZK_PROXY_PATH);
    final Map<String,Map.Entry<String,KVProxy.ServiceIface>> nodes = new ConcurrentHashMap<>();
    zkClient.subscribeChildChanges(Config.ZK_PROXY_PATH, new ZkEventListener(Config.ZK_PROXY_PATH, clientNodes) {
      @Override
      public void onChildChange(String parent, List<String> children, List<String> newAdded, List<String> deleted) {
        for (String node : newAdded) {
          String fullPath = FilenameUtils.separatorsToUnix(FilenameUtils.concat(parent, node));
          String instanceInfo = new String(zkClient.readData(fullPath));
          ServiceInstance serviceInstance = JSONObject.parseObject(instanceInfo, ServiceInstance.class);
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
        proxyList = new ArrayList<>(nodes.values());
      }
    });

    if (CollectionUtils.isEmpty(proxyList)) {
      throw new RuntimeException("Can't find Proxy!");
    }
  }

  public Map.Entry<String,KVProxy.ServiceIface> getProxy() {
    return proxyList.get(ThreadLocalRandom.current().nextInt(proxyList.size()));
  }

  public void close() {
    if (proxyList != null) {
      proxyList.clear();
    }
  }
}
