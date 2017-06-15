package com.code.labs.shardkv.common.zk;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import com.twitter.finagle.Announcement;
import com.twitter.finagle.zookeeper.ZkAnnouncer;
import com.twitter.finagle.zookeeper.ZkClientFactory;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import scala.Option;

public class ShardKVAnnouncer {

  private ZkAnnouncer zkAnnouncer;

  public ShardKVAnnouncer() {
    ZkClientFactory zkClientFactory = new ZkClientFactory(Duration.apply(30, TimeUnit.SECONDS));
    zkAnnouncer = new ZkAnnouncer(zkClientFactory);
  }

  public Future<Announcement> announce(String zk, String path, int port) {
    // set socket address to 127.0.0.1 for local debug
    // use new InetSocketAddress(SystemUtil.getHostAddress(), port) instead for online
    return zkAnnouncer.announce(zk, path, 0, new InetSocketAddress("127.0.0.1", port), Option.<String> empty());
  }
}
