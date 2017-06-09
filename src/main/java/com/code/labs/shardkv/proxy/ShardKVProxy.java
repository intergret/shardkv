package com.code.labs.shardkv.proxy;

import java.net.InetSocketAddress;

import com.code.labs.shardkv.common.zk.ZKConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.code.labs.shardkv.common.zk.ShardKVAnnouncer;
import com.google.common.base.Throwables;
import com.twitter.finagle.Announcement;
import com.twitter.finagle.ListeningServer;
import com.twitter.finagle.Thrift;
import com.twitter.util.Await;
import com.twitter.util.Future;

public class ShardKVProxy {

  private static final Logger LOG = LoggerFactory.getLogger(ShardKVProxy.class);
  private int port;
  private KVProxyImpl kvProxy;
  private ListeningServer listeningServer;
  private Future<Announcement> clusterStatus;

  public ShardKVProxy(int port) {
    this.port = port;
  }

  public void start() {
    try {
      kvProxy = new KVProxyImpl();
      listeningServer = Thrift.serveIface(new InetSocketAddress(port), kvProxy);
      ShardKVAnnouncer zkAnnouncer = new ShardKVAnnouncer();
      clusterStatus = zkAnnouncer.announce(ZKConfig.DEBUG, ZKConfig.PROXY_PATH, port);
      LOG.error("Proxy start on zk:{}, path:{}, port:{}", ZKConfig.DEBUG, ZKConfig.PROXY_PATH, port);

      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          close();
        }
      });
      Await.ready(listeningServer);
    } catch (Exception e) {
      LOG.error("Start listeningServer failed : {}", Throwables.getStackTraceAsString(e));
      close();
    }
  }

  public void close() {
    if (clusterStatus != null) {
      try {
        Await.result(clusterStatus).unannounce();
      } catch (Exception e) {
        LOG.error("{}", Throwables.getStackTraceAsString(e));
      }
    }
    if (kvProxy != null) {
      kvProxy.close();
    }
    if (listeningServer != null) {
      listeningServer.close();
    }
    LOG.info("Proxy shutdown.");
  }

  public static void main(String[] args) {
    if (args.length != 1) {
      System.out.println("Usage: ShardKVProxy <Port>");
      System.out.println("Example: ShardKVProxy 9091");
      return;
    }

    int port = Integer.valueOf(args[0]);
    ShardKVProxy proxy = null;
    try {
      proxy = new ShardKVProxy(port);
      proxy.start();
    } catch (Exception e) {
      LOG.error("Proxy start exception : {}", Throwables.getStackTraceAsString(e));
      if (proxy != null) {
        proxy.close();
      }
    }
  }
}
