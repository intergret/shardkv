package com.code.labs.shardkv.server;

import java.net.InetSocketAddress;

import com.code.labs.shardkv.common.Config;
import com.code.labs.shardkv.common.SystemUtil;
import com.code.labs.shardkv.common.zk.ShardKVAnnouncer;
import com.google.common.base.Throwables;
import com.twitter.finagle.Announcement;
import com.twitter.finagle.ListeningServer;
import com.twitter.finagle.Thrift;
import com.twitter.util.Await;
import com.twitter.util.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShardKVServer {

  private static final Logger LOG = LoggerFactory.getLogger(ShardKVServer.class);
  private int shardId;
  private int port;

  private KVServerImpl kvService;
  private ListeningServer listeningServer;
  private Future<Announcement> clusterStatus;

  public ShardKVServer(int shardId) {
    this.shardId = shardId;
    this.port = SystemUtil.getAvailablePort();
  }

  public void start() {
    try {
      String zkPath = String.format(Config.ZK_SERVER_PATH, shardId);
      kvService = new KVServerImpl();
      listeningServer = Thrift.serveIface(new InetSocketAddress(port), kvService);
      ShardKVAnnouncer zkAnnouncer = new ShardKVAnnouncer();
      clusterStatus = zkAnnouncer.announce(Config.ZK, zkPath, port);
      LOG.error("Server start on zk:{}, path:{}, port:{}", Config.ZK, zkPath, port);

      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          close();
        }
      });
      Await.ready(listeningServer);
    } catch (Exception e) {
      LOG.error("Start Server failed : {}", Throwables.getStackTraceAsString(e));
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
    if (kvService != null) {
      kvService.close();
    }
    if (listeningServer != null) {
      listeningServer.close();
    }
    LOG.info("Server shutdown.");
  }

  public static void main(String[] args) {
    if (args.length != 1) {
      System.out.println("Usage: ShardKVServer <ShardId>");
      System.out.println("Example: ShardKVServer 1");
      return;
    }

    int shardId = Integer.valueOf(args[0]);
    ShardKVServer server = null;
    try {
      server = new ShardKVServer(shardId);
      server.start();
    } catch (Exception e) {
      LOG.error("Server start failed : {}", Throwables.getStackTraceAsString(e));
      if (server != null) {
        server.close();
      }
    }
  }
}