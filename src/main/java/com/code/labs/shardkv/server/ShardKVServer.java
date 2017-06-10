package com.code.labs.shardkv.server;

import java.net.InetSocketAddress;

import com.code.labs.shardkv.common.Role;
import com.code.labs.shardkv.common.zk.ZKConfig;
import com.code.labs.shardkv.common.zk.ShardKVAnnouncer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.twitter.finagle.Announcement;
import com.twitter.finagle.ListeningServer;
import com.twitter.finagle.Thrift;
import com.twitter.util.Await;
import com.twitter.util.Future;

public class ShardKVServer {

  private static final Logger LOG = LoggerFactory.getLogger(ShardKVServer.class);
  private int shardId;
  private Role role;
  private int port;

  private KVServerImpl kvService;
  private ListeningServer listeningServer;
  private Future<Announcement> clusterStatus;

  public ShardKVServer(int shardId, Role role, int port) {
    this.shardId = shardId;
    this.role = role;
    this.port = port;
  }

  public void start() {
    try {
      kvService = new KVServerImpl(shardId);
      listeningServer = Thrift.serveIface(new InetSocketAddress(port), kvService);
      ShardKVAnnouncer zkAnnouncer = new ShardKVAnnouncer();
      String zkPath = String.format(ZKConfig.SERVER_PATH, shardId, role.name().toLowerCase());
      clusterStatus = zkAnnouncer.announce(ZKConfig.DEBUG, zkPath, port);
      LOG.error("Server start on zk:{}, path:{}, port:{}", ZKConfig.DEBUG, zkPath, port);

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
    if (kvService != null) {
      kvService.close();
    }
    if (listeningServer != null) {
      listeningServer.close();
    }
    LOG.info("Server shutdown.");
  }

  public static void main(String[] args) {
    if (args.length != 3) {
      System.out.println("Usage: ShardKVServer <ShardId> <Role> <Port>");
      System.out.println("Example: ShardKVServer 0 master 8091");
      return;
    }

    int shardId = Integer.valueOf(args[0]);
    Role role = Role.valueOf(args[1].toUpperCase());
    int port = Integer.valueOf(args[2]);

    ShardKVServer server = null;
    try {
      server = new ShardKVServer(shardId, role, port);
      server.start();
    } catch (Exception e) {
      LOG.error("Server start failed : {}", Throwables.getStackTraceAsString(e));
      if (server != null) {
        server.close();
      }
    }
  }
}