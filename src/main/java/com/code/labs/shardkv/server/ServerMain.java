package com.code.labs.shardkv.server;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.twitter.finagle.Announcement;
import com.twitter.finagle.ListeningServer;
import com.twitter.finagle.Thrift;
import com.twitter.util.Await;
import com.twitter.util.Future;

public class ServerMain {

  private static final Logger LOG = LoggerFactory.getLogger(ServerMain.class);
  public static String ZK = "127.0.0.1:2181";
  public static String ZK_PATH = "/kvservice/shard/%s";

  private int shardId;
  private int port;
  private KVServiceImpl kvService;
  private ListeningServer listeningServer;
  private Future<Announcement> clusterStatus;

  public ServerMain(int shardId, int port) {
    this.shardId = shardId;
    this.port = port;
  }

  public void startServer() {
    try {
      kvService = new KVServiceImpl(shardId);
      listeningServer = Thrift.serveIface(new InetSocketAddress(port), kvService);
      ServerAnnouncer zkAnnouncer = new ServerAnnouncer();
      String zkPath = String.format(ZK_PATH, shardId);
      clusterStatus = zkAnnouncer.announce(ZK, zkPath, port);
      System.out.println("Server start on zk:" + ZK + ", path:" + zkPath + ", port:" + port);

      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          close();
        }
      });
      Await.ready(listeningServer);
    } catch (Exception e) {
      System.out.println(e);
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
    if (args.length != 2) {
      System.out.println("Usage: ServerMain <ShardId> <Port>");
      System.out.println("Example: ServerMain 0 8091");
      return;
    }

    int shardId = Integer.valueOf(args[0]);
    int port = Integer.valueOf(args[1]);

    ServerMain server = null;
    try {
      server = new ServerMain(shardId, port);
      server.startServer();
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error("Server start failed : {}", Throwables.getStackTraceAsString(e));
      if (server != null) {
        server.close();
      }
    }
  }
}