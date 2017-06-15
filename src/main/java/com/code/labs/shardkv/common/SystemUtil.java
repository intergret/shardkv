package com.code.labs.shardkv.common;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketException;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

public class SystemUtil {

  private static final Logger LOG = LoggerFactory.getLogger(SystemUtil.class);

  public static String getHostAddress() {
    try {
      for (NetworkInterface iface : Collections.list(NetworkInterface.getNetworkInterfaces())) {
        if (!iface.getName().startsWith("vmnet") && !iface.getName().startsWith("docker")) {
          for (InetAddress raddr : Collections.list(iface.getInetAddresses())) {
            if (raddr.isSiteLocalAddress() && !raddr.isLoopbackAddress() && !(raddr instanceof Inet6Address)) {
              return raddr.getHostAddress();
            }
          }
        }
      }
    } catch (SocketException e) {}
    throw new IllegalStateException("Couldn't find the local ip address!");
  }

  public static int getAvailablePort() {
    ServerSocket serverSocket = null;
    try {
      serverSocket = new ServerSocket(0);
      return serverSocket.getLocalPort();
    } catch (IOException e) {} finally {
      if (serverSocket != null) {
        try {
          serverSocket.close();
        } catch (IOException e) {
          LOG.info("Get available port exception {}", Throwables.getStackTraceAsString(e));
        }
      }
    }
    throw new RuntimeException("No available port found!");
  }
}
