package com.code.labs.shardkv.common;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketException;
import java.util.Collections;

public class SystemUtil {

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
    try (ServerSocket ss = new ServerSocket(0)) {
      ss.setReuseAddress(true);
      return ss.getLocalPort();
    } catch (IOException e) {}
    throw new RuntimeException("No available port found!");
  }
}
