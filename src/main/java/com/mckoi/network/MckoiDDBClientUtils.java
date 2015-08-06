/*
 * Mckoi Software ( http://www.mckoi.com/ )
 * Copyright (C) 2000 - 2015  Diehl and Associates, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mckoi.network;

import com.mckoi.util.GeneralParser;
import java.io.*;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URL;
import java.net.URLConnection;
import java.text.MessageFormat;
import java.util.Properties;

/**
 * Static connection utility methods for creating MckoiDDBClient instances
 * using various ways to specify the properties needed to connect to the
 * network.
 *
 * @author Tobias Downer
 */

public class MckoiDDBClientUtils {

  /**
   * Default node heap size.
   */
  private static final long DEFAULT_NODE_CACHE_SIZE = 14 * 1024 * 1024;

  /**
   * Default JVM cache configuration.
   */
  private static final CacheConfiguration DEFAULT_CACHE_CONFIG =
                                                      new CacheConfiguration();

  // ---- Regular TCP connection to a MckoiDDB network ----

  /**
   * Creates a standard direct client connection over a TCP network.
   *
   * @param manager_servers the address of the manager servers on the network.
   * @param network_password the network password assigned the network.
   * @param output_net_interface the interface for outgoing communication
   *   (for IPv6)
   * @param introduced_latency a latency in ms artifically added to each
   *   communication for latency testing purposes.
   * @param lnc the LocalNetworkCache used to cache information locally.
   * @param max_transaction_node_heap_size the maximum size of the node heap
   *   for each individual transaction's writes.
   * @return 
   */
  public static MckoiDDBClient connectTCP(
                   ServiceAddress[] manager_servers, String network_password,
                   NetworkInterface output_net_interface,
                   int introduced_latency, LocalNetworkCache lnc,
                   long max_transaction_node_heap_size) {
    TCPMckoiDDBClient client =
        new TCPMckoiDDBClient(manager_servers, network_password,
                              output_net_interface,
                              introduced_latency, lnc,
                              max_transaction_node_heap_size);
    client.connect();
    return client;
  }

  /**
   * Creates a standard direct client connection over a TCP network.
   *
   * @param manager_servers the address of the manager server on the network.
   * @param network_password the network password assigned the network.
   * @param output_net_interface the interface for outgoing communication
   *   (for IPv6)
   * @param introduced_latency a latency in ms artifically added to each
   *   communication for latency testing purposes.
   * @param lnc the LocalNetworkCache used to cache information locally.
   * @return 
   */
  public static MckoiDDBClient connectTCP(
                   ServiceAddress[] manager_servers, String network_password,
                   NetworkInterface output_net_interface,
                   int introduced_latency, LocalNetworkCache lnc) {
    return connectTCP(manager_servers, network_password,
                      output_net_interface,
                      introduced_latency, lnc,
                      DEFAULT_NODE_CACHE_SIZE);
  }

  /**
   * Creates a standard direct client connection over a TCP network with a
   * local JVM heap cache.
   *
   * @param manager_servers the address of the manager servers on the network
   *   ordered by access priority.
   * @param network_password the network password assigned the network.
   * @param output_net_interface the interface for outgoing communication
   *   (for IPv6)
   * @param introduced_latency a latency in ms artifically added to each
   *   communication for latency testing purposes.
   * @return 
   */
  public static MckoiDDBClient connectTCP(
              ServiceAddress[] manager_servers,
              String network_password, NetworkInterface output_net_interface,
              int introduced_latency) {
    return connectTCP(
        manager_servers, network_password, output_net_interface,
        introduced_latency,
        JVMState.getJVMCacheForManager(manager_servers, DEFAULT_CACHE_CONFIG));
  }

  /**
   * Creates a standard direct client connection over a TCP network with a
   * local JVM heap cache.
   *
   * @param manager_servers the address of the manager servers on the network
   *   ordered by access priority.
   * @param network_password the network password assigned the network.
   * @param output_net_interface the interface for outgoing communication
   *   (for IPv6)
   * @return 
   */
  public static MckoiDDBClient connectTCP(
                  ServiceAddress[] manager_servers, String network_password,
                  NetworkInterface output_net_interface) {
    return connectTCP(manager_servers, network_password, output_net_interface, 0);
  }

  /**
   * Creates a TCPMckoiDDBClient object based on a Properties object
   * that contains two keys - 'manager_address', and 'network_password'.
   * 'manager_address' is a ServiceAddress string of the manager server in the
   * network (eg. 'mymachine.com:3900'). 'network_password' is the challenge
   * password string needed to talk with the machines on the network.
   * 
   * @param p
   * @return 
   * @throws java.io.IOException
   */
  public static MckoiDDBClient connectTCP(Properties p) throws IOException {

    String manager_addr = p.getProperty("manager_address");
    String net_password = p.getProperty("network_password");
    if (manager_addr == null) {
      throw new RuntimeException("'manager_address' property not found.");
    }
    if (net_password == null) {
      throw new RuntimeException("'network_password' property not found.");
    }

    // The network interface for IPv6 loop-link,
    String out_net_interface = p.getProperty("net_interface");
    NetworkInterface out_net_if = null;
    if (out_net_interface != null) {
      // Turn it into a NetworkInterface
      out_net_if = NetworkInterface.getByName(out_net_interface);
      if (out_net_if == null) {
        String err_msg = MessageFormat.format(
            "'net_interface' property is not a valid network interface: {0}",
            out_net_interface);
        throw new RuntimeException(err_msg);
      }
    }
    else {
      // PENDING: Put this warning into a log rather than print to console?
      System.out.println("WARNING: no 'net_interface' property in client.conf.");
      System.out.println("  This means link-local IPv6 will not work.");
    }

    // Get the type of connection,
    String connect_type = p.getProperty("connect_type", "direct");

    String introduced_latency_str =
            p.getProperty("introduced_latency", "0").trim();
    String transaction_node_cache_str =
            p.getProperty("transaction_cache_size", "14MB").trim();
    String global_node_cache_str =
            p.getProperty("global_cache_size", "32MB").trim();

    int introduced_latency;
    long transaction_node_cache;
    long global_node_cache;

    // NOTE: This value is for testing purposes to simulate high latency
    //   network conditions.
    try {
      introduced_latency = Integer.parseInt(introduced_latency_str);
    }
    catch (NumberFormatException e) {
      throw new RuntimeException(
        "'introduced_latency' property invalid in client configuration.", e);
    }
    // Transaction cache,
    try {
      transaction_node_cache =
            GeneralParser.parseSizeByteFormat(transaction_node_cache_str);
    }
    catch (NumberFormatException e) {
      throw new RuntimeException(
        "'transaction_cache_size' property invalid in client configuration.", e);
    }
    // Global cache,
    try {
      global_node_cache =
            GeneralParser.parseSizeByteFormat(global_node_cache_str);
    }
    catch (NumberFormatException e) {
      throw new RuntimeException(
        "'global_cache_size' property invalid in client configuration.", e);
    }

    CacheConfiguration cache_config = new CacheConfiguration();
    cache_config.setGlobalNodeCacheSize(global_node_cache);

    // Parse the manager servers list,
    ServiceAddress[] manager_servers;
    try {
      manager_servers = ServiceAddress.parseServiceAddresses(manager_addr);
      if (manager_servers.length == 0) {
        throw new RuntimeException(
                "'manager_address' property invalid in client configuration.");
      }
    }
    catch (IOException e) {
      throw new RuntimeException(
        "'manager_address' property invalid in client configuration.", e);
    }
    catch (NumberFormatException e) {
      throw new RuntimeException(
        "'manager_address' property invalid in client configuration.", e);
    }

    // Create the network cache for this connection,
    LocalNetworkCache net_cache =
               JVMState.getJVMCacheForManager(manager_servers, cache_config);

    // Direct client connect
    if (connect_type.equals("direct")) {

      return connectTCP(manager_servers, net_password, out_net_if,
                        introduced_latency, net_cache, transaction_node_cache);

    }

    // Connection to network via proxy
    else if (connect_type.equals("proxy")) {
      String proxy_host = p.getProperty("proxy_host");
      String proxy_port = p.getProperty("proxy_port");

      if (proxy_host == null) {
        throw new RuntimeException("'proxy_host' property not found.");
      }
      if (proxy_port == null) {
        throw new RuntimeException("'proxy_port' property not found.");
      }

      // Try and parse the port,
      int pport;
      try {
        pport = Integer.parseInt(proxy_port);
      }
      catch (NumberFormatException e) {
        throw new RuntimeException(
          "'proxy_port' property invalid in client configuration.", e);
      }

      InetAddress phost;
      try {
        phost = InetAddress.getByName(proxy_host);
      }
      catch (IOException e) {
        throw new RuntimeException(
          "'proxy_host' property invalid in client configuration.", e);
      }

      return connectProxyTCP(phost, pport,
                         manager_servers, net_password, introduced_latency,
                         net_cache, transaction_node_cache);

    }

    else {
      throw new RuntimeException("Unknown proxy type: " + connect_type);
    }

  }

  /**
   * Creates a TCPMckoiDDBClient object based on an InputStream
   * that contains two keys ('manager_address', and 'network_password')
   * formatted as a Properties file and used to build the connection.
   */
  public static MckoiDDBClient connectTCP(InputStream is) throws IOException {
    Properties p = new Properties();
    p.load(new BufferedInputStream(is));
    return connectTCP(p);
  }

  /**
   * Creates a TCPMckoiDDBClient object based on a URL object
   * that locates a resource that contains two keys used to build the
   * connection - 'manager_address', and 'network_password'.
   */
  public static MckoiDDBClient connectTCP(URL url) throws IOException {
    URLConnection c = url.openConnection();
    InputStream is = c.getInputStream();
    MckoiDDBClient client = connectTCP(is);
    is.close();
    return client;
  }

  /**
   * Creates a TCPMckoiDDBClient object based on a File object
   * that locates a resource that contains two keys used to build the
   * connection - 'manager_address', and 'network_password'.
   */
  public static MckoiDDBClient connectTCP(File file) throws IOException {
    FileInputStream fin = new FileInputStream(file);
    MckoiDDBClient client = connectTCP(fin);
    fin.close();
    return client;
  }

  // ------ Proxy connection to a MckoiDDB network

  /**
   * Creates a connection to a MckoiDDB network via a proxy service over a TCP
   * network.
   *
   * @param proxy_address the InetAddress of the proxy server.
   * @param proxy_port the port of the proxy server.
   * @param manager_servers the address of the manager servers on the network
   *   ordered by access priority.
   * @param network_password the network password assigned the network.
   * @param lnc the LocalNetworkCache used to cache information locally.
   * @param max_transaction_node_heap_size the maximum size of the transaction
   *   node heap.
   */
  public static MckoiDDBClient connectProxyTCP(
                     InetAddress proxy_address, int proxy_port,
                     ServiceAddress[] manager_servers, String network_password,
                     int introduced_latency,
                     LocalNetworkCache lnc,
                     long max_transaction_node_heap_size) {

    TCPProxyMckoiDDBClient proxy_client =
            new TCPProxyMckoiDDBClient(proxy_address, proxy_port,
                                       manager_servers, network_password,
                                       introduced_latency, lnc,
                                       max_transaction_node_heap_size);
    proxy_client.connect();
    return proxy_client;
  }

  /**
   * Creates a connection to a MckoiDDB network via a proxy service over a TCP
   * network.
   *
   * @param proxy_address the InetAddress of the proxy server.
   * @param proxy_port the port of the proxy server.
   * @param manager_servers the address of the manager servers on the network
   *   ordered by access priority.
   * @param network_password the network password assigned the network.
   * @param lnc the LocalNetworkCache used to cache information locally.
   */
  public static MckoiDDBClient connectProxyTCP(
                     InetAddress proxy_address, int proxy_port,
                     ServiceAddress[] manager_servers, String network_password,
                     int introduced_latency,
                     LocalNetworkCache lnc) {

    return connectProxyTCP(proxy_address, proxy_port, manager_servers,
                           network_password, introduced_latency, lnc,
                           DEFAULT_NODE_CACHE_SIZE);
  }

  /**
   * Creates a connection to a MckoiDDB network via a proxy service over a TCP
   * network, using the local JVM heap cache.
   *
   * @param proxy_address the InetAddress of the proxy server.
   * @param proxy_port the port of the proxy server.
   * @param manager_servers the address of the manager servers on the network
   *   ordered by access priority.
   * @param network_password the network password assigned the network.
   */
  public static MckoiDDBClient connectProxyTCP(
                     InetAddress proxy_address, int proxy_port,
                     ServiceAddress[] manager_servers, String network_password,
                     int introduced_latency) {
    return connectProxyTCP(
        proxy_address, proxy_port, manager_servers, network_password,
        introduced_latency,
        JVMState.getJVMCacheForManager(manager_servers, DEFAULT_CACHE_CONFIG));
  }

  // ----- Proxy connection via generic input/output stream

  public static MckoiDDBClient connectProxy(
                     InputStream in, OutputStream out,
                     ServiceAddress[] manager_servers, String network_password,
                     int introduced_latency, LocalNetworkCache lnc,
                     long max_transaction_node_heap_size) {

    ProxyMckoiDDBClient proxy_client =
            new ProxyMckoiDDBClient(in, out,
                                    manager_servers, network_password,
                                    introduced_latency, lnc,
                                    max_transaction_node_heap_size);
    proxy_client.connect();
    return proxy_client;
  }

  public static MckoiDDBClient connectProxy(
                     InputStream in, OutputStream out,
                     ServiceAddress[] manager_servers,
                     String network_password,
                     int introduced_latency,
                     long max_transaction_node_heap_size) {
    return connectProxy(
        in, out, manager_servers, network_password, introduced_latency,
        JVMState.getJVMCacheForManager(manager_servers, DEFAULT_CACHE_CONFIG),
        max_transaction_node_heap_size);
  }








  // ----- Inner classes



  /**
   * The client object used for interacting with a Mckoi distributed database
   * over a TCP network. This class employs a cache per transaction for
   * workspace related activity, set by using
   * 'setMaximumTransactionNodeCacheHeapSize'. This cache acts as a buffer for
   * storing modifications made to a transaction snapshot before being flushed
   * to the network.
   * <p>
   * It is intended that a single TCPMckoiDDBClient is instantiated per
   * JVM/network. A TCPMckoiDDBClient object is designed to service
   * sessions for any type of data model in a MckoiDDB network. While it is
   * safe for multiple TCPMckoiDDBClient objects to be created per JVM,
   * there are more opportunities for cache hits in an environment where this
   * client object is shared.
   */
  static class TCPMckoiDDBClient extends MckoiDDBClient {

    /**
     * The TCP connector used to talk with the network.
     */
    private TCPNetworkConnector tcp_connector;

    /**
     * An artifical latency component used for simulating network latency.
     */
    private final int artifical_latency;

    /**
     * Connector properties.
     */
    private final TCPConnectorValues properties;

    /**
     * Node cache constructor.
     */
    TCPMckoiDDBClient(ServiceAddress[] manager_servers,
                      String network_password,
                      NetworkInterface output_net_interface,
                      int artifical_latency,
                      LocalNetworkCache lnc,
                      long max_transaction_node_heap_size) {
      super(manager_servers, network_password, lnc,
            max_transaction_node_heap_size);

      properties = new TCPConnectorValues(
                                      network_password, output_net_interface);
      this.artifical_latency = artifical_latency;

    }

    /**
     * Connects this client to the network.
     */
    public void connect() {
      this.tcp_connector = new TCPNetworkConnector(properties);
      this.tcp_connector.setCommLatency(artifical_latency);
      super.connectNetwork(tcp_connector);
    }

  }





  static class TCPProxyMckoiDDBClient extends MckoiDDBClient {

    /**
     * The TCP proxy connector used to talk with the network.
     */
    private TCPProxyNetworkConnector proxy_connector;

    /**
     * The proxy InetAddress and port.
     */
    private final InetAddress proxy_host;
    private final int proxy_port;

    public TCPProxyMckoiDDBClient(InetAddress proxy_host, int proxy_port,
                                ServiceAddress[] manager_servers,
                                String network_password,
                                int artifical_latency,
                                LocalNetworkCache lnc,
                                long max_transaction_node_heap_size) {
      super(manager_servers, network_password,
            lnc, max_transaction_node_heap_size);
      // PENDING: Handle artifical_latency,
      this.proxy_host = proxy_host;
      this.proxy_port = proxy_port;
    }

    /**
     * Connects this client to the network.
     */
    public void connect() {
      this.proxy_connector = new TCPProxyNetworkConnector(getNetworkPassword());
      this.proxy_connector.connect(proxy_host, proxy_port);
      super.connectNetwork(proxy_connector);
    }

  }

  static class ProxyMckoiDDBClient extends MckoiDDBClient {

    /**
     * The proxy connector used to talk with the network.
     */
    private ProxyNetworkConnector proxy_connector;

    /**
     * The proxy input and output stream
     */
    private final InputStream in;
    private final OutputStream out;

    public ProxyMckoiDDBClient(InputStream in, OutputStream out,
                               ServiceAddress[] manager_servers,
                               String network_password,
                               int artifical_latency,
                               LocalNetworkCache lnc,
                               long max_transaction_node_heap_size) {
      super(manager_servers, network_password,
            lnc, max_transaction_node_heap_size);
      // PENDING: Handle artifical_latency,
      this.in = in;
      this.out = out;
    }

    /**
     * Connects this client to the network.
     */
    public void connect() {
      this.proxy_connector = new ProxyNetworkConnector(getNetworkPassword());
      this.proxy_connector.connect(in, out);
      super.connectNetwork(proxy_connector);
    }

  }

}
