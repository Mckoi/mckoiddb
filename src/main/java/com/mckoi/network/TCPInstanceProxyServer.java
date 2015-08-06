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

import java.io.*;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A proxy service that accepts message requests from a client connection and
 * dispatches them to the network as if the messages were from a local client.
 * This would typically be bound to localhost and connections would be piped
 * to it via a secure tunnel.
 *
 * @author Tobias Downer
 */

public class TCPInstanceProxyServer implements Runnable {

  /**
   * The network address this service is bound to.
   */
  private final InetAddress bind_interface;

  /**
   * The port on which proxy connections may be received.
   */
  private final int port;

  /**
   * The NetworkInterface upon which we use to connect to the Mckoi DDB network.
   */
  private final NetworkInterface out_network_interface;

  /**
   * The thread pool.
   */
  private final ExecutorService thread_pool;

  /**
   * The logger.
   */
  private final Logger log;

  /**
   * Constructs the instance server.
   * 
   * @param bind_address the address to bind the proxy server to.
   * @param port the port to bind the proxy server to.
   * @param out_network_interface the output network interface.
   */
  public TCPInstanceProxyServer(InetAddress bind_address, int port,
                                NetworkInterface out_network_interface) {

    this.log = Logger.getLogger("com.mckoi.network.Log");

    this.bind_interface = bind_address;
    this.port = port;
    this.out_network_interface = out_network_interface;

    if (bind_interface != null) {
      if (bind_interface instanceof Inet6Address &&
          bind_interface.isLinkLocalAddress()) {
        Inet6Address ipv6_addr = (Inet6Address) bind_interface;
        NetworkInterface net_if = ipv6_addr.getScopedInterface();
        if (net_if == null) {
          throw new IllegalStateException("Link local IPv6 address must have a scope id.");
        }
      }
    }

    // The thread pool for servicing client requests,
    thread_pool = Executors.newCachedThreadPool();
  }

  /**
   * Runs the tcp instance service, blocking until the server is killed.
   */
  public void run() {
    ServerSocket socket;
    try {
      socket = new ServerSocket(port, 150, bind_interface);
      socket.setSoTimeout(0);
    }
    catch (IOException e) {
      log.log(Level.SEVERE, "IO Error when starting socket", e);
      return;
    }
    while (true) {
      Socket s;
      try {
        // The socket to run the server,
        s = socket.accept();

        // Dispatch the connection to the thread pool,
        thread_pool.execute(new ProxyConnection(s));

      }
      catch (IOException e) {
        log.log(Level.WARNING, "Socket Error", e);
      }
    }
  }

  // ----- Inner classes -----

  /**
   * Handles a single socket connection.
   */
  private class ProxyConnection implements Runnable {

    private final Socket s;
    private final HashMap<String, String> message_dictionary;

    ProxyConnection(Socket s) {
      this.s = s;
      this.message_dictionary = new HashMap();
    }

    /**
     * The connection process loop.
     */
    @Override
    public void run() {
      InputStream socket_in;
      OutputStream socket_out;
      try {
        // 30 minute timeout on proxy connections,
        s.setSoTimeout(30 * 60 * 1000);

        // Get as input and output stream on the sockets,
        socket_in = s.getInputStream();
        socket_out = s.getOutputStream();

        // Wrap the input stream in a data and buffered input stream,
        BufferedInputStream bin = new BufferedInputStream(socket_in, 1024);
        DataInputStream din = new DataInputStream(bin);

        // Wrap the output stream in a data and buffered output stream,
        BufferedOutputStream bout = new BufferedOutputStream(socket_out, 1024);
        DataOutputStream dout = new DataOutputStream(bout);

        // Perform the handshake,
        long systemtime = System.currentTimeMillis();
        dout.writeLong(systemtime);
        dout.flush();
        long back = din.readLong();
        if (systemtime != back) {
          throw new IOException("Bad protocol request");
        }
        dout.writeUTF("MckoiDDB Proxy Service v1.0");
        dout.flush();
        final String net_password = din.readUTF();

        // The connector to proxy commands via,
        TCPConnectorValues properties =
                  new TCPConnectorValues(net_password, out_network_interface);
        final TCPNetworkConnector connector =
                                         new TCPNetworkConnector(properties);

        // The rest of the communication will be command requests;
        while (true) {

          // Read the command,
          char command = din.readChar();
          if (command == '0') {
            // Close connection if we receive a '0' command char
            dout.close();
            din.close();
            return;
          }
          ServiceAddress address = ServiceAddress.readFrom(din);
          MessageStream msg = MessageStream.readFrom(din, message_dictionary);

          ProcessResult msg_result;

          // Proxy the command over the network,
          if (command == 'a') {
            msg_result = connector.connectInstanceAdmin(address).process(msg);
          }
          else if (command == 'b') {
            msg_result = connector.connectBlockServer(address).process(msg);
          }
          else if (command == 'm') {
            msg_result = connector.connectManagerServer(address).process(msg);
          }
          else if (command == 'r') {
            msg_result = connector.connectRootServer(address).process(msg);
          }
          else {
            throw new IOException("Unknown command to proxy: " + command);
          }

          // Return the result,
          ((MessageStream) msg_result).writeTo(dout, message_dictionary);
          dout.flush();

        }

      }
      catch (IOException e) {
        if (e instanceof SocketException &&
            e.getMessage().equals("Connection reset")) {
          // Ignore connection reset messages,
        }
        else if (e instanceof EOFException) {
          // Ignore this one too,
        }
        else {
          log.log(Level.SEVERE, "IO Error during connection input", e);
        }
      }
      finally {
        // Make sure the socket is closed before we return from the thread,
        try {
          s.close();
        }
        catch (IOException e) {
          log.log(Level.SEVERE, "IO Error on connection close", e);
        }
      }
    }

  }

}
