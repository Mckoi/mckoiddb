/**
 * com.mckoi.network.TCPInstanceProxyServer  Jul 18, 2009
 *
 * Mckoi Database Software ( http://www.mckoi.com/ )
 * Copyright (C) 2000 - 2010  Diehl and Associates, Inc.
 *
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 3 as published by
 * the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License version 3
 * along with this program.  If not, see ( http://www.gnu.org/licenses/ ) or
 * write to the Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA  02111-1307, USA.
 *
 * Change Log:
 *
 *
 */

package com.mckoi.network;

import java.io.*;
import java.net.*;
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
   * The thread pool.
   */
  private final ExecutorService thread_pool;

  /**
   * The logger.
   */
  private final Logger log;

  /**
   * Constructs the instance server.
   */
  public TCPInstanceProxyServer(InetAddress bind_address, int port) {

    this.log = Logger.getLogger("com.mckoi.network.Log");

    this.bind_interface = bind_address;
    this.port = port;

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
    public void run() {
      InputStream socket_in = null;
      OutputStream socket_out = null;
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
        final TCPNetworkConnector connector =
                                         new TCPNetworkConnector(net_password);

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
