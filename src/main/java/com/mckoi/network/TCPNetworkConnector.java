/**
 * com.mckoi.network.TCPNetworkConnector  Dec 1, 2008
 *
 * Mckoi Database Software ( http://www.mckoi.com/ )
 * Copyright (C) 2000 - 2012  Diehl and Associates, Inc.
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
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An implementation of NetworkConnector used by a client to connect to nodes
 * in a Mckoi Network over TCP/IP. This implementation provides connection
 * keep-alive like semantics for node communication.
 *
 * @author Tobias Downer
 */

class TCPNetworkConnector implements NetworkConnector {

  /**
   * The background thread that kills connections that timeout.
   */
  private ConnectionDestroyThread background_thread;

  /**
   * The pool of active connections from this JVM to other nodes in the
   * network.
   */
  private final HashMap<ServiceAddress, TCPConnection> connection_pool;

  /**
   * The network password used to connect to the services.
   */
  private String password;

  private int introduced_latency = 0;

  // ---------- Logging ----------

  private static final Logger log = Logger.getLogger("com.mckoi.network.Log");


  /**
   * Constructor.
   */
  TCPNetworkConnector(String password) {

    // Security check,
    SecurityManager security = System.getSecurityManager();
    if (security != null)
         security.checkPermission(MckoiNetworkPermission.CREATE_TCP_CONNECTOR);

    connection_pool = new HashMap();
    this.password = password;

    // This thread kills connections that have timed out.
    background_thread = new ConnectionDestroyThread(log, connection_pool);
    background_thread.setDaemon(true);
    background_thread.start();
  }

  @Override
  public void stop() {
    background_thread.stopConnectionDestroy();
  }

  /**
   * Returns a connection with the given service host.
   */
  private TCPConnection getConnection(ServiceAddress address)
                                                          throws IOException {
    TCPConnection c;
    synchronized (connection_pool) {
      c = connection_pool.get(address);
      // If there isn't, establish a connection,
      if (c == null) {
        c = new TCPConnection();
        c.connect(password, address);
        connection_pool.put(address, c);
      }
      else {
        c.addLock();
      }
    }
    return c;
  }

  /**
   * Clean the address from the connection cache.
   */
  private void invalidateConnection(ServiceAddress address) {
    synchronized (connection_pool) {
      connection_pool.remove(address);
    }
  }
  
  /**
   * Releases a connection from some obligation, therefore permitting it to be
   * released.
   */
  private void releaseConnection(TCPConnection c) {
    synchronized (connection_pool) {
      c.removeLock();
    }
  }

  /**
   * Sets an artifical latency to each communication command, intended for
   * testing purposes to simulate network communication.
   */
  void setCommLatency(int introduced_latency) {
    this.introduced_latency = introduced_latency;
  }





  @Override
  public void finalize() throws Throwable {
    background_thread.stopConnectionDestroy();
    super.finalize();
  }
  
  
  
  // ---------- Implemented from NetworkConnector ----------

  /**
   * Connects to the instance administration component of the given address.
   */
  @Override
  public MessageProcessor connectInstanceAdmin(ServiceAddress address) {
    return new RemoteMessageProcessor(address, 'a');
  }

  /**
   * Connects to a block server at the given address.
   */
  @Override
  public MessageProcessor connectBlockServer(ServiceAddress address) {
    return new RemoteMessageProcessor(address, 'b');
  }

  /**
   * Connects to a manager server at the given address.
   */
  @Override
  public MessageProcessor connectManagerServer(ServiceAddress address) {
    return new RemoteMessageProcessor(address, 'm');
  }

  /**
   * Connects to a root server at the given address.
   */
  @Override
  public MessageProcessor connectRootServer(ServiceAddress address) {
    return new RemoteMessageProcessor(address, 'r');
  }

  // ----- Inner classes -----

  /**
   * A connection with a node in the network, from this node.
   */
  private static class TCPConnection {

    /**
     * The socket connection.
     */
    private Socket s;

    /**
     * The buffered input and output stream on this socket.
     */
    private InputStream in;
    private OutputStream out;

    /**
     * The number of locks on this object.
     */
    private long lock_count;

    /**
     * The time this connection was last used.
     */
    private long last_lock_timestamp;

    /**
     * The message string dictionary.
     */
    private HashMap<String, String> message_dictionary;

    /**
     * Constructs the connection on the given socket.
     */
    TCPConnection() {
      this.lock_count = 1;
      last_lock_timestamp = System.currentTimeMillis();
    }

    void connect(String password, final ServiceAddress addr)
                                                           throws IOException {

      // Creating the socket connection is a privileged operation because it
      // is dynamic (a call stack that ends up here can be from anything).
      // We assume that all objects that call through to this are secured.
      try {
        AccessController.doPrivileged(new PrivilegedExceptionAction() {
          @Override
          public Object run() throws IOException {
            s = new Socket(addr.asInetAddress(), addr.getPort());
            return null;
          }
        });
      }
      // Rethrow as IOException
      catch (PrivilegedActionException e) {
        throw (IOException) e.getCause();
      }

      // Set up socket properties,
      s.setSoTimeout(30 * 1000);  // 30 second timeout,
      s.setTcpNoDelay(true);
      int cur_send_buf_size = s.getSendBufferSize();
      if (cur_send_buf_size < 256 * 1024) {
        s.setSendBufferSize(256 * 1024);
      }
      int cur_receive_buf_size = s.getReceiveBufferSize();
      if (cur_receive_buf_size < 256 * 1024) {
        s.setReceiveBufferSize(256 * 1024);
      }

      in = new BufferedInputStream(s.getInputStream(), 4000);
      out = new BufferedOutputStream(s.getOutputStream(), 4000);

      DataInputStream din = new DataInputStream(in);
      long rv = din.readLong();

      // Send the password,
      DataOutputStream dout = new DataOutputStream(out);
      dout.writeLong(rv);
      short sz = (short) password.length();
      dout.writeShort(sz);
      for (int i = 0; i < sz; ++i) {
        dout.writeChar(password.charAt(i));
      }
      dout.flush();

      message_dictionary = new HashMap(128);

    }

    void close() throws IOException {
      s.close();
      message_dictionary = null;
    }

    void addLock() {
      ++lock_count;
      last_lock_timestamp = System.currentTimeMillis();
    }

    void removeLock() {
      --lock_count;
    }

  }

  /**
   * A message processor on the given remote address.
   */
  private class RemoteMessageProcessor implements MessageProcessor {

    /**
     * The remote address,
     */
    private final ServiceAddress address;

    /**
     * The command dispatcher code.
     */
    private final char command_code;

    /**
     * Constructor.
     */
    RemoteMessageProcessor(ServiceAddress address, char c) {
      if (address == null) {
        throw new NullPointerException();
      }
      this.address = address;
      this.command_code = c;
    }

    
    private ProcessResult processInternal(
                                    MessageStream msg_stream, int try_count) {
      TCPConnection c = null;
      try {
        // Check if there's a connection in the pool already,
        c = getConnection(address);

        synchronized (c) {
          DataOutputStream dout = new DataOutputStream(c.out);
          DataInputStream din = new DataInputStream(c.in);

          // Write the message.
          dout.writeChar(command_code);
          msg_stream.writeTo(dout, c.message_dictionary);
          dout.flush();

          // Fetch the result,
          MessageStream msg_result =
                            MessageStream.readFrom(din, c.message_dictionary);

          // If there's a test latency,
          if (introduced_latency > 0) {
            try {
              Thread.sleep(introduced_latency);
            }
            catch (InterruptedException e) {
              throw new Error("Interrupted", e);
            }
          }

          // And return it,
          return msg_result;
        }

      }
      catch (IOException e) {

        // We log this as a warning. The failure is propogated back in an
        // error message.
        if (try_count == 0) {
          log.log(Level.WARNING, "IOException processing a message", e);
        }

        // On an IOException we invalidate the connection forcing the
        // system to create a new socket. The reason for this is because an
        // IOException destroys the communication format.

        invalidateConnection(address);

        // If this is a 'connection reset by peer' error, or a socket
        // exception, we retry the command one more time.

        if (try_count == 0 &&
            (e instanceof SocketException ||
             e instanceof EOFException ||
             e instanceof SocketTimeoutException)) {

          log.log(Level.WARNING,
                    "Retrying process after exception: {0}",
                    new Object[] { e.getClass().getName() });

          // And retry,
          return processInternal(msg_stream, try_count + 1);
        }

        MessageStream msg_result = new MessageStream(16);
        msg_result.addMessage("E");
        if (e instanceof EOFException) {
          msg_result.addExternalThrowable(new ExternalThrowable(
                     new RuntimeException("EOF (is net password correct?)", e)));
        }
        else {
          // Report this error as a msg_stream fault,
          msg_result.addExternalThrowable(new ExternalThrowable(e));
//                     new RuntimeException(e.getMessage(), e)));
        }
        msg_result.closeMessage();
        return msg_result;
      }
      // Make sure we release the connection,
      finally {
        if (c != null) {
          releaseConnection(c);
        }
      }
    }
    
    
    @Override
    public ProcessResult process(MessageStream msg_stream) {
      return processInternal(msg_stream, 0);
    }

  }

  /**
   * A thread that kills connections that timed out.
   */
  private static class ConnectionDestroyThread extends Thread {

    private boolean stopped = false;

    private final Logger log;
    private final HashMap<ServiceAddress, TCPConnection> connection_pool;

    /**
     * Constructor.
     */
    ConnectionDestroyThread(Logger log,
                     HashMap<ServiceAddress, TCPConnection> connection_pool) {
      this.log = log;
      this.connection_pool = connection_pool;
    }


    @Override
    public void run() {
      try {
        ArrayList<TCPConnection> timeout_list = new ArrayList();
        while (true) {
          timeout_list.clear();
          synchronized (connection_pool) {
            // We check the connections every 2 minutes,
            connection_pool.wait(2 * 60 * 1000);
            long time_now = System.currentTimeMillis();
            Set<ServiceAddress> s = connection_pool.keySet();
            // For each key entry,
            Iterator<ServiceAddress> i = s.iterator();
            while (i.hasNext()) {
              ServiceAddress addr = i.next();
              TCPConnection c = connection_pool.get(addr);
//              System.out.println("CHECK: " + (c.last_lock_timestamp+(10*1000)) + " - " + time_now);
//              System.out.println("c.lock_count = " + c.lock_count);
              // If lock is 0, and past timeout, we can safely remove it.
              // The timeout on a connection is 5 minutes plus the poll artifact
              if (c.lock_count == 0 &&
                  c.last_lock_timestamp + (5 * 60 * 1000) < time_now) {
                
                i.remove();
                timeout_list.add(c);

              }
            }

            // If the thread was stopped, we finish the run method which stops
            // the thread.
            if (stopped) {
              return;
            }

          }  // synchronized (connection_pool)

          // For each connection that timed out,
          for (TCPConnection c : timeout_list) {
//            System.out.println("KILLING: " + c.s);
            DataOutputStream dout = new DataOutputStream(c.out);
            // Write the stream close message, and flush,
            try {
              dout.writeChar('e');
              dout.flush();
              c.s.close();
            }
            catch (IOException e) {
              log.log(Level.SEVERE, "Failed to dispose timed out connection", e);
            }
          }

        }
      }
      catch (InterruptedException e) {
        // Thread was killed,
      }
    }

    void stopConnectionDestroy() {
      synchronized (connection_pool) {
        stopped = true;
        connection_pool.notifyAll();
      }
    }

  }
  
}
