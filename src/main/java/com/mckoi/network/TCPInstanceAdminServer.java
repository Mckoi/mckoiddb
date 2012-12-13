/**
 * com.mckoi.network.TCPInstanceAdminServer  Dec 1, 2008
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

import com.mckoi.util.AnalyticsHistory;
import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * The admin server is a single service that runs on every JVM instance that
 * is a node of a Mckoi Network, and handles online configuration and message
 * passing functionality of Mckoi Network related services in the JVM.
 * <p>
 * This service simple starts or stops services without checking if running the
 * service breaks network topology rules. The role of enforcing topology rules
 * is enforced by an external process.
 * <p>
 * Every node of a TCP backed Mckoi Network must run this service.
 *
 * @author Tobias Downer
 */

public class TCPInstanceAdminServer implements Runnable {

  /**
   * The configuration file.
   */
  private final NetworkConfigResource config_file;

  /**
   * The network address this service is bound to.
   */
  private final InetAddress bind_interface;

  /**
   * The socket.
   */
  private ServerSocket socket;

  /**
   * The port on which administration commands may be received.
   */
  private final int port;
  
  /**
   * The password string needed to communicate with the administration server.
   */
  private final String password_string;

  /**
   * The thread pool.
   */
  private final ExecutorService thread_pool;

  /**
   * The base path to which stores data related to information stored by this
   * service.
   */
  private final File base_path;

  /**
   * The logger.
   */
  private final static Logger log = Logger.getLogger("com.mckoi.network.Log");

  /**
   * The timer thread,
   */
  private Timer timer;

  /**
   * The analytics object for operations on this node.
   */
  private final AnalyticsHistory analytics;

  /**
   * The current list of active connections.
   */
  private final ArrayList<Connection> connection_list;


  /**
   * The possible services that can run in this JVM.
   */
  private final Object server_manager_lock = new Object();
  private LocalFileSystemBlockServer block_server;
  private LocalFileSystemManagerServer manager_server;
  private LocalFileSystemRootServer root_server;

  /**
   * For managing startup notification.
   */
  private final Object startup_lock = new Object();
  private boolean run_invoked = false;
  private boolean instance_started = false;
  private boolean instance_stopped = false;
  

  /**
   * Constructs the instance server.
   */
  public TCPInstanceAdminServer(NetworkConfigResource config_file,
                          InetAddress bind_address, int port,
                          Properties node_properties) throws IOException {

    // Create the connection list
    connection_list = new ArrayList();

    // Set the log level,
    String val = node_properties.getProperty("log_level", "info");
    if (val != null) {
      log.setLevel(Level.parse(val.toUpperCase(Locale.ENGLISH)));
    }

    // Logging directory,
    val = node_properties.getProperty("log_directory");
    if (val != null) {
      // Set a log directory,
      File f = new File(val.trim());
      if (!f.exists()) {
        f.mkdirs();
      }
      if (!f.isDirectory()) {
        throw new RuntimeException("\"log_directory\" value is not a directory.");
      }

      val = val.replace("\\", "/");
      if (!val.endsWith("/")) {
        val = val + "/";
      }
      val = val + "ddb.log";

      int logfile_limit = (1 * 1024 * 1024);
      String lf_limit = node_properties.getProperty("logfile_limit");
      if (lf_limit != null) {
        logfile_limit = Integer.parseInt(lf_limit);
      }
      int logfile_count = 4;
      String lf_count = node_properties.getProperty("logfile_count");
      if (lf_count != null) {
        logfile_count = Integer.parseInt(lf_count);
      }

      // Output to the log file,
      FileHandler fhandler =
                      new FileHandler(val, logfile_limit, logfile_count, true);
      fhandler.setFormatter(new SimpleFormatter());
      log.addHandler(fhandler);
      // Disable using parent handlers for log (disables logging to system.err)
      if (node_properties.getProperty("log_use_parent_handlers",
                                      "yes").equals("no")) {
        log.setUseParentHandlers(false);
      }

    }

    this.config_file = config_file;
    this.bind_interface = bind_address;
    this.port = port;

    // Get the network password property,
    val = node_properties.getProperty("network_password");
    if (val == null) {
      log.log(Level.SEVERE, "Couldn't find property: \"network_password\"");
      throw new RuntimeException("Couldn't find property: \"network_password\"");
    }
    else {
      String password_str = val.trim();
      if (password_str.length() == 0) {
        log.log(Level.SEVERE, "The network_password property is not set");
        throw new RuntimeException("The network_password property is not set");
      }
      else if (password_str.length() < 3) {
        log.log(Level.SEVERE, "The network_password property is too short");
        throw new RuntimeException("The network_password property is too short");
      }

      this.password_string = password_str;
      log.config("Network password is set.");
    }

    // Get the base path property,
    val = node_properties.getProperty("node_directory");
    if (val == null) {
      log.log(Level.SEVERE, "Couldn't find property: \"node_directory\"");
      throw new RuntimeException("Couldn't find property: \"node_directory\"");
    }
    else {
      File f = new File(val.trim());
      if (!f.exists()) {
        f.mkdirs();
      }
      if (!f.isDirectory()) {
        log.log(Level.SEVERE, "\"node_directory\" value is not a directory.");
        throw new RuntimeException("\"node_directory\" value is not a directory.");
      }
      this.base_path = f;
      log.log(Level.CONFIG, "Set node directory to {0}", val);
    }

    // The thread pool for servicing client requests,
    thread_pool = Executors.newCachedThreadPool();

    // Load the config file,
    config_file.load();

    // The analytics object,
    analytics = new AnalyticsHistory();

  }

  /**
   * Constructs the instance server.
   */
  public TCPInstanceAdminServer(NetworkConfigResource config_file,
                          InetAddress bind_address, int port,
                          String password, File base_path) throws IOException {

    this(config_file, bind_address, port, makeConfig(password, base_path));

//    this.config_file = config_file;
//    this.bind_interface = bind_address;
//    this.port = port;
//    this.password_string = password;
//    this.base_path = base_path;
//
//    this.log = Logger.getLogger("com.mckoi.network.Log");
//
//    // The thread pool for servicing client requests,
//    thread_pool = Executors.newCachedThreadPool();
//
//    // Load the config file,
//    config_file.load();
  }

  private static Properties makeConfig(String net_password, File base_path)
                                                           throws IOException {
    Properties p = new Properties();
    p.setProperty("network_password", net_password);
    p.setProperty("node_directory", base_path.getCanonicalPath());
    return p;
  }


  /**
   * Starts a service.
   */
  private void startService(String service_type) throws IOException {
    // This service as a ServiceAddress object,
    ServiceAddress this_service =
                             new ServiceAddress(bind_interface, port);

    // Start the services,
    synchronized (server_manager_lock) {
      if (!base_path.exists()) {
        base_path.mkdirs();
      }
      if (service_type.equals("block_server")) {
        if (block_server == null) {
          File npath = new File(base_path, "block");
          if (!npath.exists()) {
            npath.mkdir();
          }
          File active_f = new File(base_path, BLOCK_RUN_FILE);
          active_f.createNewFile();
          block_server = new LocalFileSystemBlockServer(
                     new TCPNetworkConnector(password_string), npath, timer);
          block_server.start();
        }
      }
      else if (service_type.equals("manager_server")) {
        if (manager_server == null) {
          File npath = new File(base_path, "manager");
          if (!npath.exists()) {
            npath.mkdir();
          }
          File active_f = new File(base_path, MANAGER_RUN_FILE);
          active_f.createNewFile();
          manager_server = new LocalFileSystemManagerServer(
                   new TCPNetworkConnector(password_string), base_path, npath,
                   this_service, timer);
          manager_server.start();
        }
      }
      else if (service_type.equals("root_server")) {
        if (root_server == null) {
          File npath = new File(base_path, "root");
          if (!npath.exists()) {
            npath.mkdir();
          }
          File active_f = new File(base_path, ROOT_RUN_FILE);
          active_f.createNewFile();
          root_server = new LocalFileSystemRootServer(
                   new TCPNetworkConnector(password_string), npath,
                   this_service, timer);
          root_server.start();
        }
      }
      else {
        throw new RuntimeException("Unknown service: " + service_type);
      }
    }
  }

  /**
   * Stops a service.
   */
  private void stopService(String service_type) throws IOException {
    synchronized (server_manager_lock) {
      if (service_type.equals("block_server")) {
        if (block_server != null) {
          File active_f = new File(base_path, BLOCK_RUN_FILE);
          active_f.delete();
          block_server.stop();
          block_server = null;
        }
      }
      else if (service_type.equals("manager_server")) {
        if (manager_server != null) {
          File active_f = new File(base_path, MANAGER_RUN_FILE);
          active_f.delete();
          manager_server.stop();
          manager_server = null;
        }
      }
      else if (service_type.equals("root_server")) {
        if (root_server != null) {
          File active_f = new File(base_path, ROOT_RUN_FILE);
          active_f.delete();
          root_server.stop();
          root_server = null;
        }
      }
      else {
        throw new RuntimeException("Unknown service: " + service_type);
      }
    }
  }


  /**
   * A timed task that updates the network configuration.
   */
  private TimerTask config_update_task = new TimerTask() {
    @Override
    public void run() {
      try {
        config_file.load();
      }
      catch (IOException e) {
        log.log(Level.SEVERE, "Network config error: ", e);
      }
    }
  };

  /**
   * Closes any currently open socket for this admin server.
   */
  public void close() {
    try {
      if (socket != null) {
        socket.close();
      }
    }
    catch (IOException e) {
      log.log(Level.SEVERE, "IOException closing service: ", e);
    }
  }

  /**
   * If this service isn't started, blocks until the service is started and
   * accepting incoming connections. Returns immediately if started.
   */
  public void waitUntilStarted() {
    synchronized (startup_lock) {
      while (!instance_started) {
        try {
          startup_lock.wait();
        }
        catch (InterruptedException e) {
          throw new Error("Interrupted", e);
        }
      }
    }
  }

  /**
   * If this service is started, blocks until the service is stopped and
   * no longer accepting incoming connections. Returns immediately if stopped.
   */
  public void waitUntilStopped() {
    synchronized (startup_lock) {
      while (!instance_stopped) {
        try {
          startup_lock.wait();
        }
        catch (InterruptedException e) {
          throw new Error("Interrupted", e);
        }
      }
    }
  }
  
  
  /**
   * Runs the tcp instance service, blocking until the server is killed or a
   * critical stop condition is encountered with one of the services running
   * in the JVM.
   */
  @Override
  public void run() {
    
    synchronized (startup_lock) {
      if (run_invoked) {
        throw new RuntimeException("Service was run before.");
      }
      run_invoked = true;
    }

    // The timer thread,
    this.timer = new ExceptionCatchingTimer(true);

    try {
      // Start services as necessary,
      File check_file;
      try {
        check_file = new File(base_path, BLOCK_RUN_FILE);
        if (check_file.exists()) {
          startService("block_server");
        }
        check_file = new File(base_path, MANAGER_RUN_FILE);
        if (check_file.exists()) {
          startService("manager_server");
        }
        check_file = new File(base_path, ROOT_RUN_FILE);
        if (check_file.exists()) {
          startService("root_server");
        }
      }
      catch (IOException e) {
        log.log(Level.SEVERE, "IO Error when starting services", e);
        return;
      }

      // Schedule a refresh of the config file,
      // (We add a little entropy to ensure the network doesn't get hit by
      //  synchronized requests).
      Random r = new Random();
      long second_mix = r.nextInt(1000);
      timer.scheduleAtFixedRate(config_update_task, 50 * 1000,
                                ((2 * 59) * 1000) + second_mix);

      try {
        socket = new ServerSocket(port, 150, bind_interface);
        socket.setSoTimeout(0);
        int cur_receive_buf_size = socket.getReceiveBufferSize();
        if (cur_receive_buf_size < 256 * 1024) {
          socket.setReceiveBufferSize(256 * 1024);
        }
      }
      catch (IOException e) {
        log.log(Level.SEVERE, "IO Error when starting socket", e);
        return;
      }

      log.log(Level.INFO, "Node Started on {0} port {1}",
              new Object[] { bind_interface.getHostAddress(), port });

      try {
        
        // Set the 'instance_started' flag.
        synchronized (startup_lock) {
          instance_started = true;
          startup_lock.notifyAll();
        }

        while (true) {
          // The socket to run the server,
          Socket s = socket.accept();
          s.setTcpNoDelay(true);
          int cur_send_buf_size = s.getSendBufferSize();
          if (cur_send_buf_size < 256 * 1024) {
            s.setSendBufferSize(256 * 1024);
          }

          // Make sure this ip address is allowed,
          InetAddress inet_address = s.getInetAddress();
          String ip_addr = inet_address.getHostAddress();
//          log.info("Connection from " + ip_addr);
          // Check it's allowed,
          if (inet_address.isLoopbackAddress() ||
              config_file.isIPAllowed(ip_addr)) {
            // Dispatch the connection to the thread pool,
            thread_pool.execute(new Connection(s));
          }
          else {
            log.log(Level.SEVERE, "Connection from IP refused (not on whitelist): {0}", ip_addr);
          }
        }

      }
      catch (IOException e) {
        log.log(Level.WARNING, "Socket Closed");
      }
      catch (Exception e) {
        log.log(Level.SEVERE,
                "DDB instance service stopped because of exception.", e);
      }

    }
    finally {

      try {
        this.timer.cancel();
        // Shut down the thread pool,
        this.thread_pool.shutdown();

        synchronized (connection_list) {
          for (Connection c : connection_list) {
            try {
              // If the socket isn't closed, close it
              if (!c.s.isClosed()) {
                c.s.close();
              }
            }
            catch (IOException e) {
              // Ignore
            }
          }
        }

        // Stop services as necessary,
        File check_file;
        check_file = new File(base_path, BLOCK_RUN_FILE);
        if (check_file.exists() && block_server != null) {
          block_server.stop();
          block_server = null;
        }
        try {
          check_file = new File(base_path, MANAGER_RUN_FILE);
          if (check_file.exists() && manager_server != null) {
            manager_server.stop();
            manager_server = null;
          }
        }
        catch (IOException e) {
          log.log(Level.SEVERE, "IO Error when stopping services", e);
        }
        check_file = new File(base_path, ROOT_RUN_FILE);
        if (check_file.exists() && root_server != null) {
          root_server.stop();
          root_server = null;
        }

      }
      catch (Exception e) {
        e.printStackTrace(System.err);
      }
      finally {
      
        // Reset the 'instance_started' flag.
        synchronized (startup_lock) {
          instance_started = true;
          instance_stopped = true;
          startup_lock.notifyAll();
        }

      }

    }
  }
  

  
  // ----- Inner classes -----
  
  /**
   * Handles a single socket connection.
   */
  private class Connection implements Runnable {

    private final Random random_generator;
    final Socket s;
    final HashMap<String, String> message_dictionary;

    Connection(Socket s) {
      this.s = s;
      this.random_generator = new Random();
      this.message_dictionary = new HashMap();
    }

    /**
     * Processes an administration command.
     */
    public MessageStream processAdminCommand(MessageStream msg_in) {
      // The message output,
      MessageStream msg_out = new MessageStream(32);
      // For each message in the message input,
      for (Message m : msg_in) {
        try {
          String command = m.getName();
          // Report on the services running,
          if (command.equals("report")) {
            synchronized (server_manager_lock) {
              long tm = Runtime.getRuntime().totalMemory();
              long fm = Runtime.getRuntime().freeMemory();
              long td = base_path.getTotalSpace();
              long fd = base_path.getUsableSpace();

              msg_out.addMessage("R");
              if (block_server == null) {
                msg_out.addString("block_server=no");
              }
              else {
                msg_out.addString(Long.toString(block_server.getBlockCount()));
              }
              msg_out.addString("manager_server=" +
                                (manager_server == null ? "no" : "yes"));
              msg_out.addString("root_server=" +
                                (root_server == null ? "no" : "yes"));
              msg_out.addLong(tm - fm);
              msg_out.addLong(tm);
              msg_out.addLong(td - fd);
              msg_out.addLong(td);
              msg_out.closeMessage();
            }
          }
          else if (command.equals("reportStats")) {
            // Analytics stats; we convert the stats to a long[] array and
            // send it as a reply.
            long[] stats = analytics.getStats();
            msg_out.addMessage("R");
            msg_out.addLongArray(stats);
            msg_out.closeMessage();
          }
          else {
            // Starts a service,
            if (command.equals("start")) {
              String service_type = (String) m.param(0);
              startService(service_type);
            }
            // Stops a service,
            else if (command.equals("stop")) {
              String service_type = (String) m.param(0);
              stopService(service_type);
            }
            else {
              throw new RuntimeException("Unknown command: " + command);
            }
            
            // Add reply message,
            msg_out.addMessage("R");
            msg_out.addLong(1);
            msg_out.closeMessage();
            
          }

        }
        catch (VirtualMachineError e) {
          log.log(Level.SEVERE, "VM Error", e);
          // This will end the connection
          throw e;
        }
        catch (Throwable e) {
          log.log(Level.SEVERE, "Exception during process", e);
          msg_out.addMessage("E");
          msg_out.addExternalThrowable(new ExternalThrowable(e));
          msg_out.closeMessage();
        }
      }
      return msg_out;
    }

    /**
     * Generates a 'no service' error, for when a call is made on a service
     * that isn't being run.
     */
    private MessageStream noServiceError(String service_name) {
      MessageStream msg_out = new MessageStream(16);
      msg_out.addMessage("E");

      StringBuilder b = new StringBuilder();
      b.append("The service requested (");
      b.append(service_name);
      b.append(") is not being run on the instance: ");
      b.append(bind_interface.getHostAddress());
      b.append(":");
      b.append(port);
      msg_out.addExternalThrowable(new ExternalThrowable(
                              new ServiceNotConnectedException(b.toString())));
      msg_out.closeMessage();
      return msg_out;
    }
    
    /**
     * The connection process loop.
     */
    @Override
    public void run() {
      InputStream socket_in;
      OutputStream socket_out;
      try {

        synchronized (connection_list) {
          connection_list.add(this);
        }

        // Get as input and output stream on the sockets,
        socket_in = s.getInputStream();
        socket_out = s.getOutputStream();

        // Wrap the input stream in a data and buffered input stream,
        BufferedInputStream bin = new BufferedInputStream(socket_in, 4000);
        DataInputStream din = new DataInputStream(bin);

        // Wrap the output stream in a data and buffered output stream,
        BufferedOutputStream bout = new BufferedOutputStream(socket_out, 4000);
        DataOutputStream dout = new DataOutputStream(bout);

        // Write a random long and see if it gets pinged back from the client,
        long rv = random_generator.nextLong();
        dout.writeLong(rv);
        dout.flush();
        long read_backv = din.readLong();
        if (rv != read_backv) {
          // Silently close if the value not returned,
          dout.close();
          din.close();
          return;
        }

        // Read the password string from the stream,
        short sz = din.readShort();
        StringBuilder buf = new StringBuilder(sz);
        for (int i = 0; i < sz; ++i) {
          buf.append(din.readChar());
        }
        String password_code = buf.toString();

        // If it doesn't match, terminate the thread immediately,
        if (!password_code.equals(password_string)) {
          log.log(Level.SEVERE, "Client provided bad password");
          return;
        }

        // The main command dispatch loop for this connection,
        while (true) {
          // Read the command destination,
          char destination = din.readChar();
          // Exit thread command,
          if (destination == 'e') {
            return;
          }
          // Read the message stream object
          MessageStream message_stream =
                               MessageStream.readFrom(din, message_dictionary);

          ProcessResult message_out;

          // For analytics
          long benchmark_start = System.currentTimeMillis();

          // Destined for the administration module,
          if (destination == 'a') {
            message_out = processAdminCommand(message_stream);
          }
          // For a block server in this JVM
          else if (destination == 'b') {
            if (block_server == null) {
              message_out = noServiceError("Block");
            }
            else {
              message_out = block_server.getProcessor().process(message_stream);
            }
              
          }
          // For a manager server in this JVM
          else if (destination == 'm') {
            if (manager_server == null) {
              message_out = noServiceError("Manager");
            }
            else {
              message_out = manager_server.getProcessor().process(message_stream);
            }
          }
          // For a root server in this JVM
          else if (destination == 'r') {
            if (root_server == null) {
              message_out = noServiceError("Root");
            }
            else {
              message_out = root_server.getProcessor().process(message_stream);
            }
          }
          else {
            throw new IOException("Unknown destination: " + destination);
          }

          // Update the stats
          long benchmark_end = System.currentTimeMillis();
          long time_took = benchmark_end - benchmark_start;
          analytics.addEvent(benchmark_end, time_took);

          // Write and flush the output message,
          ((MessageStream) message_out).writeTo(dout, message_dictionary);
          dout.flush();

        }  // while (true)

      }
      catch (IOException e) {
        if (e instanceof SocketException &&
            (e.getMessage().equals("Connection reset") ||
             e.getMessage().equals("socket closed") )) {
          // Ignore connection reset messages,
        }
        else if (e instanceof EOFException) {
          // Ignore this one also,
        }
        else {
          log.log(Level.SEVERE, "IO Error during connection input", e);
        }
      }
      // Oops, another exception happened, log it,
      catch (RuntimeException e) {
        log.log(Level.SEVERE, "Exception on node worker", e);
        throw e;
      }
      // Try and log any errors,
      catch (Error e) {
        log.log(Level.SEVERE, "Error on node worker", e);
        throw e;
      }
      finally {
        synchronized (connection_list) {
          connection_list.remove(this);
        }
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

  private static class ExceptionCatchingTimer extends Timer {

    private ExceptionCatchingTimer(boolean d) {
      super(d);
    }

    @Override
    public void schedule(TimerTask task, long delay) {
      super.schedule(new CException(task), delay);
    }

    @Override
    public void schedule(TimerTask task, Date time) {
      super.schedule(new CException(task), time);
    }

    @Override
    public void schedule(TimerTask task, long delay, long period) {
      super.schedule(new CException(task), delay, period);
    }

    @Override
    public void schedule(TimerTask task, Date firstTime, long period) {
      super.schedule(new CException(task), firstTime, period);
    }

    @Override
    public void scheduleAtFixedRate(TimerTask task, long delay, long period) {
      super.scheduleAtFixedRate(new CException(task), delay, period);
    }

    @Override
    public void scheduleAtFixedRate(TimerTask task, Date firstTime, long period) {
      super.scheduleAtFixedRate(new CException(task), firstTime, period);
    }

  }

  private static class CException extends TimerTask {

    private TimerTask fallthrough;

    public CException(TimerTask fallthrough) {
      this.fallthrough = fallthrough;
    }

    @Override
    public long scheduledExecutionTime() {
      return fallthrough.scheduledExecutionTime();
    }

    @Override
    public void run() {
      try {
        fallthrough.run();
      }
      catch (Error e) {
        // We let these exceptions fall through and stop the timer,
      }
      catch (Throwable e) {
        // Otherwise log throwable,
        log.log(Level.SEVERE, "Exception during task", e);
      }

    }

    @Override
    public boolean cancel() {
      return fallthrough.cancel();
    }

  }


  // ----- Static vars -----

  /**
   * The run files (denote the servers that are running).
   */
  private static String BLOCK_RUN_FILE = "runblock";
  private static String MANAGER_RUN_FILE = "runmanager";
  private static String ROOT_RUN_FILE = "runroot";

}
