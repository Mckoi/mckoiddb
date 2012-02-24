/**
 * com.mckoi.network.LocalFileSystemRootServer  Nov 30, 2008
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

import com.mckoi.data.KeyObjectTransaction;
import com.mckoi.data.NodeReference;
import com.mckoi.util.ByteArrayUtil;
import com.mckoi.util.StrongPagedAccess;
import java.io.*;
import java.security.SecureRandom;
import java.text.MessageFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The root server maintains a set of temporal root node lists. Each list
 * item is a timestamp and root node address pair, which expresses a version
 * of a database state at the time of a root node 'publish'. Each root node
 * list is named. The root server allows the implementation of features such
 * as rollback, multiple databases, complex commit processing, transaction
 * debugging, temporal querying and branching.
 * <p>
 * This implementation maps each path as a sequential access file in the local
 * filesystem. Each time a new version of a path is published, a timestamp/
 * root node address pair is atomically added to the end of the file
 * corresponding to the access path.
 * <p>
 * The root server is perhaps the most fragile part of the network. It may
 * deal with a large number of requests and updates, and may be a bottleneck
 * in scaling. It must be located in a central location on a network. The
 * publish and request operations are extremely simple and the bandwidth of
 * each operation is low. If performance is critical there are alternative
 * implementations of a root server that come to mind, such as entirely heap
 * based.
 * <p>
 * An important consideration is the case if the root server fails, the
 * access to the paths the server manages as a whole fails. If information in
 * the root server is completely lost, it is not easy to reconstruct the data
 * and so it is of utmost importance that the root server is run on very
 * stable hardware. The problem with failure/data loss on the root server can
 * somewhat be solved by frequent backup and online server replication.
 * Replication can help scale for read access (since then multiple servers can
 * service query requests) but this can not solve write bottlenecks.
 * <p>
 * Note that the management of root servers for different paths could be split
 * off onto individual servers per path. However, only one root server may
 * be in management of an access path at any one time.
 * <p>
 * Regarding scalability, I can foresee the cost of operations needed to
 * maintain a root server are low enough that bottlenecks such as network
 * bandwidth utilization and limitations with TCP connection handshaking will
 * come into play before machine hardware limitations do. Therefore for very
 * massive scaling requirements we would recommend implementing multiple
 * root servers servicing split database partitions (aka. data set sharding).
 * <p>
 * The commands a root server processor understands are;
 * <p>
 *
 *
 * <b>void addConsensusProcessor(String path_name, String fun, DataAddress root_node)</b> -
 * Creates a blank database path called 'path_name' and associates the given
 * consensus function to the path name to manage updates on that database
 * through this root server. The 'fun' string is a Java class object
 * which must implement com.mckoi.network.ConsensusProcessor and be available
 * in the local classpath of the server JVM process.
 * <p>
 * If the path name already exists then the given root_node is appended to the
 * end of the existing path. If the path name doesn't exist then it's created
 * and set to an initially empty state.
 * <p>
 * This function is intended to only be called by an administration interface.
 * <p>
 *
 *
 * <b>void removeConsensusProcessor(String path_name)</b> -
 * Unlinks a consensus processor from the given path name so that it does not
 * respond to queries anymore. Once a named consensus processor is removed,
 * it will not be listed in QueryConsensusProcessors().
 * <p>
 * This function is intended to only be called by an administration interface.
 * <p>
 *
 *
 * <b>String[] queryConsensusProcessors()</b> -
 * Returns a list of all named database paths and information about their
 * consensus Processor for all paths registered with this root server.
 * <p>
 *
 *
// * <b>void publishPath(String path_name, DataAddress root_node)</b> -
// * Records the given root node as the most recent version of the database
// * state as returned by the 'getPathNow' call. Note that, although this
// * operation is atomic, the atomicity is not intended to determine the
// * consensus to which of multiple conflicting database states is current,
// * rather the means in which to 'publish' a new state to the rest of the
// * network.
// * <p>
// * With this in mind, there is an implied process that must happen before
// * a new version of the database is published by this call to ensure a
// * change is consistant with the rules of the data model. While
// * this process happens, new states can not be published (well, actually
// * that depends on the updates).
// * <p>
// * What typically will happen in a commit process is a change to the database
// * is proposed, the commit process locks, the commit process merges the
// * changes from the proposed transaction into the most recent version.
// * If there are conflicts the commit fails, otherwise the merged transaction
// * is made the current version, either case the commit process then unlocks.
// * <p>
// * Sound complicated? It is, and a tough problem to solve in any environment
// * let alone a distributed fault-tolerant one.
// * <p>
 * 
 * 
 * <b>DataAddress getPathNow(String path_name)</b> -
 * Returns the current root node address of the database with the given path
 * name. This returns the last root node address published via a call to
 * 'publishPath'. This operation is atomic, however the atomicity is not
 * intended to determine the consensus of which state is the most current.
 * It is intended to ensure that a process that requires stateful access can
 * be assured that the value was the last published if the 'publish' and
 * 'get' methods occurred sequentially (via a lock on publish).
 * <p>
 * Note that due to latency, the returned root node may be out of date by the
 * time it is processed if publishing new states is permitted (publish hasn't
 * been locked by another process). Therefore if this call is part of a
 * process that must consider the current database state, a global lock
 * process is implied, held by the calling method, that must prevent
 * publishing new states.
 * <p>
 * 
 * 
 * <b>DataAddress[] getPathHistorical(String path_name, long time_start, long time_end)</b> -
 * Returns a list of historical root node addresses that have been published
 * between the given points in time. An historical root node can be
 * published to a path, or used to create a new path (this will branch the
 * database state into a new isolated branch).
 * <p>
 * Note that historical information may periodically be cleaned from the
 * system, however, those rules are not defined within the scope of this
 * specification.
 * <p>
 * 
 * 
 * <b>long getCurrentTimeMillis()</b> -
 * Returns System.currentTimeMillis() as reported by the JVM managing the
 * root server, used as a reference point for calls to 'getPathHistorical'.
 * 
 * 
 *
 * @author Tobias Downer
 */

public class LocalFileSystemRootServer {

  /**
   * The network connector for communicating with the rest of the network.
   */
  private final NetworkConnector network;
  
  /**
   * The file where the path logs are stored.
   */
  private final File path;

  /**
   * This service address.
   */
  private final ServiceAddress this_service;

  /**
   * The timer.
   */
  private final Timer timer;

  /**
   * The stop state volatile object.
   */
  private volatile Throwable stop_state;

  /**
   * The lock map so updates to paths happen atomically.
   */
  private final HashMap<String, PathAccess> lock_db;

  /**
   * The address of the manager servers this root server is bound to.
   */
  private volatile ServiceAddress[] manager_servers;

  /**
   * The service tracker object for messages from this manager.
   */
  private final ServiceStatusTracker service_tracker;

  /**
   * A cache for path info resolved through this root server.
   */
  private final HashMap<String, PathInfo> path_info_map;

  /**
   * A queue of 'loadPathInfo' commands pending.
   */
  private final ArrayList<PathInfo> load_path_info_queue;

  /**
   * A lock to ensure serial load path info methods.
   */
  private final Object load_path_info_lock = new Object();

  /**
   * Initialization queue.
   */
  private final ArrayList<File> path_initialization_queue;

  /**
   * The cache configuration object.
   */
  private final CacheConfiguration cache_configuration;

  /**
   * The logger.
   */
  private final static Logger log = Logger.getLogger("com.mckoi.network.Log");

  /**
   * A secure random.
   */
  private final static SecureRandom RANDOM = new SecureRandom();

  /**
   * The size of each root entry.
   */
  private final static int ROOT_ITEM_SIZE = 24;

  
  /**
   * DEBUG value (do not use in production).
   */
  private static long DBG_count = 0;

  

  /**
   * Constructs the root server.
   */
  public LocalFileSystemRootServer(NetworkConnector connector,
                                   File path,
                                   ServiceAddress this_service,
                                   Timer timer) {
    this.network = connector;
    this.path = path;
    this.this_service = this_service;
    this.timer = timer;
    this.lock_db = new HashMap(128);
    this.path_info_map = new HashMap(128);
    this.load_path_info_queue = new ArrayList(64);
    this.path_initialization_queue = new ArrayList(64);
    this.cache_configuration = new CacheConfiguration();

    this.manager_servers = null;

    this.service_tracker = new ServiceStatusTracker(network);

    this.service_tracker.addListener(new ServiceStatusListener() {
      @Override
      public void statusChange(ServiceAddress address, String service_type,
                               String old_status, String new_status) {
        // If it's a manager service, and the new status is UP
        if (service_type.equals("manager") && new_status.startsWith("UP")) {
          // Init and load all pending paths,
          processInitQueue();
          loadAllPendingPaths();
        }
        // If it's a root service, and the new status is UP
        if (service_type.equals("root")) {
          if (new_status.startsWith("UP")) {
            // Load all pending paths,
            loadAllPendingPaths();
          }
          // If a root server went down,
          else if (new_status.startsWith("DOWN")) {
            // Scan the paths managed by this root server and desynchronize
            // any not connected to a majority of servers.
            desyncPathsDependentOn(address);
          }
        }
      }
    });
  }
  
  /**
   * Starts the server.
   */
  public void start() {

    try {

      // Read the manager server address from the properties file,
      Properties p = new Properties();

      // Contains the root properties,
      File prop_file = new File(path, "00.properties");
      if (prop_file.exists()) {
        FileInputStream fin = new FileInputStream(prop_file);
        p.load(fin);
        fin.close();
      }

      // Fetch the manager server property,
      String v = p.getProperty("manager_server_address");
      if (v != null) {
        String[] addresses = v.split(",");
        int sz = addresses.length;
        manager_servers = new ServiceAddress[sz];
        for (int i = 0; i < sz; ++i) {
          String address_string = addresses[i].trim();
          if (address_string.length() > 0) {
            manager_servers[i] = ServiceAddress.parseString(addresses[i]);
          }
        }
      }

    }
    catch (IOException e) {
      throw new RuntimeException("IO Error: " + e.getMessage(), e);
    }

    // Adds all the files to the path info queue,
    File[] root_files = path.listFiles();
    for (File f : root_files) {
      String fname = f.getName();
      if (!fname.contains(".")) {
        path_initialization_queue.add(f);
      }
    }

    // Schedule the load on the timer thread,
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        // Load the paths if we can,
        processInitQueue();
        loadAllPendingPaths();
      }
    }, 2000);

  }
  
  /**
   * Stops the server.
   */
  public void stop() {
    manager_servers = null;
  }

  /**
   * Adds paths in the path initialization to the load path info queue. Any
   * errors generated while attempting to load from the manager are ignored.
   */
  private void processInitQueue() {
    synchronized (load_path_info_queue) {
      synchronized (path_initialization_queue) {
        Iterator<File> it = path_initialization_queue.iterator();
        while (it.hasNext()) {
          File f = it.next();
          try {
            // Load the path info from the managers,
            PathInfo path_info = loadFromManagers(f.getName(), -1);
            // Add to the queue,
            load_path_info_queue.add(path_info);
            // Remove the item,
            it.remove();
          }
          catch (RuntimeException e) {
            log.log(Level.FINE, "Error on path init", e);
            log.log(Level.FINE, "Trying path init {0} later", f.getName());
          }
        }
      }
    }
  }

  /**
   * Loads all pending path infos on the load queue.
   */
  private void loadAllPendingPaths() {
    // Make a copy of the pending path info loads,
    ArrayList<PathInfo> pi_list = new ArrayList(64);
    synchronized (load_path_info_queue) {
      for (PathInfo pi : load_path_info_queue) {
        pi_list.add(pi);
      }
    }
    // Do the load operation on the pending,
    try {
      for (PathInfo pi : pi_list) {
        loadPathInfo(pi);
      }
    }
    catch (IOException e) {
      log.log(Level.SEVERE, "IO Error", e);
    }
  }

  /**
   * Check the path name is valid. A path name must be a letter or digit and
   * lower case.
   */
  private void checkPathNameValid(String name) {
    int sz = name.length();
    boolean invalid = false;
    for (int i = 0; i < sz; ++i) {
      char c = name.charAt(i);
      // If the character is not a letter or digit or lower case, then it's
      // invalid
      if (!Character.isLetterOrDigit(c) || Character.isUpperCase(c)) {
        invalid = true;
      }
    }

    // Exception if invalid,
    if (invalid) {
      throw new RuntimeException("Path name '" + name +
              "' is invalid, must contain only lower case letters or digits.");
    }
  }


  /**
   * Polls all the root machines in the given list.
   */
  private void pollAllRootMachines(ServiceAddress[] machines) {
    // Create the message,
    MessageStream msg_out = new MessageStream(16);
    msg_out.addMessage("poll");
    msg_out.addString("LRSPoll");
    msg_out.closeMessage();

    for (int i = 0; i < machines.length; ++i) {
      ServiceAddress machine = machines[i];
      // If the service is up in the tracker,
      if (service_tracker.isServiceUp(machine, "root")) {
        // Poll it to see if it's really up.
        // Send the poll to the service,
        MessageProcessor processor = network.connectRootServer(machine);
        ProcessResult msg_in = processor.process(msg_out);
        // If return is a connection fault,
        Iterator<Message> it = msg_in.iterator();
        while (it.hasNext()) {
          Message m = it.next();
          if (m.isError() && ReplicatedValueStore.isConnectionFault(m)) {
            service_tracker.reportServiceDownClientReport(machine, "root");
          }
        }
      }
    }
  }



  /**
   * Desynchronizes any paths found that have the given root server as a
   * dependent and are currently unavailable (a majority of the root services
   * are down in the tracker).
   */
  private void desyncPathsDependentOn(ServiceAddress root_service) {

    ArrayList<PathInfo> desynced_paths = new ArrayList(64);

    synchronized (lock_db) {
      // The set of all paths,
      Set<String> paths = lock_db.keySet();
      for (String in_path : paths) {
        // Get the PathInfo for the path,
        PathAccess path_access = lock_db.get(in_path);
        // We have to be available to continue,
        if (path_access.isSynchronized()) {
          PathInfo path_info = path_access.getPathInfo();
          // If it's null, we haven't finished initialization yet,
          if (path_info != null) {
            // Check if the path info is dependent on the service that changed
            // status.
            ServiceAddress[] path_servers = path_info.getRootServers();
            boolean is_dependent = false;
            for (ServiceAddress ps : path_servers) {
              if (ps.equals(root_service)) {
                is_dependent = true;
                break;
              }
            }
            // If the path is dependent on the root service that changed
            // status
            if (is_dependent) {
              int available_count = 1;
              // Check availability.
              for (int i = 0; i < path_servers.length; ++i) {
                ServiceAddress paddr = path_servers[i];
                if (!paddr.equals(this_service) &&
                    service_tracker.isServiceUp(paddr, "root")) {
                  ++available_count;
                }
              }
              // If less than a majority available,
              if (available_count <= path_servers.length / 2) {
                // Mark the path is unavailable and add to the path info queue,
                path_access.markAsNotAvailable();
                desynced_paths.add(path_info);
              }
            }
          }
        }
      }
    }

    // Add the desync'd paths to the queue,
    synchronized (load_path_info_queue) {
      load_path_info_queue.addAll(desynced_paths);
    }

    // Log message for desyncing,
    if (log.isLoggable(Level.INFO)) {
      StringBuilder b = new StringBuilder();
      for (int i = 0 ; i < desynced_paths.size(); ++i) {
        b.append(desynced_paths.get(i).getPathName());
        b.append(", ");
      }
      log.log(Level.INFO, "COMPLETE: desync paths {0} at {1}",
                    new Object[] { b.toString(),
                                   this_service.displayString() });
    }

  }

  /**
   * Creates a unique time based uid.
   */
  private long createUID() {
    // PENDING: Normalize time over all the servers?
    long time_ms = System.currentTimeMillis();
    return time_ms;
  }

  /**
   * Returns the PathAccess object for the given path_name. Guarentees that
   * there is only one PathAccess per path name.
   */
  private PathAccess getPathAccess(String path_name) {

    // Check the name given is valid,
    checkPathNameValid(path_name);

    // Fetch the lock object for the given path name,
    PathAccess path_file;

    synchronized (lock_db) {
      path_file = lock_db.get(path_name);
      if (path_file == null) {
        path_file = new PathAccess(path_name);
        lock_db.put(path_name, path_file);
      }
      return path_file;
    }
  }

//  /**
//   * Loads a PathAccess from the local summary file. Caches the object.
//   */
//  private PathAccess loadPathAccess(String name) throws IOException {
//    // Fetch the lock object for the given path name,
//    PathAccess path_file;
//
//    synchronized (lock_db) {
//      path_file = lock_db.get(name);
//      if (path_file == null) {
//        // Not found in the local map, so read it from the file system.
//        File f = new File(path, name);
//        // If it doesn't exist, generate an error
//        if (!f.exists()) {
//          throw new RuntimeException(
//                  "Path '" + name + "' not found on root server (" +
//                  this_service.displayString() + ")");
//        }
//        // If it does exist, does the .deleted file exist indicating this root
//        // path was removed,
//        if (new File(path, name + ".deleted").exists()) {
//          throw new RuntimeException(
//                  "Path '" + name + "' did exist but was deleted.");
//        }
//        // Read the summary data for this path.
//        File summary_f = new File(path, name + ".summary");
//        Properties p = new Properties();
//        FileInputStream file_in = new FileInputStream(summary_f);
//        p.load(file_in);
//        file_in.close();
//
//        // The consensus function name
//        String consensus_class = p.getProperty("consensus_function");
//
//        // Format it into a PathAccess object,
//        path_file = new PathAccess(f, name, consensus_class);
//
//        // Put it in the local map
//        lock_db.put(name, path_file);
//      }
//    }
//    return path_file;
//  }
//
//  /**
//   * Fetches the PathAccess object for the given path name. Generates an
//   * exception if it's not found in the local file system.
//   */
//  private PathAccess fetchPathAccess(String path_name) throws IOException {
//    // Load the path access,
//    PathAccess path_file = loadPathAccess(path_name);
////    // Synchronize on the root access lock,
////    synchronized (path_file.root_access_lock) {
////      // Ask the managers for the current root servers/leader for the path and
////      // set it locally,
////      if (!path_file.root_info_current) {
////
////        ServiceAddress[] man_srvs = manager_servers;
////
////        // Query all the available manager servers for the path info,
////        ArrayList<PathInfo> paths = new ArrayList();
////        for (int i = 0; i < man_srvs.length; ++i) {
////          ServiceAddress manager_service = man_srvs[i];
////          if (service_tracker.isServiceUp(manager_service, "manager")) {
////            Message m =
////              comm.doFailableManagerFunction(manager_service,
////                                             "internalGetPathInfo", path_name);
////            if (!m.isError()) {
////              paths.add((PathInfo) m.param(0));
////            }
////          }
////        }
////
////        // Pick the majority path info
////        PathInfo path_info = pickMajorityPathInfo(paths, man_srvs.length);
////
////        // Set the root server details,
////        path_file.root_leader = path_info.getRootLeader();
////        path_file.root_servers = path_info.getRootServers();
////        path_file.root_info_current = true;
////
////      }
////    }
//    return path_file;
//  }

  /**
   * Notifies all the root servers of the path that the proposal was posted.
   */
  private void notifyAllRootServersOfPost(PathInfo path_info,
                          long uid, DataAddress root_node) throws IOException {

    // The root servers for the path,
    ServiceAddress[] roots = path_info.getRootServers();

    // Create the message,
    MessageStream msg_out = new MessageStream(16);
    msg_out.addMessage("notifyNewProposal");
    msg_out.addString(path_info.getPathName());
    msg_out.addLong(uid);
    msg_out.addDataAddress(root_node);
    msg_out.closeMessage();

    for (int i = 0; i < roots.length; ++i) {
      ServiceAddress machine = roots[i];
      // Don't notify this service,
      if (!machine.equals(this_service)) {
        // If the service is up in the tracker,
        if (service_tracker.isServiceUp(machine, "root")) {
          // Send the message to the service,
          MessageProcessor processor = network.connectRootServer(machine);
          ProcessResult msg_in = processor.process(msg_out);
          // If return is a connection fault,
          Iterator<Message> it = msg_in.iterator();
          while (it.hasNext()) {
            Message m = it.next();
            if (m.isError() && ReplicatedValueStore.isConnectionFault(m)) {
              service_tracker.reportServiceDownClientReport(machine, "root");
            }
          }
        }
      }
    }

  }

  /**
   * Posts the given DataAddress item to the path file with the given
   * timestamp.
   */
  private void postToPath(PathInfo path_info, DataAddress root_node)
                                                           throws IOException {

    // We can't post if this service is not the root leader,
    if (!path_info.getRootLeader().equals(this_service)) {
      log.log(Level.SEVERE, "Failed, {0} is not root leader for {1}",
              new Object[] { this_service.displayString(),
                             path_info.getPathName() });
      throw new RuntimeException(
              "Can't post update, this root service (" +
              this_service.displayString() +
              ") is not the root leader for the path: " +
              path_info.getPathName());
    }

    // Fetch the path access object for the given name.
    PathAccess path_file = getPathAccess(path_info.getPathName());

    // Only allow post if complete and synchronized
    path_file.checkIsSynchronized();

    // Create a unique time based uid.
    long uid = createUID();

    // Post the data address to the path,
    path_file.postProposalToPath(uid, root_node);

    // Notify all the root servers of this post,
    notifyAllRootServersOfPost(path_info, uid, root_node);

  }

  /**
   * Returns the last DataAddress posted to the given path access.
   */
  private DataAddress getPathLast(PathInfo path_info) throws IOException {

    // Fetch the path access object for the given name.
    PathAccess path_file = getPathAccess(path_info.getPathName());

    // Returns the last entry
    return path_file.getPathLast();

  }

  /**
   * Returns a historical subset of root nodes for the database path between
   * the given points in time. This is guarenteed to return DataAddress
   * objects regardless of whether there were commit operations at the given
   * time or not. The only case when this returns an empty array is when
   * there are no roots stored for the path.
   */
  private DataAddress[] getHistoricalPathRoots(PathInfo path_info,
                          long time_start, long time_end) throws IOException {

    // Fetch the path access object for the given name.
    PathAccess path_file = getPathAccess(path_info.getPathName());

    // Returns the roots
    return path_file.getHistoricalPathRoots(time_start, time_end);

  }

  /**
   * Returns the set of all paths roots that were published since the given
   * root, but not including the root. Returns an empty array if the given
   * root is the current root (there are no previously saved versions).
   * <p>
   * The set is ordered from last to first (the newest version is first in the
   * list).
   */
  private DataAddress[] getPathRootsSince(PathInfo path_info,
                                DataAddress root) throws IOException {

    // Fetch the path access object for the given name.
    PathAccess path_file = getPathAccess(path_info.getPathName());

    return path_file.getPathRootsSince(root);

  }

//  /**
//   * Adds a consensus processor to this root server that can service commit
//   * requests on a database.
//   */
//  private void addConsensusProcessor(String path_name, String fun,
//                                   DataAddress base_root) throws IOException {
//
//    // Check the name given is valid,
//    checkPathNameValid(path_name);
//
//    PathAccess path_file;
//    synchronized (lock_db) {
//      path_file = lock_db.get(path_name);
//      if (path_file != null) {
//        // If it's in the local map, generate an error,
//        throw new RuntimeException("Path '" + path_name + "' already exists.");
//      }
//      // If it's not in the map, check if the file exists,
//      File f = new File(path, path_name);
//      if (f.exists()) {
//        // If it's in the local map, generate an error,
//        throw new RuntimeException("Path file for '" + path_name +
//                                   "' exists on this root server.");
//      }
//      // Otherwise, it's ok to add it,
//
//      // Create the root file
//      f.createNewFile();
//      // Create a summary file for storing information about the path
//      File summary_f = new File(path, path_name + ".summary");
//      Properties p = new Properties();
//      p.setProperty("consensus_function", fun);
//      FileOutputStream file_out = new FileOutputStream(summary_f);
//      p.store(file_out, null);
//      file_out.close();
//    }
//
//    if (base_root != null) {
//      // Finally publish the base_root to the path,
//      // Create a basic PathInfo for publishing on this service only. Note that
//      // this is fine for an internal installation function.
//      PathInfo path_info = new PathInfo(path_name,
//                          this_service, new ServiceAddress[] { this_service });
//      postToPath(path_info, base_root);
//    }
//
//  }

//  /**
//   * Removes the consensus processor from this root server if its defined.
//   */
//  private void removeConsensusProcessor(String path_name) throws IOException {
//
//    // Check the name given is valid,
//    checkPathNameValid(path_name);
//
//    synchronized (lock_db) {
//      // Check the file exists,
//      File f = new File(path, path_name);
//      if (!f.exists()) {
//        // If it's not in the local map, generate an error,
//        throw new RuntimeException("Path file for '" + path_name +
//                                   "' doesn't exist on this root server.");
//      }
//      // We simply add a '.delete' file to indicate it's deleted
//      File del_file = new File(path, path_name + ".deleted");
//      del_file.createNewFile();
//
//      // Remove it from the map if it's there
//      lock_db.remove(path_name);
//    }
//
//  }

//  /**
//   * Sets the root leader for the given path name to the given root service.
//   */
//  private void internalSetLeader(String path_name,
//                               ServiceAddress root_leader) throws IOException {
//
//    // Check the name given is valid,
//    checkPathNameValid(path_name);
//
//    synchronized (lock_db) {
//      // Check the file exists,
//      File f = new File(path, path_name);
//      if (!f.exists()) {
//        // If it's not in the local map, generate an error,
//        throw new RuntimeException("Path file for '" + path_name +
//                                   "' doesn't exist on this root server.");
//      }
//
//      // root leader string,
//      String root_leader_str = root_leader.formatString();
//
//      // Read the summary data for this path.
//      File summary_f = new File(path, path_name + ".summary");
//      Properties p = new Properties();
//      FileInputStream file_in = new FileInputStream(summary_f);
//      p.load(file_in);
//      file_in.close();
//
//      // Set the root leader,
//      p.setProperty("root_leader", root_leader_str);
//      // Write out the changed summary file for the path,
//      FileOutputStream file_out = new FileOutputStream(summary_f);
//      p.store(file_out, null);
//      file_out.close();
//
//      // Set the leader in the path access,
//      PathAccess path_access = fetchPathAccess(path_name);
//      path_access.root_leader = root_leader;
//
//    }
//
//  }

//  /**
//   * List all the active paths on this root server. This is intended as an
//   * administrators function. The interaction with the file system needed for
//   * this operation may take a while.
//   */
//  private void consensusProcessorReport(MessageStream reply_message)
//                                                           throws IOException {
//
//    ArrayList<String> out_list = new ArrayList();
//    ArrayList<String> deleted_list = new ArrayList();
//
//    synchronized (lock_db) {
//      File[] all_files = path.listFiles();
//      for (File file : all_files) {
//        if (file.isFile()) {
//          String fname = file.getName();
//          if (fname.endsWith(".deleted")) {
//            deleted_list.add(fname.substring(0, fname.length() - 8));
//          }
//          else if (!fname.endsWith(".summary") &&
//                   !fname.endsWith(".properties")) {
//            out_list.add(fname);
//          }
//        }
//      }
//    }
//
//    // Remove the deleted items,
//    for (String n : deleted_list) {
//      out_list.remove(n);
//    }
//
//    // Turn it into a path set
//    String[] path_set = new String[out_list.size()];
//    path_set = out_list.toArray(path_set);
//    // Now query each path name and find out its consensus function and make
//    // that into a new array,
//    String[] funs_set = new String[path_set.length];
//    for (int i = 0; i < path_set.length; ++i) {
//      String ipath = path_set[i];
//      // Fetch the path summary file
//      PathAccess path_file = fetchPathAccess(ipath);
//      // Get the consensus_processor class name
//      funs_set[i] = path_file.consensus_proc_name;
//    }
//
//    // Add the information into the array,
//    reply_message.addMessage("R");
//    reply_message.addStringArr(path_set);
//    reply_message.addStringArr(funs_set);
//    reply_message.closeMessage();
//
//  }

  /**
   * Returns the name of the consensus function for the path name managed by
   * this root.
   */
  private String getConsensusProcessor(String path_name) throws IOException {
    // Check the name given is valid,
    checkPathNameValid(path_name);

    // Fetch the path access object for the given name.
    PathAccess path_file = getPathAccess(path_name);

    // Return the processor name
    return path_file.getPathInfo().getConsensusFunction();
  }

  /**
   * Performs the commit operation on the given path with the given proposal.
   */
  private DataAddress performCommit(PathInfo path_info, DataAddress proposal)
                                     throws IOException, CommitFaultException {

    ServiceAddress[] man_srvs = manager_servers;

    // Fetch the path access object for the given name.
    PathAccess path_file = getPathAccess(path_info.getPathName());

    ConsensusProcessor consensus_proc;
    try {
      consensus_proc = path_file.getConsensusProcessor();
    }
    catch (ClassNotFoundException e) {
      throw new CommitFaultException("Class not found: {0}", e.getMessage());
    }
    catch (InstantiationException e) {
      throw new CommitFaultException("Class instantiation exception: {0}", e.getMessage());
    }
    catch (IllegalAccessException e) {
      throw new CommitFaultException("Illegal Access exception: {0}", e.getMessage());
    }

    // Create the connection object (should be fairly lightweight)
    LocalNetworkCache local_net_cache =
                 JVMState.getJVMCacheForManager(man_srvs, cache_configuration);
    ConsensusDDBConnection connection =
            new LFSRSConnection(path_info, network,
                                man_srvs, local_net_cache, service_tracker);

    // Perform the commit,
    return consensus_proc.commit(connection, proposal);

  }

  /**
   * Returns the stats for the given snapshot.
   */
  private String iGetSnapshotStats(PathInfo path_info, DataAddress snapshot)
                                                          throws IOException {

    ServiceAddress[] man_srvs = manager_servers;

    // Fetch the path access object for the given name.
    PathAccess path_file = getPathAccess(path_info.getPathName());

    ConsensusProcessor consensus_proc;
    try {
      consensus_proc = path_file.getConsensusProcessor();
    }
    catch (ClassNotFoundException e) {
      throw new RuntimeException("Class not found: " + e.getMessage());
    }
    catch (InstantiationException e) {
      throw new RuntimeException("Class instantiation exception: " + e.getMessage());
    }
    catch (IllegalAccessException e) {
      throw new RuntimeException("Illegal Access exception: " + e.getMessage());
    }

    // Create the connection object (should be fairly lightweight)
    LocalNetworkCache local_net_cache =
                 JVMState.getJVMCacheForManager(man_srvs, cache_configuration);
    ConsensusDDBConnection connection =
            new LFSRSConnection(path_info, network,
                                man_srvs, local_net_cache, service_tracker);

    // Generate and return the stats string,
    return consensus_proc.getStats(connection, snapshot);
  }

  /**
   * Checks if the given consensus processor object is valid and can be
   * instantiated. Throws an exception if not.
   */
  private void checkConsensusClass(String fun) {
    try {
      Class c = Class.forName(fun);
      ConsensusProcessor processor = (ConsensusProcessor) c.newInstance();
      processor.getName();
    }
    catch (ClassNotFoundException e) {
      throw new RuntimeException("Class not found: " + e.getMessage());
    }
    catch (InstantiationException e) {
      throw new RuntimeException("Class instantiation exception: " + e.getMessage());
    }
    catch (IllegalAccessException e) {
      throw new RuntimeException("Illegal Access exception: " + e.getMessage());
    }
  }

  /**
   * Returns the stats for the given path.
   */
  private String iGetPathStats(PathInfo path_info) throws IOException {
    return iGetSnapshotStats(path_info, getPathLast(path_info));
  }

  /**
   * Calls the initialize function of the consensus processor on the given
   * path.
   */
  private void initializePath(PathInfo path_info) throws IOException {

    ServiceAddress[] man_srvs = manager_servers;

    // Fetch the path access object for the given name.
    PathAccess path_file = getPathAccess(path_info.getPathName());

    ConsensusProcessor consensus_proc;
    try {
      consensus_proc = path_file.getConsensusProcessor();
    }
    catch (ClassNotFoundException e) {
      throw new RuntimeException("Class not found: " + e.getMessage());
    }
    catch (InstantiationException e) {
      throw new RuntimeException("Class instantiation exception: " + e.getMessage());
    }
    catch (IllegalAccessException e) {
      throw new RuntimeException("Illegal Access exception: " + e.getMessage());
    }

    // Create the connection object (should be fairly lightweight)
    LocalNetworkCache local_net_cache =
                 JVMState.getJVMCacheForManager(man_srvs, cache_configuration);
    // Make the connection on the database
    ConsensusDDBConnection connection =
            new LFSRSConnection(path_info, network,
                                man_srvs, local_net_cache, service_tracker);

    // Make an initial empty database for the path,
    // PENDING: We could keep a cached version of this image, but it's
    //   miniscule in size.
    NetworkTreeSystem tree_system =
                      new NetworkTreeSystem(network, man_srvs,
                                            local_net_cache, service_tracker);
    tree_system.setMaximumNodeCacheHeapSize(1 * 1024 * 1024);
    DataAddress empty_db_addr = tree_system.createEmptyDatabase();
    // Publish the empty state to the path,
    connection.publishToPath(empty_db_addr);
    // Call the initialize function,
    consensus_proc.initialize(connection);
  }

  /**
   * Loads a PathInfo into this root server, assuming that this root server is
   * either the root leader or a root server of the path. This goes through the
   * initialization procedure for setting up this service as a source point for
   * the path.
   * <p>
   * Works for both cases when we are setting up a fresh new root server for a
   * path, or we are reintroducing a server that served for the path but was
   * down and missed proposal notifications.
   * <p>
   * The steps followed are; 1) Check availability, if this path initializing
   * will not constitute a majority of available servers for the path then we
   * defer loading until a majority is available. 2) Each available server is
   * contacted and data is replicated. 3) The path is marked available.
   */
  private void loadPathInfo(final PathInfo path_info) throws IOException {

    synchronized (load_path_info_lock) {

      PathAccess path_file = getPathAccess(path_info.getPathName());
      path_file.setInitialPathInfo(path_info);

//      // Fail if there's already local data for this path,
//      if (path_file.hasLocalData()) {
//        log.log(Level.SEVERE,
//                "Local data for path {0} already exists on root server at {1}",
//                new Object[] { path_info.getPathName(),
//                               this_service.displayString() });
//        throw new RuntimeException(
//                "Local data for path already exists on root server");
//      }

      synchronized (load_path_info_queue) {
        if (!load_path_info_queue.contains(path_info)) {
          load_path_info_queue.add(path_info);
        }
      }

      ServiceAddress[] root_servers = path_info.getRootServers();

      // Poll all the root servers managing the path,
      pollAllRootMachines(root_servers);

      // Check availability,
      int available_count = 0;
      for (ServiceAddress addr : root_servers) {
        if (service_tracker.isServiceUp(addr, "root")) {
          ++available_count;
        }
      }

      // Majority not available?
      if (available_count <= (root_servers.length / 2)) {
        log.log(Level.INFO, "Majority of root servers unavailable, " +
                            "retrying loadPathInfo later");

        // Leave the path info on the load_path_info_queue, therefore it will
        // be retried when the root server is available again,

        return;
      }

      log.log(Level.INFO, "loadPathInfo on path {0} at {1}",
                  new Object[] { path_info.getPathName(),
                                 this_service.displayString() });

      // Create the local data object if not present,
      path_file.openLocalData();

      // Sync counter starts at 1, because we know we are self replicated.
      int sync_counter = 1;

      // Synchronize with each of the available root servers (but not this),
      for (ServiceAddress addr : root_servers) {
        if (!addr.equals(this_service)) {
          if (service_tracker.isServiceUp(addr, "root")) {
            boolean success = synchronizePathInfoData(path_file, addr);
            // If we successfully synchronized, increment the counter,
            if (success == true) {
              ++sync_counter;
            }
          }
        }
      }

//      log.log(Level.FINEST, "sync_counter = {0}", sync_counter);

      // Remove from the queue if we successfully sync'd with a majority of
      // the root servers for the path,
      if (sync_counter > path_info.getRootServers().length / 2) {
        // Replay any proposals that were incoming on the path, and mark the
        // path as synchronized/available,
        path_file.markAsAvailable();

        synchronized (load_path_info_queue) {
          load_path_info_queue.remove(path_info);
        }

        log.log(Level.INFO, "COMPLETE: loadPathInfo on path {0} at {1}",
                    new Object[] { path_info.getPathName(),
                                   this_service.displayString() });
      }

    }

  }

  /**
   * RPC when the root leader accepts a proposal.
   */
  private void notifyNewProposal(String path_name, long uid,
                                 DataAddress node) throws IOException {

    // Fetch the path access object for the given name.
    PathAccess path_file = getPathAccess(path_name);

    // Go tell the PathAccess
    path_file.notifyNewProposal(uid, node);

  }

  /**
   * Fetches a bundle of entries from the given record in the path. Used for
   * initial synchronization only.
   */
  private Object[] internalFetchPathDataBundle(String path_name,
                              long uid, DataAddress addr) throws IOException {

    // Fetch the path access object for the given name.
    PathAccess path_file = getPathAccess(path_name);
    // Init the local data if we need to,
    path_file.openLocalData();

    // Fetch 256 entries,
    return path_file.fetchPathDataBundle(uid, addr, 256);

  }

  /**
   * Contacts the given root server and copies any data from the path to this
   * root server. Returns true if we successfully synchronized, false if we
   * failed to synchronize with the root.
   */
  private boolean synchronizePathInfoData(PathAccess path_file,
                              ServiceAddress root_server) throws IOException {

    // Get the last entry,
    PathRecordEntry last_entry = path_file.getLastEntry();

    long uid;
    DataAddress daddr;

    if (last_entry == null) {
      uid = 0;
      daddr = null;
    }
    else {
      uid = last_entry.uid;
      daddr = last_entry.addr;
    }

    while (true) {

      // Fetch a bundle for the path from the root server,
      MessageStream msg_out = new MessageStream(8);
      msg_out.addMessage("internalFetchPathDataBundle");
      msg_out.addString(path_file.path_name);
      msg_out.addLong(uid);
      msg_out.addDataAddress(daddr);
      msg_out.closeMessage();

      // Send the command
      MessageProcessor processor = network.connectRootServer(root_server);
      ProcessResult result = processor.process(msg_out);

      long[] uids = null;
      DataAddress[] data_addrs = null;

      Iterator<Message> it = result.iterator();
      while (it.hasNext()) {
        Message m = it.next();
        if (m.isError()) {
          // If it's a connection fault, report the error and return false
          if (ReplicatedValueStore.isConnectionFault(m)) {
            service_tracker.reportServiceDownClientReport(root_server, "root");
            return false;
          }
          else {
            throw new RuntimeException(m.getErrorMessage());
          }
        }
        else {
          uids = (long[]) m.param(0);
          data_addrs = (DataAddress[]) m.param(1);
        }
      }

      // If it's empty, we reached the end so return,
      if (uids == null || uids.length == 0) {
        break;
      }

      // Insert the data
      path_file.addPathDataEntries(uids, data_addrs);

      // The last,
      uid = uids[uids.length - 1];
      daddr = data_addrs[data_addrs.length - 1];

    }

    return true;
  }

  /**
   * Inform this root server of the manager server addresses.
   */
  private void informOfManagers(ServiceAddress[] manager_servers)
                                                          throws IOException {

    StringBuilder b = new StringBuilder();
    for (int i = 0; i < manager_servers.length; ++i) {
      b.append(manager_servers[i].formatString());
      if (i < manager_servers.length - 1) {
        b.append(",");
      }
    }

    // Write the manager server address to the properties file,
    Properties p = new Properties();
    p.setProperty("manager_server_address", b.toString());

    // Contains the root properties,
    File prop_file = new File(path, "00.properties");
    FileOutputStream fout = new FileOutputStream(prop_file);
    p.store(fout, null);
    fout.close();

    this.manager_servers = manager_servers;
  }

  /**
   * Clear this root server of all manager entries.
   */
  private void clearOfManagers() throws IOException {

    // Write the manager server address to the properties file,
    Properties p = new Properties();
    p.setProperty("manager_server_address", "");

    // Contains the root properties,
    File prop_file = new File(path, "00.properties");
    FileOutputStream fout = new FileOutputStream(prop_file);
    p.store(fout, null);
    fout.close();

    this.manager_servers = new ServiceAddress[0];
  }


//  /**
//   * Binds this root server with the given manager server.
//   */
//  private void iBindWithManager(ServiceAddress manager) throws IOException {
////    if (this.manager_server != null) {
////      throw new RuntimeException(
////                  "This root server is already bound to a manager server");
////    }
//
//    ServiceAddress[] man_srvs = manager_servers;
//
//    int nsz = (man_srvs == null) ? 0 : man_srvs.length;
//    ServiceAddress[] copy_arr = new ServiceAddress[nsz + 1];
//    if (man_srvs != null) {
//      for (int i = 0; i < nsz; ++i) {
//        if (man_srvs[i].equals(manager)) {
//          throw new RuntimeException("Manager already bound to this root");
//        }
//        copy_arr[i] = man_srvs[i];
//      }
//    }
//    copy_arr[nsz] = manager;
//    StringBuilder b = new StringBuilder();
//    for (int i = 0; i < copy_arr.length; ++i) {
//      b.append(copy_arr[i].formatString());
//      if (i < copy_arr.length - 1) {
//        b.append(",");
//      }
//    }
//
//    // Write the manager server address to the properties file,
//    Properties p = new Properties();
//    p.setProperty("manager_server_address", b.toString());
//
//    // Contains the root properties,
//    File prop_file = new File(path, "00.properties");
//    FileOutputStream fout = new FileOutputStream(prop_file);
//    p.store(fout, null);
//    fout.close();
//
//    this.manager_servers = copy_arr;
//  }
//
//  /**
//   * Unbinds this root server with the given manager server.
//   */
//  private void iUnbindWithManager(ServiceAddress manager) throws IOException {
//
//    ServiceAddress[] man_srvs = manager_servers;
//
//    if (man_srvs == null || man_srvs.length == 0) {
//      throw new RuntimeException(
//                          "This root server is not bound to a manager server");
//    }
//
//    int nsz = (man_srvs == null) ? 0 : man_srvs.length;
//    ArrayList<ServiceAddress> copy_arr = new ArrayList();
//    if (man_srvs != null) {
//      for (int i = 0; i < nsz; ++i) {
//        if (!man_srvs[i].equals(manager)) {
//          copy_arr.add(manager);
//        }
//      }
//    }
//    if (copy_arr.size() == nsz) {
//      throw new RuntimeException("Manager not bound to this root.");
//    }
//    StringBuilder b = new StringBuilder();
//    for (int i = 0; i < copy_arr.size(); ++i) {
//      b.append(copy_arr.get(i).formatString());
//      if (i < copy_arr.size() - 1) {
//        b.append(",");
//      }
//    }
//
//    // Write the manager server address to the properties file,
//    Properties p = new Properties();
//    p.remove("manager_server_address");
//
//    // Contains the root properties,
//    File prop_file = new File(path, "00.properties");
//    FileOutputStream fout = new FileOutputStream(prop_file);
//    p.store(fout, null);
//    fout.close();
//
//    this.manager_servers =
//                        copy_arr.toArray(new ServiceAddress[copy_arr.size()]);
//  }

  /**
   * Queries the managers for the path name and version. If the version
   * provided does not match the most recent the manager has in its database
   * then null is returned. Also if the majority decided path info can not
   * be determined then null is returned.
   */
  private PathInfo loadFromManagers(String path_name,
                                    int path_info_version) {

    ServiceAddress[] man_srvs = manager_servers;

    PathInfo path_info = null;
    boolean found_one = false;

    // Query all the available manager servers for the path info,
    for (int i = 0; i < man_srvs.length && path_info == null; ++i) {
      ServiceAddress manager_service = man_srvs[i];
      if (service_tracker.isServiceUp(manager_service, "manager")) {

        MessageStream msg_out = new MessageStream(32);
        msg_out.addMessage("getPathInfoForPath");
        msg_out.addString(path_name);
        msg_out.closeMessage();

        MessageProcessor processor =
                                 network.connectManagerServer(manager_service);
        ProcessResult result = processor.process(msg_out);
        Iterator<Message> it = result.iterator();
        Message last_m = null;
        while (it.hasNext()) {
          Message m = it.next();
          if (m.isError()) {
            if (!ReplicatedValueStore.isConnectionFault(m)) {
              // If it's not a connection fault, we rethrow the error,
              throw new RuntimeException(m.getErrorMessage());
            }
            else {
              service_tracker.reportServiceDownClientReport(
                                                   manager_service, "manager");
            }
          }
          else {
            last_m = m;
            found_one = true;
          }
        }

        if (last_m != null) {
          path_info = (PathInfo) last_m.param(0);
        }
      }
    }

    // If not received a valid reply from a manager service, generate exception
    if (found_one == false) {
      throw new ServiceNotConnectedException("Managers not available");
    }

    // Create and return the path info object,
    return path_info;

//    // Ask the managers for the current root servers/leader for the path and
//    // set it locally,
//
//    ServiceAddress[] man_srvs = manager_servers;
//
//    int try_count = 0;
//
//    PathInfo path_info = null;
//
//    while (path_info == null && try_count < 2) {
//      // Query all the available manager servers for the path info,
//      ArrayList<PathInfo> paths = new ArrayList();
//      for (int i = 0; i < man_srvs.length; ++i) {
//        ServiceAddress manager_service = man_srvs[i];
//        if (service_tracker.isServiceUp(manager_service, "manager")) {
//          // Returns null if the given version not current,
//          Message m =
//            comm.doFailableManagerFunction(manager_service,
//                          "internalGetPathInfo", path_name, path_info_version);
//          if (!m.isError()) {
//            paths.add((PathInfo) m.param(0));
//          }
//        }
//      }
//
//      // Pick the majority path info. Throws InvalidPathInfoException if a
//      // majority path info is not currently available,
//      try {
//        path_info =
//              DefaultManagerServer.pickMajorityAgreedPathInfo(
//                                                       paths, man_srvs.length);
//      }
//      catch (InvalidPathInfoException e) {
//        // Rethrow if we tried several times to pick a path info.
//        if (try_count >= 2) {
//          throw e;
//        }
//        // Wait 25ms
//        try {
//          Thread.currentThread().wait(25);
//        }
//        catch (InterruptedException e2) { /* ignore */ }
//      }
//
//      // We try a number of times incase we happen to be in a race condition
//      // where manager servers are in the process of being updated.
//      ++try_count;
//    }
//
//    // Create and return the path info object,
//    return new RSPathInfo(path_info, path_info_version);
  }

  /**
   * Given a path name and version number, returns a PathInfo object that
   * describes the root leader and root servers for the path. If the version
   * number doesn't match the latest version known by this root server, an
   * exception is generated. This exception should force the client to update
   * its own knowledge about a path. This method also generates an exception
   * if the given path name is not managed by this service.
   */
  private PathInfo getPathInfo(String path_name, int path_info_version) {
    synchronized (path_info_map) {
      PathInfo rs_path_info = path_info_map.get(path_name);
      if (rs_path_info == null) {
        rs_path_info = loadFromManagers(path_name, path_info_version);
        if (rs_path_info == null) {
          throw new InvalidPathInfoException("Unable to load from managers");
        }
        path_info_map.put(path_name, rs_path_info);
      }
      // If it's out of date,
      if (rs_path_info.getVersionNumber() != path_info_version) {
        throw new InvalidPathInfoException("Path info version out of date");
      }
      return rs_path_info;
    }
  }

  /**
   * Sets PathInfo and version information for the given path name. This is
   * called by the manager when a path has a new version.
   */
  private void internalSetPathInfo(String path_name, int path_info_version,
                                   PathInfo path_info) {
    synchronized (path_info_map) {
      path_info_map.put(path_name, path_info);
    }
    // Set the path info in the path access object,
    PathAccess path_file = getPathAccess(path_name);
    path_file.setPathInfo(path_info);
  }

  /**
   * Called when a virtual machine error is caught, causing the server to go
   * into a stop state.
   */
  private void enterStopState(VirtualMachineError e) {
    stop_state = e;
  }

  /**
   * If in a stop state, throw a general error detailing the error.
   */
  private void checkStopState() {
    Throwable se = stop_state;
    if (se != null) {
      throw new Error("Stop State: " + se.getMessage(), se);
    }
  }

  
  
  /**
   * Returns a MessageProcessor object for communicating with this root server.
   */
  public MessageProcessor getProcessor() {
    return new LFSRootServerProcessor();
  }
  
  
  
  // ----- Inner classes -----

  /**
   * Represents an access path of the database.
   */
  private class PathAccess {

    /**
     * The path name.
     */
    private final String path_name;

    /**
     * The current PathInfo object for this path access. This is updated
     * immediately when the path info is changed in the manager cluster, so
     * it represents the most current path knowledge.
     */
    private PathInfo path_info;

    /**
     * An instantiation of the consensus processor for this path.
     */
    private ConsensusProcessor consensus_processor;

    /**
     * The RandomAccessFile that is used to read and write to the file.
     */
    private RandomAccessFile internal_file;

    /**
     * The StrongPagedAccess object used to access the data in the path.
     */
    private StrongPagedAccess paged_file;

    /**
     * The last DataAddress posted, or null if none posted.
     */
    private DataAddress last_data_address;

    /**
     * The queue of unapplied proposals, formatted as a tuple (long[] uid,
     * DataAddress root_node).
     */
    private final ArrayList proposal_queue;


    private final Object access_lock = new Object();


    /**
     * Set to true when initialization is complete and the state of the path's
     * data is known to be synchronized with the path dependents.
     */
    private boolean complete_and_synchronized = false;

    /**
     * Set to true when the path info is initially set.
     */
    private boolean path_info_set = false;


    /**
     * Temporary work buffer.
     */
    private byte[] buf = new byte[32];
    
    /**
     * Constructor.
     */
    private PathAccess(String path_name) {
      this.path_name = path_name;
      this.proposal_queue = new ArrayList();
    }


    /**
     * Performs a binary search on a path list finding the position of the
     * record closest to the given time.
     */
    private long binarySearch(final StrongPagedAccess access,
                           long low, long high,
                           final long search_uid) throws IOException {
      while (low <= high) {
        long mid = (low + high) >> 1;

        long pos = mid * ROOT_ITEM_SIZE;
        long mid_uid = access.readLong(pos);
        
        if (mid_uid < search_uid) { //mid_timestamp < timestamp) {
          low = mid + 1;
        }
        else if (mid_uid > search_uid) { //mid_timestamp > timestamp) {
          high = mid - 1;
        }
        else {
          return mid;
        }
      }
      return -(low + 1);
    }

    /**
     * Returns true if there is data for the path name stored locally.
     */
    private boolean hasLocalData() {
      File path_data_file = new File(path, path_name);
      if (path_data_file.exists()) {
        return true;
      }
      return false;
    }

    /**
     * Returns true if the path is synchronized.
     */
    private boolean isSynchronized() {
      synchronized (access_lock) {
        return complete_and_synchronized;
      }
    }

    /**
     * Throws an exception if this path access is not complete and synchronized
     * (it's going through the initialization procedure).
     */
    private void checkIsSynchronized() {
      synchronized (access_lock) {
        if (!complete_and_synchronized) {
          String path_info_name =
                  (path_info == null) ? path_name : path_info.getPathName();
          throw new RuntimeException(MessageFormat.format(
                  "Path {0} on root server {1} is not available",
                  path_info_name, this_service.displayString()));
        }
      }
    }

    /**
     * Sets the path info only if it has not ever been set before.
     */
    private void setInitialPathInfo(PathInfo path_info) {
      synchronized (access_lock) {
//        System.out.println(" *** SETINITIALPATHINFO(" + path_info + ")");
        if (path_info_set == false && this.path_info == null) {
          path_info_set = true;
          setPathInfo(path_info);
        }
      }
    }

    /**
     * Sets the path info for this path access.
     */
    private void setPathInfo(PathInfo path_info) {
      synchronized (access_lock) {
        this.path_info = path_info;
        this.consensus_processor = null;
      }
    }

    /**
     * Returns the current path info.
     */
    private PathInfo getPathInfo() {
      synchronized (access_lock) {
        return path_info;
      }
    }

    /**
     * Opens the initial local data in the file system for this path. If
     * data is not present then it is opened in an initial state.
     */
    private void openLocalData() throws IOException {
      synchronized (access_lock) {
        if (internal_file == null) {
          internal_file = new RandomAccessFile(new File(path, path_name), "rw");
          paged_file = new StrongPagedAccess(internal_file, 1024);
        }
      }
    }

    /**
     * Adds a proposal to the end of the path data.
     */
    private void postProposalToPath(long uid, DataAddress root_node)
                                                           throws IOException {

      // Synchronize over the random access path file,
      synchronized (access_lock) {

        // Write the root node entry to the end of the file,
        RandomAccessFile f = internal_file;
        long pos = f.length();
        f.seek(pos);
        ByteArrayUtil.setLong(uid, buf, 0);
        NodeReference node_ref = root_node.getValue();
        ByteArrayUtil.setLong(node_ref.getHighLong(), buf, 8);
        ByteArrayUtil.setLong(node_ref.getLowLong(), buf, 16);
        f.write(buf, 0, ROOT_ITEM_SIZE);
        paged_file.invalidateSection(pos, ROOT_ITEM_SIZE);

//        // Sync the file,
//        try {
//          f.getFD().sync();
//        }
//        catch (SyncFailedException e) {
//          // We ignore this exception, the underlying system doesn't support
//          // file sync.
//        }
        last_data_address = root_node;
      }
    }

    /**
     * Post the proposal if the uid is not present. Note that the search
     * procedure is not exact, so there is the posibility that the record is
     * already in the path and this will add a second entry.
     */
    private void postProposalIfNotPresent(
                       long uid, DataAddress root_node) throws IOException {

      synchronized (access_lock) {

        long set_size = internal_file.length() / ROOT_ITEM_SIZE;

        // The position of the uid,
        long pos = binarySearch(paged_file, 0, set_size - 1, uid);
        if (pos < 0) {
          pos = -(pos + 1);
        }
        // Crudely clear the cache if it has reached a certain threshold,
        paged_file.clearIfOverSize(4);

        // Go back some entries
        pos = Math.max(0, pos - 256);

        // Search,
        while (true) {
          if (pos >= set_size) {
            // End if pos is greater or equal to the size,
            break;
          }
          long read_uid = paged_file.readLong((pos * ROOT_ITEM_SIZE) + 0);
          long node_h = paged_file.readLong((pos * ROOT_ITEM_SIZE) + 8);
          long node_l = paged_file.readLong((pos * ROOT_ITEM_SIZE) + 16);
          NodeReference node = new NodeReference(node_h, node_l);
          DataAddress data_address = new DataAddress(node);
          // Crudely clear the cache if it's reached a certain threshold,
          paged_file.clearIfOverSize(4);

          // Found, so return
          if (uid == read_uid && root_node.equals(data_address)) {
            return;
          }

          ++pos;
        }

        // Not found, so post
        postProposalToPath(uid, root_node);

      }
    }

    /**
     * Posts the bundle of entries to the path, provided the entries are
     * not already present in the set.
     */
    private void addPathDataEntries(long[] uids, DataAddress[] addrs)
                                                          throws IOException {
      synchronized (access_lock) {
        int sz = uids.length;
        for (int i = 0; i < sz; ++i) {
          long uid = uids[i];
          DataAddress addr = addrs[i];
          // Post the proposal if it's not present
          postProposalIfNotPresent(uid, addr);
        }
      }
    }

    /**
     * Replays any pending messages on the path and then marks the path as
     * complete and available.
     */
    private void markAsAvailable() throws IOException {
      synchronized (access_lock) {
        int sz = proposal_queue.size() / 2;
        for (int i = 0; i < sz; ++i) {
          // Fetch the uid and root_node pair,
          long uid = (Long) proposal_queue.get((i * 2));
          DataAddress root_node =
                       (DataAddress) proposal_queue.get((i * 2) + 1);

          // Search for the uid, if it's not in the list then we post it to
          // the path.
          postProposalIfNotPresent(uid, root_node);

        }

        // Clear the proposal queue
        proposal_queue.clear();
        // Complete this path instance,
        complete_and_synchronized = true;

      }
    }

    /**
     * Marks the path as unavailable. This is called when it is found a
     * majority of root servers in the path are not available.
     */
    private void markAsNotAvailable() {
      synchronized (access_lock) {
        complete_and_synchronized = false;
      }
    }


//    /**
//     * Posts a root node to the internal file.
//     */
//    private void postToPath(DataAddress root_node) throws IOException {
//      // Synchronize over the random access path file,
//      synchronized (access_lock) {
//
//        // Only allow post if complete and synchronized
//        checkIsSynchronized();
//
//        // Write the root node entry to the end of the file,
//        RandomAccessFile f = internal_file;
//        long pos = f.length();
//        f.seek(pos);
//        ByteArrayUtil.setLong(System.currentTimeMillis(), buf, 0);
//        NodeReference node_ref = root_node.getValue();
//        ByteArrayUtil.setLong(node_ref.getHighLong(), buf, 8);
//        ByteArrayUtil.setLong(node_ref.getLowLong(), buf, 16);
//        f.write(buf, 0, ROOT_ITEM_SIZE);
//        paged_file.invalidateSection(pos, ROOT_ITEM_SIZE);
//
////        // Sync the file,
////        try {
////          f.getFD().sync();
////        }
////        catch (SyncFailedException e) {
////          // We ignore this exception, the underlying system doesn't support
////          // file sync.
////        }
//        last_data_address = root_node;
//      }
//    }

    /**
     * Fetches a bundle of entries from the path, making a guess on where the
     * initial entry should be found, but reverting to an exhaustive search
     * if not found. If the given entry is not found, then an empty bundle
     * is returned. Otherwise 'bundle_size' entries from after the record
     * are returned.
     */
    private Object[] fetchPathDataBundle(
               long from_uid, DataAddress from_addr, int bundle_size)
                                                          throws IOException {

      ArrayList<PathRecordEntry> entries_al = new ArrayList(bundle_size);

      // Synchronize over the object
      synchronized (access_lock) {
        // Check near the end (most likely to be found there),

        long set_size = internal_file.length() / ROOT_ITEM_SIZE;

        long search_s;
        long found = -1;
        long pos;
        if (from_uid > 0) {
          // If from_uid is real,
          pos = binarySearch(paged_file,
                             0, set_size - 1, from_uid);
          pos = Math.max(0, pos - 256);

          search_s = pos;

          // Search to the end,
          while (true) {
            // End condition,
            if (pos >= set_size) {
              break;
            }

            PathRecordEntry entry = getEntryAt(pos * ROOT_ITEM_SIZE);
            if (entry.uid == from_uid && entry.addr.equals(from_addr)) {
              // Found!
              found = pos;
              break;
            }
            ++pos;
          }

        }
        else {
          // If from_uid is less than 0, it indicates to fetch the bundle of
          // path entries from the start.
          pos = -1;
          found = 0;
          search_s = 0;
        }

        // If not found,
        if (found < 0) {
          // Try from search_s to 0
          pos = search_s - 1;
          while (true) {
            // End condition,
            if (pos < 0) {
              break;
            }

            PathRecordEntry entry = getEntryAt(pos * ROOT_ITEM_SIZE);
            if (entry.uid == from_uid && entry.addr.equals(from_addr)) {
              // Found!
              found = pos;
              break;
            }
            --pos;
          }
        }

        // Go to the next entry,
        ++pos;

        // Still not found, or at the end
        if (found < 0 || pos >= set_size) {
          return new Object[] { new long[0], new DataAddress[0] };
        }

        // Fetch a bundle of records from the position
        while (true) {
          // End condition,
          if (pos >= set_size || entries_al.size() >= bundle_size) {
            break;
          }

          PathRecordEntry entry = getEntryAt(pos * ROOT_ITEM_SIZE);
          entries_al.add(entry);

          ++pos;
        }
      }

      // Format it as long[] and DataAddress[] arrays
      int sz = entries_al.size();
      long[] uids = new long[sz];
      DataAddress[] addrs = new DataAddress[sz];
      for (int i = 0; i < sz; ++i) {
        uids[i] = entries_al.get(i).uid;
        addrs[i] = entries_al.get(i).addr;
      }
      return new Object[] { uids, addrs };

    }

    /**
     * Return the entry at the given absolute position in the file.
     */
    private PathRecordEntry getEntryAt(long pos) throws IOException {
      // Synchronize over the object
      synchronized (access_lock) {
        // Read the long at the position of the root node reference,
        long uid = paged_file.readLong(pos);
        long root_node_ref_h = paged_file.readLong(pos + 8);
        long root_node_ref_l = paged_file.readLong(pos + 16);
        NodeReference root_node_ref =
                           new NodeReference(root_node_ref_h, root_node_ref_l);
        DataAddress data_address = new DataAddress(root_node_ref);
        // Clear the cache if it's over a certain size,
        paged_file.clearIfOverSize(4);

        return new PathRecordEntry(uid, data_address);
      }

    }

    /**
     * Return the last entry in the path.
     */
    private PathRecordEntry getLastEntry() throws IOException {
      // Synchronize over the object
      synchronized (access_lock) {
        // Read the root node entry from the end of the file,
        long pos = internal_file.length();
        if (pos == 0) {
          // Nothing in the file, so return null
          return null;
        }
        else {
          return getEntryAt(pos - ROOT_ITEM_SIZE);
        }
      }
    }

    /**
     * Return the last root node stored for this path.
     */
    private DataAddress getPathLast() throws IOException {
      // Synchronize over the object
      synchronized (access_lock) {

        // Only allow if complete and synchronized
        checkIsSynchronized();

        if (last_data_address == null) {

          // Read the last entry from the file,
          PathRecordEntry r = getLastEntry();
          if (r == null) {
            return null;
          }

          // Cache the DataAddress part,
          last_data_address = r.addr;
        }

        return last_data_address;
      }
    }

    /**
     * Returns a historical subset of root nodes for the database path between
     * the given points in time. This is guarenteed to return DataAddress
     * objects regardless of whether there were commit operations at the given
     * time or not. The only case when this returns an empty array is when
     * there are no roots stored for the path.
     */
    private DataAddress[] getHistoricalPathRoots(
                          long time_start, long time_end) throws IOException {

      ArrayList<DataAddress> nodes = new ArrayList();
      synchronized (access_lock) {

        // Only allow if complete and synchronized
        checkIsSynchronized();

        // We perform a binary search for the start and end time in the set of
        // root nodes since the records are ordered by time. Note that the key
        // is a timestamp obtained by System.currentTimeMillis() that could
        // become out of order if the system time is changed or other
        // misc time synchronization oddities. Because of this, we consider the
        // records 'roughly' ordered and it should be noted the result may not
        // be exactly correct.

        long set_size = internal_file.length() / ROOT_ITEM_SIZE;

        long start_p = binarySearch(paged_file,
                                    0, set_size - 1, time_start);
        long end_p = binarySearch(paged_file,
                                  0, set_size - 1, time_end);
        if (start_p < 0) {
          start_p = -(start_p + 1);
        }
        if (end_p < 0) {
          end_p = -(end_p + 1);
        }
        // Crudely clear the cache if it has reached a certain threshold,
        paged_file.clearIfOverSize(4);

        if (start_p >= end_p - 1) {
          start_p = end_p - 2;
          end_p = end_p + 2;
        }

        start_p = Math.max(0, start_p);
        end_p = Math.min(set_size, end_p);

        // Return the records,
        while (true) {
          if (start_p > end_p || start_p >= set_size) {
            // End if start has reached the end,
            break;
          }
          long node_h = paged_file.readLong((start_p * ROOT_ITEM_SIZE) + 8);
          long node_l = paged_file.readLong((start_p * ROOT_ITEM_SIZE) + 16);
          NodeReference node = new NodeReference(node_h, node_l);
          nodes.add(new DataAddress(node));
          // Crudely clear the cache if it's reached a certain threshold,
          paged_file.clearIfOverSize(4);

          ++start_p;
        }
      }

      // Return the nodes array,
      return nodes.toArray(new DataAddress[nodes.size()]);
    }

    /**
     * Returns the set of all paths roots that were published since the given
     * root, but not including the root. Returns an empty array if the given
     * root is the current root (there are no previously saved versions).
     * <p>
     * The set is ordered from last to first (the newest version is first in the
     * list).
     */
    private DataAddress[] getPathRootsSince(
                                        DataAddress root) throws IOException {

      // The returned list,
      ArrayList<DataAddress> root_list = new ArrayList(6);

      // Synchronize over the object
      synchronized (access_lock) {

        // Only allow if complete and synchronized
        checkIsSynchronized();

        boolean found = false;

        // Start position at the end of the file,
        long pos = internal_file.length();

        while (found == false && pos > 0) {
          // Iterate backwards,
          pos = pos - ROOT_ITEM_SIZE;
          // Read the root node for this record,
          long root_node_ref_h = paged_file.readLong(pos + 8);
          long root_node_ref_l = paged_file.readLong(pos + 16);
          NodeReference root_node_ref =
                            new NodeReference(root_node_ref_h, root_node_ref_l);
          // Crudely clear the cache if it's reached a certain threshold,
          paged_file.clearIfOverSize(4);
          // Did we find the root node?
          DataAddress root_node_da = new DataAddress(root_node_ref);
          if (root_node_da.equals(root)) {
            found = true;
          }
          else {
            root_list.add(root_node_da);
          }
        }

        // If not found, report error
        if (!found) {
          // Bit of an obscure error message. This basically means we didn't
          // find the root node requested in the path file.
          throw new RuntimeException("Root not found in version list");
        }

        // Return the array as a list,
        return root_list.toArray(new DataAddress[root_list.size()]);

      }

    }

    /**
     * Notification from a remote root server that a proposal has been accepted
     * by the leader.
     */
    private void notifyNewProposal(long uid, DataAddress proposal)
                                                           throws IOException {
      synchronized (access_lock) {
        if (!complete_and_synchronized) {
          // If this isn't complete, we add to the queue
          proposal_queue.add(uid);
          proposal_queue.add(proposal);
          return;
        }

        // Otherwise add the entry,
        postProposalToPath(uid, proposal);

      }
    }

    /**
     * Returns the consensus processor object for this path.
     */
    private ConsensusProcessor getConsensusProcessor()
                                            throws ClassNotFoundException,
                                                   InstantiationException,
                                                   IllegalAccessException {

      synchronized (access_lock) {
        ConsensusProcessor consensus_proc = consensus_processor;
        if (consensus_proc == null) {
          // Instantiate a new class object for the commit,
          Class c = Class.forName(path_info.getConsensusFunction());
          ConsensusProcessor processor = (ConsensusProcessor) c.newInstance();

          // Attach it with the PathAccess object,
          consensus_processor = processor;
          consensus_proc = processor;
        }
        return consensus_proc;
      }
    }

  }
  
  
  
  /**
   * A root server processor object that processes message streams.
   */
  private class LFSRootServerProcessor extends AbstractProcessor {
    
    /**
     * Processes messages.
     */
    @Override
    public MessageStream process(MessageStream message_stream) {
      // The reply message,
      MessageStream reply_message = new MessageStream(32);

      // The messages in the stream,
      Iterator<Message> iterator = message_stream.iterator();
      while (iterator.hasNext()) {
        Message m = iterator.next();
        try {
          checkStopState();

          // publishPath(String path_name, long path_info_version,
          //             DataAddress)
          if (m.getName().equals("publishPath")) {
            publishPath((String) m.param(0), (Integer) m.param(1),
                        (DataAddress) m.param(2));
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }
          // DataAddress getPathNow(String path_name, long path_info_version)
          else if (m.getName().equals("getPathNow")) {
            DataAddress data_address =
                        getPathNow((String) m.param(0), (Integer) m.param(1));
            reply_message.addMessage("R");
            reply_message.addDataAddress(data_address);
            reply_message.closeMessage();
          }
          // DataAddress[] getPathHistorical(String path_name,
          //                                 long path_info_version
          //                                 long time_start, long time_end)
          else if (m.getName().equals("getPathHistorical")) {
            DataAddress[] data_addresses = getPathHistorical(
                    (String) m.param(0),
                    (Integer) m.param(1), (Long) m.param(2), (Long) m.param(3));
            reply_message.addMessage("R");
            reply_message.addDataAddressArr(data_addresses);
            reply_message.closeMessage();
          }
          // long getCurrentTimeMillis()
          else if (m.getName().equals("getCurrentTimeMillis")) {
            long time_millis = System.currentTimeMillis();
            reply_message.addMessage("R");
            reply_message.addLong(time_millis);
            reply_message.closeMessage();
          }





//          // addConsensusProcessor(String path, String fun, DataAddress addr)
//          else if (m.getName().equals("addConsensusProcessor")) {
//            addConsensusProcessor((String) m.param(0),
//                                  (String) m.param(1),
//                                  (DataAddress) m.param(2));
//            reply_message.addMessage("R");
//            reply_message.addInteger(1);
//            reply_message.closeMessage();
//          }
//          // removeConsensusProcessor(String path)
//          else if (m.getName().equals("removeConsensusProcessor")) {
//            removeConsensusProcessor((String) m.param(0));
//            reply_message.addMessage("R");
//            reply_message.addInteger(1);
//            reply_message.closeMessage();
//          }
//          // consensusProcessorReport()
//          else if (m.getName().equals("consensusProcessorReport")) {
//            consensusProcessorReport(reply_message);
//          }

          // commit(String path, long path_info_version, DataAddress proposal)
          else if (m.getName().equals("commit")) {
            DataAddress result =
                  commit((String) m.param(0), (Integer) m.param(1),
                         (DataAddress) m.param(2));

            reply_message.addMessage("R");
            reply_message.addDataAddress(result);
            reply_message.closeMessage();
          }

          // getConsensusProcessor(String path)
          else if (m.getName().equals("getConsensusProcessor")) {
            String consensus_proc = getConsensusProcessor((String) m.param(0));
            reply_message.addMessage("R");
            reply_message.addString(consensus_proc);
            reply_message.closeMessage();
          }

          // initialize(String path,
          //            long path_info_version)
          else if (m.getName().equals("initialize")) {
            initialize((String) m.param(0), (Integer) m.param(1));
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }

          // getPathStats(String path, long path_info_version)
          else if (m.getName().equals("getPathStats")) {
            String stats = getPathStats((String) m.param(0), (Integer) m.param(1));
            reply_message.addMessage("R");
            reply_message.addString(stats);
            reply_message.closeMessage();
          }
          // getSnapshotStats(String path, long path_info_Version,
          //                  DataAddress snapshot)
          else if (m.getName().equals("getSnapshotStats")) {
            String stats = getSnapshotStats(
                    (String) m.param(0), (Integer) m.param(1),
                    (DataAddress) m.param(2));
            reply_message.addMessage("R");
            reply_message.addString(stats);
            reply_message.closeMessage();
          }

          // checkConsensusClass(String fun)
          else if (m.getName().equals("checkConsensusClass")) {
            checkConsensusClass((String) m.param(0));
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }

          // informOfManagers(ServiceAddress[] manager_servers)
          else if (m.getName().equals("informOfManagers")) {
            informOfManagers((ServiceAddress[]) m.param(0));
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }
          // clearOfManagers()
          else if (m.getName().equals("clearOfManagers")) {
            clearOfManagers();
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }

//          // bindWithManager()
//          else if (m.getName().equals("bindWithManager")) {
//            bindWithManager((ServiceAddress) m.param(0));
//            reply_message.addMessage("R");
//            reply_message.addInteger(1);
//            reply_message.closeMessage();
//          }
//          // unbindWithManager()
//          else if (m.getName().equals("unbindWithManager")) {
//            unbindWithManager((ServiceAddress) m.param(0));
//            reply_message.addMessage("R");
//            reply_message.addInteger(1);
//            reply_message.closeMessage();
//          }


          // loadPathInfo(PathInfo path_info)
          else if (m.getName().equals("loadPathInfo")) {
            loadPathInfo((PathInfo) m.param(0));
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }
          // notifyNewProposal(String path_name, long uid, DataAddress)
          else if (m.getName().equals("notifyNewProposal")) {
            notifyNewProposal((String) m.param(0),
                              (Long) m.param(1), (DataAddress) m.param(2));
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }
          // internalSetPathInfo(String path_name, long ver, PathInfo path_info)
          else if (m.getName().equals("internalSetPathInfo")) {
            internalSetPathInfo((String) m.param(0), (Integer) m.param(1),
                                (PathInfo) m.param(2));
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }
          // internalFetchPathDataBundle(String path_name,
          //                             long uid, DataAddress addr)
          else if (m.getName().equals("internalFetchPathDataBundle")) {
            Object[] r = internalFetchPathDataBundle((String) m.param(0),
                                 (Long) m.param(1), (DataAddress) m.param(2));
            reply_message.addMessage("R");
            reply_message.addLongArray((long[]) r[0]);
            reply_message.addDataAddressArr((DataAddress[]) r[1]);
            reply_message.closeMessage();
          }

          // poll()
          else if (m.getName().equals("poll")) {
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }

          else {
            throw new RuntimeException("Unknown command: " + m.getName());
          }

        }
        catch (VirtualMachineError e) {
          log.log(Level.SEVERE, "VM Error", e);
          enterStopState(e);
          throw e;
        }
        catch (Throwable e) {
          log.log(Level.SEVERE, "Exception during process", e);
          reply_message.addMessage("E");
          reply_message.addExternalThrowable(new ExternalThrowable(e));
          reply_message.closeMessage();
        }
      }

      return reply_message;

    }

    /**
     * Records the given root node as the most recent version of the database
     * state as returned by the 'getPathNow' call. Note that, although this
     * operation is atomic, the atomicity is not intended to determine the
     * consensus to which of multiple conflicting database states is current,
     * rather the means in which to 'publish' a new state to the rest of the
     * network.
     * <p>
     * With this in mind, there is an implied process that must happen before
     * a new version of the database is published by this call to ensure a
     * change is consistant with the rules of the database schema. While
     * this process happens, new states can not be published (well, actually
     * that depends on the updates).
     * <p>
     * What typically will happen in a commit process is a change to the
     * database is proposed, the commit process locks, the commit process
     * merges the changes from the proposed transaction into the most recent
     * version. If there are conflicts the commit fails, otherwise the merged
     * transaction is made the current version, either case the commit process
     * then unlocks.
     * <p>
     * Sound complicated? It is, and a tough problem to solve in any environment
     * let alone a distributed fault-tolerant one.
     * <p>
     * The 'path_info_version' value is used to determine if the client has a
     * current version of the PathInfo. If there is a version mismatch an error
     * code is generated.
     */
    private void publishPath(String path_name, int path_info_version,
                             DataAddress root_node) throws IOException {

      // Find the PathInfo object from the path_info_version. If the path
      // version is out of date then an exception is generated.
      PathInfo path_info = getPathInfo(path_name, path_info_version);

      postToPath(path_info, root_node);
    }

    /**
     * Returns the current root node address of the database with the given
     * path name. This returns the last root node address published via a call
     * to 'publishPath'. This operation is atomic, however the atomicity is not
     * intended to determine the consensus of which state is the most current.
     * It is intended to ensure that a process that requires stateful access
     * can be assured that the value was the last published if the 'publish'
     * and 'get' methods occurred sequentially (via a lock on publish).
     * <p>
     * Note that due to latency, the returned root node may be out of date by
     * the time it is processed if publishing new states is permitted (publish
     * hasn't been locked by another process). Therefore if this call is part
     * of a process that must consider the current database state, a global
     * lock process is implied, held by the calling method, that must prevent
     * publishing new states.
     */
    private DataAddress getPathNow(String path_name, int path_info_version)
                                                          throws IOException {

      // Find the PathInfo object from the path_info_version. If the path
      // version is out of date then an exception is generated.
      PathInfo path_info = getPathInfo(path_name, path_info_version);

      return getPathLast(path_info);
    }
    
    /**
     * Returns a list of historical root node addresses that have been
     * published between the given points in time. An historical root node can
     * be published to a path, or used to create a new path (this will branch
     * the database state into a new isolated branch).
     * <p>
     * Note that historical information may periodically be cleaned from the
     * system, however, those rules are not defined within the scope of this
     * specification.
     */
    private DataAddress[] getPathHistorical(
                   String path_name, int path_info_version,
                   long time_start, long time_end) throws IOException {
      // Find the PathInfo object from the path_info_version. If the path
      // version is out of date then an exception is generated.
      PathInfo path_info = getPathInfo(path_name, path_info_version);

      return getHistoricalPathRoots(path_info, time_start, time_end);
    }

    /**
     * Initializes the path.
     */
    private void initialize(String path_name, int path_info_version)
                                                           throws IOException {
      // Find the PathInfo object from the path_info_version. If the path
      // version is out of date then an exception is generated.
      PathInfo path_info = getPathInfo(path_name, path_info_version);

      initializePath(path_info);
    }

    /**
     * Performs the commit operation.
     */
    private DataAddress commit(String path_name, int path_info_version,
               DataAddress proposal) throws IOException, CommitFaultException {

      // Find the PathInfo object from the path_info_version. If the path
      // version is out of date then an exception is generated.
      PathInfo path_info = getPathInfo(path_name, path_info_version);

      return performCommit(path_info, proposal);
    }

//    /**
//     * Binds this root server with the given manager server. A root server may
//     * only be bound to one or no manager servers.
//     */
//    private void bindWithManager(ServiceAddress manager_server)
//                                                           throws IOException {
//      iBindWithManager(manager_server);
//    }
//
//    /**
//     * Unbinds this root server with the given manager server. A root server
//     * may only be bound to one or no manager servers.
//     */
//    private void unbindWithManager(ServiceAddress manager_server)
//                                                           throws IOException {
//      iUnbindWithManager(manager_server);
//    }

    private String getSnapshotStats(String path_name, int path_info_version,
                                    DataAddress address) throws IOException {

      // Find the PathInfo object from the path_info_version. If the path
      // version is out of date then an exception is generated.
      PathInfo path_info = getPathInfo(path_name, path_info_version);

      return iGetSnapshotStats(path_info, address);
    }

    private String getPathStats(String path_name, int path_info_version)
                                                         throws IOException {

      // Find the PathInfo object from the path_info_version. If the path
      // version is out of date then an exception is generated.
      PathInfo path_info = getPathInfo(path_name, path_info_version);

      return iGetPathStats(path_info);
    }

  }

  // ---------- Inner classes ----------

  /**
   * The implementation of ConsensusDDBConnection passed to consensus
   * processors for processing of a database proposal.
   */
  private class LFSRSConnection implements ConsensusDDBConnection {

    /**
     * The database path.
     */
    private PathInfo path;

    /**
     * The network tree system manager object.
     */
    private NetworkTreeSystem tree_system;


    /**
     * Constructor.
     */
    private LFSRSConnection(PathInfo path,
                            NetworkConnector network_connector,
                            ServiceAddress[] manager_servers,
                            LocalNetworkCache local_network_cache,
                            ServiceStatusTracker tracker) {

      this.path = path;
      this.tree_system = new NetworkTreeSystem(
             network_connector, manager_servers, local_network_cache, tracker);

    }

    private RuntimeException handleIOError(IOException e) {
      return new RuntimeException("IO Error: " + e.getMessage());
    }


    // ---------- Implemented methods ----------

    @Override
    public void publishToPath(DataAddress root_node) {
      try {
        postToPath(path, root_node);
      }
      catch (IOException e) {
        throw handleIOError(e);
      }
    }

    @Override
    public DataAddress getCurrentSnapshot() {
      try {
        return getPathLast(path);
      }
      catch (IOException e) {
        throw handleIOError(e);
      }
    }

    @Override
    public DataAddress[] getHistoricalSnapshots(long time_start,
                                                long time_end) {
      try {
        return getHistoricalPathRoots(path, time_start, time_end);
      }
      catch (IOException e) {
        throw handleIOError(e);
      }
    }

    @Override
    public DataAddress[] getSnapshotsSince(DataAddress root_node) {
      try {
        return getPathRootsSince(path, root_node);
      }
      catch (IOException e) {
        throw handleIOError(e);
      }
    }

    @Override
    public KeyObjectTransaction createTransaction(DataAddress root_node) {
      return tree_system.createTransaction(root_node);
    }

    @Override
    public DataAddress flushTransaction(KeyObjectTransaction transaction) {
      return tree_system.flushTransaction(transaction);
    }

  }

  private static class PathRecordEntry {
    private long uid;
    private DataAddress addr;
    public PathRecordEntry(long uid, DataAddress addr) {
      this.uid = uid;
      this.addr = addr;
    }
  }

}
