/**
 * com.mckoi.network.NetworkTreeSystem  Nov 27, 2008
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

import com.mckoi.data.*;
import com.mckoi.store.AreaWriter;
import com.mckoi.util.ByteArrayUtil;
import java.io.*;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.CRC32;

/**
 * An implementation of TreeSystem in which the tree nodes are stored over
 * a Mckoi Network setup. This implementation does not perform reference
 * counting for leaf nodes, instead relying on a garbage collection process
 * to determine node accessibility and cleaning resources as necessary.
 * <p>
 * NetworkTreeSystem manages a version of the database tree for a client,
 * and allows free writing of nodes of the tree into the network block
 * system. Each client machine will typically create one instance of this
 * object to which the client application server can interact with the network.
 * The client application may make all sorts of different requirements of
 * this object, such as many concurrent transactions, etc.
 * <p>
 * This object manages a network backed node cache. It is intended that this
 * object is alive over the entire period the client application server is
 * alive for, and expensive one-time initialization should be anticipated by
 * the developer.
 *
 * @author Tobias Downer
 */

class NetworkTreeSystem implements TreeSystem {

  /**
   * The type identifiers for branch and leaf nodes in the tree.
   */
  private static final short STORE_LEAF_TYPE   = 0x019e0;
  private static final short STORE_BRANCH_TYPE = 0x022e0;
  
  /**
   * The NetworkConnector object this object uses to talk with the servers
   * on the network.
   */
  private final NetworkConnector connector;

  /**
   * The network address of the manager servers.
   */
  private final ServiceAddress[] manager_addresses;

  /**
   * A local node cache for nodes in the network.
   */
  private final LocalNetworkCache local_network_cache;

  /**
   * A tracker of service availability on the network.
   */
  private final ServiceStatusTracker service_tracker;

  /**
   * A HashMap used to control failure notifications.
   */
  private final HashMap<ServiceAddress, Long> failure_flood_control;
  private final HashMap<ServiceAddress, Long> failure_flood_control_bidc;

  /**
   * Maximum transaction node heap size use (defaults to 32MB).
   */
  private long max_transaction_node_heap_size = 32 * 1024 * 1024;



  // ---------- Stop condition handling ----------

  /**
   * This is set to the error in the case of a VM error or IOException that is
   * a critical stopping condition.
   */
  private volatile CriticalStopError critical_stop_error = null;

  /**
   * For debugging.
   */
  public volatile long network_comm_count = 0;
  public volatile long network_fetch_comm_count = 0;

  // ---------- Logging ----------

  /**
   * The Logger object.
   */
  private final Logger log;

  /**
   * Constructor.
   */
  NetworkTreeSystem(NetworkConnector connector,
                    ServiceAddress[] manager_addresses,
                    LocalNetworkCache local_network_cache,
                    ServiceStatusTracker service_tracker) {
    this.connector = connector;
    this.manager_addresses = manager_addresses;
    this.local_network_cache = local_network_cache;
    this.service_tracker = service_tracker;
    this.failure_flood_control = new HashMap();
    this.failure_flood_control_bidc = new HashMap();

    log = Logger.getLogger("com.mckoi.network.Log");

  }


  /**
   * Returns true if the message is a connection failure message.
   */
  private static boolean isConnectionFailMessage(Message m) {
    // PENDING: This should detect comm failure rather than catch-all.
    if (m.isError()) {
      ExternalThrowable et = m.getExternalThrowable();
      String error_class_name = et.getClassName();
      // If it's a connect exception,
      if (error_class_name.equals("java.net.ConnectException")) {
        return true;
      }
      else if (error_class_name.equals(
                          "com.mckoi.network.ServiceNotConnectedException")) {
        return true;
      }
    }

    return false;
  }

  /**
   * Notify all the manager servers, ignoring any errors or connection
   * failures (used for event notification only).
   */
  private void notifyAllManagers(MessageStream msg_out) {

    ProcessResult[] msg_ins = new ProcessResult[manager_addresses.length];
    for (int i = 0; i < manager_addresses.length; ++i) {
      ServiceAddress manager = manager_addresses[i];
      if (service_tracker.isServiceUp(manager, "manager")) {
        MessageProcessor processor = connector.connectManagerServer(manager);
        msg_ins[i] = processor.process(msg_out);
      }
    }

    for (int i = 0; i < manager_addresses.length; ++i) {
      ProcessResult msg_in = msg_ins[i];
      if (msg_in != null) {
        ServiceAddress manager = manager_addresses[i];
        // If any errors happened,
        for (Message m : msg_in) {
          // If it's a connection fail message, we try connecting to another
          // manager.
          if (isConnectionFailMessage(m)) {
            // Tell the tracker it's down,
            service_tracker.reportServiceDownClientReport(manager, "manager");
            break;
          }
        }
      }
    }

  }

  /**
   * Processes a single manager role command on the first manager server
   * currently available.
   */
  public ProcessResult processManager(MessageStream msg_out) {

    // We go through all the manager addresses from first to last until we
    // find one that is currently up,

    // This uses a service status tracker object maintained by the network
    // cache to keep track of manager servers that aren't operational.

    ProcessResult msg_in = null;
    for (int i = 0; i < manager_addresses.length; ++i) {
      ServiceAddress manager = manager_addresses[i];
      if (service_tracker.isServiceUp(manager, "manager")) {
        MessageProcessor processor = connector.connectManagerServer(manager);
        msg_in = processor.process(msg_out);

        boolean failed = false;
        // If any errors happened,
        for (Message m : msg_in) {
          // If it's a connection fail message, we try connecting to another
          // manager.
          if (isConnectionFailMessage(m)) {
            // Tell the tracker it's down,
            service_tracker.reportServiceDownClientReport(manager, "manager");
            failed = true;
            break;
          }
        }

        if (!failed) {
          return msg_in;
        }
      }
    }

    // If we didn't even try one, we test the first manager.
    if (msg_in == null) {
      MessageProcessor processor =
                         connector.connectManagerServer(manager_addresses[0]);
      msg_in = processor.process(msg_out);
    }

    // All managers are currently down, so return the last msg_in,
    return msg_in;

  }

  /**
   * Processes a message on a single root server. This will always throw an
   * exception if the root service is not connected, or if the remote service
   * threw an InvalidPathInfoException.
   * <p>
   * This inspects the result for all connection failures, and rethrows them as
   * a 'ServiceNotConnectedException'. Also rethrows any
   * 'InvalidPathInfoException' generated by the service.
   */
  private Message processSingleRoot(
                           MessageStream msg_out, ServiceAddress root_server) {

    MessageProcessor processor = connector.connectRootServer(root_server);
    ProcessResult msg_in = processor.process(msg_out);
    Message last_m = null;
    for (Message m : msg_in) {
      last_m = m;
      if (m.isError()) {
        // If it's a connection failure, inform the service tracker and throw
        // service not available exception.
        if (isConnectionFailMessage(m)) {
          service_tracker.reportServiceDownClientReport(root_server, "root");
          throw new ServiceNotConnectedException(root_server.displayString());
        }

        String error_class_name = m.getExternalThrowable().getClassName();
        // Rethrow InvalidPathInfoException locally,
        if (error_class_name.equals(
                              "com.mckoi.network.InvalidPathInfoException")) {
          throw new InvalidPathInfoException(m.getErrorMessage());
        }
      }
    }
    return last_m;
  }


  /**
   * Returns the PathInfo for the given path name. This first checks the local
   * cache for the path info. If it's not found there, the manager cluster is
   * queried and the result is put in the cache.
   * <p>
   * Note that the returned PathInfo may be out of date, in which case
   * performing a function using the PathInfo will throw an
   * InvalidPathInfoException. If that happens, the cache should be wiped of
   * the path name, and next time this function is called a forced refresh
   * of the PathInfo from the manager server cluster will occur.
   */
  private PathInfo getPathInfoFor(String path_name) {
    PathInfo path_info = local_network_cache.getPathInfo(path_name);
    if (path_info == null) {
      // Path info not found in the cache, so query the manager cluster for the
      // info.

      MessageStream msg_out = new MessageStream(16);
      msg_out.addMessage("getPathInfoForPath");
      msg_out.addString(path_name);
      msg_out.closeMessage();

      ProcessResult msg_in = processManager(msg_out);

      for (Message m : msg_in) {
        if (m.isError()) {
          log.log(Level.SEVERE, "'getPathInfoFor' command failed: {0}",
                                m.getErrorMessage());
          log.log(Level.SEVERE, m.getExternalThrowable().getStackTrace());
          throw new RuntimeException(m.getErrorMessage());
        }
        else {
          path_info = (PathInfo) m.param(0);
        }
      }

      if (path_info == null) {
        throw new RuntimeException("Path not found: " + path_name);
      }

      // Put it in the local cache,
      local_network_cache.putPathInfo(path_name, path_info);
    }
    return path_info;
  }

  /**
   * Inserts nodes into the network to create an empty meta database. Returns
   * the address of the root node of the empty database in the network.
   */
  DataAddress createEmptyDatabase() throws IOException {

//    // Temporary node heap for creating a starting database
//    TreeNodeHeap node_heap = new TreeNodeHeap(17, 4 * 1024 * 1024);
//
//    // Write a root node to the store,
//    // Create an empty head node
//    TreeLeaf head_leaf = node_heap.createEmptyLeaf(null, Key.HEAD_KEY, 256);
//    // Insert a tree identification pattern
//    head_leaf.put(0, new byte[] { 1, 1, 1, 1 }, 0, 4);
//    // Create an empty tail node
//    TreeLeaf tail_leaf = node_heap.createEmptyLeaf(null, Key.TAIL_KEY, 256);
//    // Insert a tree identification pattern
//    tail_leaf.put(0, new byte[] { 1, 1, 1, 1 }, 0, 4);
//
//    // The write sequence,
//    TreeWriteSequence seq = new TreeWriteSequence();
//    seq.sequenceNodeWrite(head_leaf);
//    seq.sequenceNodeWrite(tail_leaf);
//    NodeReference[] refs = performTreeWrite(seq);
//
//    // Create a branch,
//    TreeBranch root_branch =
//                       node_heap.createEmptyBranch(null, getMaxBranchSize());
//    root_branch.set(refs[0], 4,
//                    Key.TAIL_KEY,
//                    refs[1], 4);
//
//    seq = new TreeWriteSequence();
//    seq.sequenceNodeWrite(root_branch);
//    refs = performTreeWrite(seq);

    // The child reference is a sparse node element
    NodeReference child_ref =
                           NodeReference.createSpecialSparseNode((byte) 1, 4);

    // Create a branch,
    TreeBranch root_branch = new TreeBranch(
                    NodeReference.createInMemoryNode(0L), getMaxBranchSize());
    root_branch.set(child_ref, 4,
                    Key.TAIL_KEY,
                    child_ref, 4);

    TreeWriteSequence seq = new TreeWriteSequence();
    seq.sequenceNodeWrite(root_branch);
    NodeReference[] refs = performTreeWrite(seq);

    // The written root node reference,
    final NodeReference root_id = refs[0];

//    // Delete the head and tail leaf, and the root branch
//    node_heap.delete(head_leaf.getReference());
//    node_heap.delete(tail_leaf.getReference());
//    node_heap.delete(root_branch.getReference());

    // Return the root,
    return new DataAddress(root_id);

  }

  /**
   * Create a transaction snapshot based on the given root node.
   */
  KeyObjectTransaction createTransaction(DataAddress root_node) {
    return new NetworkTreeSystemTransaction(this, 0, root_node);
  }

  /**
   * Creates an empty transaction snapshot.
   */
  KeyObjectTransaction createEmptyTransaction() {
    return new NetworkTreeSystemTransaction(this, 0);
  }

  /**
   * Flushes all updates made on the given transaction out to the network and
   * returns the root node that represents the flushed snapshot of the
   * database. This is used as part of the process to build a persistent
   * version.
   */
  DataAddress flushTransaction(KeyObjectTransaction transaction) {
    NetworkTreeSystemTransaction net_transaction =
                                   (NetworkTreeSystemTransaction) transaction;
    try {
      net_transaction.checkOut();
      return new DataAddress(net_transaction.getRootNodeRef());
    }
    catch (IOException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  /**
   * Performs a commit operation for the given proposal. The proposal is
   * represented as a DataAddress which is the result of a flushed transaction.
   * The proposal is formatted in such a way that the consensus processor
   * assigned for the path on the root server will understand how to process
   * it.
   */
  DataAddress performCommit(String path_name,
                     DataAddress proposal) throws CommitFaultException {

    // Get the PathInfo object for the given path name,
    PathInfo path_info = getPathInfoFor(path_name);

    // We can only commit on the root leader,
    ServiceAddress root_server = path_info.getRootLeader();
    try {
      // PENDING; If the root leader is not available, we need to go through
      //   a new leader election process.

      MessageStream msg_out = new MessageStream(16);
      msg_out.addMessage("commit");
      msg_out.addString(path_name);
      msg_out.addInteger(path_info.getVersionNumber());
      msg_out.addDataAddress(proposal);
      msg_out.closeMessage();

      Message m = processSingleRoot(msg_out, root_server);

      if (m.isError()) {
        // Rethrow commit fault locally,
        if (m.getExternalThrowable().getClassName().equals(
                       "com.mckoi.network.CommitFaultException")) {
          throw new CommitFaultException(m.getErrorMessage());
        }
        else {

          log.log(Level.SEVERE, "'performCommit' command failed: {0}",
                            m.getErrorMessage());
          log.log(Level.SEVERE, m.getExternalThrowable().getStackTrace());

          throw new RuntimeException(m.getErrorMessage());
        }
      }
      // Return the DataAddress of the result transaction,
      return (DataAddress) m.param(0);

    }
    catch (InvalidPathInfoException e) {
      // Clear the cache and requery the manager server for a new path info,
      local_network_cache.putPathInfo(path_name, null);
      return performCommit(path_name, proposal);
    }

  }

  /**
   * Queries the manager server and finds all active paths on the network.
   */
  String[] findAllPaths() {
    MessageStream msg_out = new MessageStream(16);
    msg_out.addMessage("getAllPaths");
    msg_out.closeMessage();

    // Process a command on the manager,
    ProcessResult msg_in = processManager(msg_out);

    for (Message m : msg_in) {
      if (m.isError()) {
        log.log(Level.SEVERE, "'getAllPaths' command failed: {0}",
                              m.getErrorMessage());
        log.log(Level.SEVERE, m.getExternalThrowable().getStackTrace());
        throw new RuntimeException(m.getErrorMessage());
      }
      else {
        return (String[]) m.param(0);
      }
    }
    throw new RuntimeException("Bad formatted message stream");
  }

  /**
   * Queries the network and returns the name of the consensus function for the
   * given path.
   */
  String getConsensusFunction(String path_name) {

    PathInfo path_info = getPathInfoFor(path_name);
    return path_info.getConsensusFunction();

  }

  /**
   * Internal method that fetches the current root from the root server.
   */
  private DataAddress internalGetPathNow(PathInfo path_info,
                                         ServiceAddress root_server) {
    MessageStream msg_out = new MessageStream(16);
    msg_out.addMessage("getPathNow");
    msg_out.addString(path_info.getPathName());
    msg_out.addInteger(path_info.getVersionNumber());
    msg_out.closeMessage();

    Message m = processSingleRoot(msg_out, root_server);
    if (m.isError()) {
      log.log(Level.SEVERE, "'internalGetPathNow' command failed: {0}",
                            m.getErrorMessage());
      log.log(Level.SEVERE, m.getExternalThrowable().getStackTrace());
      throw new RuntimeException(m.getErrorMessage());
    }
    return (DataAddress) m.param(0);
  }

  /**
   * Returns the DataAddress of the root node of the given path, or null if
   * there is no current root address. This may need to query the manager
   * server cluster for the latest information about the given path. If the
   * path name can not be resolved or the manager cluster is not available,
   * returns an exception.
   */
  DataAddress getPathNow(String path_name) {

    // Get the PathInfo object for the given path name,
    PathInfo path_info = getPathInfoFor(path_name);

    // Try the root leader first,
    ServiceAddress root_server = path_info.getRootLeader();
    try {
      DataAddress data_address = internalGetPathNow(path_info, root_server);

      // PENDING; if the root leader is not available, query the replicated
      //   root servers with this path.

      return data_address;

    }
    catch (InvalidPathInfoException e) {
      // Clear the cache and requery the manager server for a new path info,
      local_network_cache.putPathInfo(path_name, null);
      return getPathNow(path_name);
    }
  }

  /**
   * Internal method for fetching the historical root nodes of a path.
   */
  private DataAddress[] internalGetPathHistorical(PathInfo path_info,
                      ServiceAddress server, long time_start, long time_end) {

    MessageStream msg_out = new MessageStream(16);
    msg_out.addMessage("getPathHistorical");
    msg_out.addString(path_info.getPathName());
    msg_out.addInteger(path_info.getVersionNumber());
    msg_out.addLong(time_start);
    msg_out.addLong(time_end);
    msg_out.closeMessage();

    Message m = processSingleRoot(msg_out, server);
    if (m.isError()) {

      log.log(Level.SEVERE, "'internalGetPathHistorical' command failed: {0}",
                            m.getErrorMessage());
      log.log(Level.SEVERE, m.getExternalThrowable().getStackTrace());

      throw new RuntimeException(m.getErrorMessage());
    }

    return (DataAddress[]) m.param(0);

  }

  /**
   * Queries the root server for the given path name and returns the set of all
   * published versions of the database root node between the times given,
   * where the time values follow the conventions of System.currentTimeMillis().
   */
  DataAddress[] getPathHistorical(
                           String path_name, long time_start, long time_end) {

    // Get the PathInfo object for the given path name,
    PathInfo path_info = getPathInfoFor(path_name);

    // Try the root leader first,
    ServiceAddress root_server = path_info.getRootLeader();
    try {
      DataAddress[] data_addresses = internalGetPathHistorical(
                                 path_info, root_server, time_start, time_end);

      // PENDING; if the root leader is not available, query the replicated
      //   root servers with this path.

      return data_addresses;

    }
    catch (InvalidPathInfoException e) {
      // Clear the cache and requery the manager server for a new path info,
      local_network_cache.putPathInfo(path_name, null);
      return getPathHistorical(path_name, time_start, time_end);
    }

  }

  /**
   * Disposes the given transaction.
   */
  void disposeTransaction(KeyObjectTransaction transaction)
                                                          throws IOException {
    ((NetworkTreeSystemTransaction) transaction).doFullDispose();
  }


  /**
   * Sets the maximum node heap size that can be allocated by a single
   * transaction object.
   */
  void setMaximumNodeCacheHeapSize(long size_in_bytes) {
    max_transaction_node_heap_size = size_in_bytes;
  }


  /**
   * {@inhericDoc}
   */
  public void checkPoint() {
    // This is a 'no-op' for the network system. This is called when a cache
    // flush occurs, so one idea might be to use this as some sort of hint?
  }

  // ----- Implemented from TreeSystem -----
  
  /**
   * {@inhericDoc}
   */
  public int getMaxBranchSize() {
    // PENDING: Make this user-definable.
    // Remember though - you can't change this value on the fly so we'll need
    //   some central management on the network for configuration values.

//    // Note: 25 results in a branch size of around 1024 in size when full so
//    //   limits the maximum size of a branch to this size.
    return 14;
  }

  /**
   * {@inhericDoc}
   */
  public int getMaxLeafByteSize() {
    // PENDING: Make this user-definable.
    // Remember though - you can't change this value on the fly so we'll need
    //   some central management on the network for configuration values.
    return 6134;
  }

  /**
   * {@inhericDoc}
   */
  public final void checkCriticalStop() {
    if (critical_stop_error != null) {
      // We wrap the critical stop error a second time to ensure the stack
      // trace accurately reflects where the failure originated.
      throw new CriticalStopError(
              critical_stop_error.getMessage(), critical_stop_error);
    }
  }
  
  /**
   * {@inhericDoc}
   */
  public final CriticalStopError handleIOException(IOException e) {
    log.log(Level.SEVERE, "Critical stop IO Error", e);
    critical_stop_error = new CriticalStopError(e.getMessage(), e);
    throw critical_stop_error;
  }
  
  /**
   * {@inhericDoc}
   */
  public final CriticalStopError handleVMError(VirtualMachineError e) {
// We don't attempt to log vm errors
//    log.log(Level.SEVERE, "Critical stop VM Error", e);
    critical_stop_error = new CriticalStopError(e.getMessage(), e);
    throw critical_stop_error;
  }

  /**
   * {@inhericDoc}
   */
  public long getNodeHeapMaxSize() {
    return max_transaction_node_heap_size;
  }


  private final HashMap<ServiceAddress, Integer> closeness_map = new HashMap();

  /**
   * Returns a value that indicates the 'closeness' of this machine to the
   * given ServiceAddress. A high value means the machine is far away
   * therefore accessing information from it should not happen if there is
   * a closer machine. Returning '0' indicates this object is running on the
   * same machine as the given address.
   */
  private int findClosenessToHere(ServiceAddress node) {
    synchronized (closeness_map) {
      Integer closeness = closeness_map.get(node);
      if (closeness == null) {

        try {
          InetAddress machine_address = node.asInetAddress();

          Enumeration<NetworkInterface> local_interfaces =
                                       NetworkInterface.getNetworkInterfaces();
          boolean is_local = false;
interface_loop:
          for (NetworkInterface netint : Collections.list(local_interfaces)) {
            Enumeration<InetAddress> addresses = netint.getInetAddresses();
            for (InetAddress addr : Collections.list(addresses)) {
              // If this machine address is on this machine, return true,
              if (machine_address.equals(addr)) {
                is_local = true;
                break interface_loop;
              }
            }
          }
          // If the interface is local,
          if (is_local) {
            closeness = 0;
          }
          else {
            // Not local,
            closeness = 10000;
          }

        }
        catch (SocketException e) {
          // Unknown closeness,
          // Log a severe error,
          log.log(Level.SEVERE, "Unable to determine if node local", e);
          closeness = Integer.MAX_VALUE;
        }

        // Put it in the map,
        closeness_map.put(node, closeness);
      }
      return closeness;
    }
  }

  /**
   * Reports a block server failure to the manager server.
   */
  private void reportBlockServerFailure(ServiceAddress address) {

    // Report the failure,
    log.log(Level.WARNING,
            "Reporting failure for {0} to manager server",
            address.displayString());

    // Failure throttling,
    synchronized (failure_flood_control) {
      long current_time = System.currentTimeMillis();
      Long last_address_fail_time = failure_flood_control.get(address);
      if (last_address_fail_time != null &&
          last_address_fail_time + (30 * 1000) > current_time) {
        // We don't respond to failure notifications on the same address if a
        // failure notice arrived within a minute of the last one accepted.
        return;
      }
      failure_flood_control.put(address, current_time);
    }

    MessageStream message_out = new MessageStream(16);
    message_out.addMessage("notifyBlockServerFailure");
    message_out.addServiceAddress(address);
    message_out.closeMessage();

    // Process the failure report message on the manager server,
    notifyAllManagers(message_out);

  }

  /**
   * Reports a possible block id corruption message to the first available
   * manager server.
   */
  private void reportBlockIdCorruption(
            ServiceAddress block_server, BlockId block_id, String fail_type) {

    // Report the failure,
    log.log(Level.WARNING,
            "Reporting a data failure (type = {0}) for block {1} at block server {2}",
            new Object[] { fail_type, block_id, block_server.displayString()});

    // Failure throttling,
    synchronized (failure_flood_control_bidc) {
      long current_time = System.currentTimeMillis();
      Long last_address_fail_time =
                                 failure_flood_control_bidc.get(block_server);
      if (last_address_fail_time != null &&
          last_address_fail_time + (10 * 1000) > current_time) {
        // We don't respond to failure notifications on the same address if a
        // failure notice arrived within a minute of the last one accepted.
        return;
      }
      failure_flood_control_bidc.put(block_server, current_time);
    }

    MessageStream message_out = new MessageStream(16);
    message_out.addMessage("notifyBlockIdCorruption");
    message_out.addServiceAddress(block_server);
    message_out.addBlockId(block_id);
    message_out.addString(fail_type);
    message_out.closeMessage();

    // Process the failure report message on the manager server,
    // (Ignore any error message generated)
    processManager(message_out);

  }

  /**
   * Returns the list of servers that contain the given block.
   */
  private Map<BlockId, List<BlockServerElement>> getServerListForBlocks(
                                                     List<BlockId> block_ids) {

    // The result map,
    HashMap<BlockId, List<BlockServerElement>> result_map = new HashMap();

    ArrayList<BlockId> none_cached = new ArrayList(block_ids.size());
    for (BlockId block_id : block_ids) {
      List<BlockServerElement> v =
              local_network_cache.getServersWithBlock(block_id);
      // If it's cached (and the cache is current),
      if (v != null) {
        result_map.put(block_id, v);
      }
      // If not cached, add to the list of none cached entries,
      else {
        none_cached.add(block_id);
      }
    }

    // If there are no 'none_cached' blocks,
    if (none_cached.size() == 0) {
      // Return the result,
      return result_map;
    }

    // Otherwise, we query the manager server for current records on the given
    // blocks.

    MessageStream message_out = new MessageStream(15);

    for (BlockId block_id : none_cached) {
      message_out.addMessage("getServerList");
      message_out.addBlockId(block_id);
      message_out.closeMessage();
    }

    // Process a command on the manager,
    ProcessResult message_in = processManager(message_out);

    int n = 0;
    for (Message m : message_in) {
      if (m.isError()) {
        
        log.log(Level.SEVERE, "'getServerListsForBlocks' command failed: {0}",
                              m.getErrorMessage());
        log.log(Level.SEVERE, m.getExternalThrowable().getStackTrace());

        throw new RuntimeException(m.getErrorMessage());
      }
      else {
        int sz = (Integer) m.param(0);
        ArrayList<BlockServerElement> srvs = new ArrayList(sz);
        for (int i = 0; i < sz; ++i) {
          ServiceAddress address = (ServiceAddress) m.param(1 + (i * 2));
          String status = (String) m.param(1 + (i * 2) + 1);
          srvs.add(new BlockServerElement(address, status));
        }

        // Shuffle the list
        Collections.shuffle(srvs);

        // Move the server closest to this node to the start of the list,
        int closest = 0;
        int cur_close_factor = Integer.MAX_VALUE;
        for (int i = 0; i < sz; ++i) {
          BlockServerElement elem = srvs.get(i);
          int closeness_factor = findClosenessToHere(elem.getAddress());
          if (closeness_factor < cur_close_factor) {
            cur_close_factor = closeness_factor;
            closest = i;
          }
        }

        // Swap if necessary,
        if (closest > 0) {
          Collections.swap(srvs, 0, closest);
        }

        // Put it in the result map,
        BlockId block_id = none_cached.get(n);
        result_map.put(block_id, srvs);
        // Add it to the cache,
        // NOTE: TTL hard-coded to 15 minute
        local_network_cache.putServersForBlock(block_id, srvs,
                                               15 * 60 * 1000);

      }
      ++n;
    }

    // Return the list
    return result_map;
  }

  /**
   * {@inhericDoc}
   */
  public TreeNode[] fetchNode(NodeReference[] node_refs) {
    // The number of nodes,
    int node_count = node_refs.length;
    // The array of read nodes,
    TreeNode[] result_nodes = new TreeNode[node_count];

//    if (node_refs.length > 1) {
//      System.out.print("fetching: ");
//      for (long nr : node_refs) {
//        System.out.print(nr);
//        System.out.print(", ");
//      }
//      System.out.println();
//    }

    // Resolve special nodes first,
    {
      int i = 0;
      for (NodeReference node_ref : node_refs) {
        if (node_ref.isSpecial()) {
          result_nodes[i] = StoreBackedTreeSystem.specialStaticNode(node_ref);
        }
        ++i;
      }
    }

    // Group all the nodes to the same block,
    ArrayList<BlockId> unique_blocks = new ArrayList();
    ArrayList<ArrayList<NodeReference>> unique_block_list = new ArrayList();
    {
      int i = 0;
      for (NodeReference node_ref : node_refs) {
        // If it's not a special node,
        if (!node_ref.isSpecial()) {
          // Get the block id and add it to the list of unique blocks,
          DataAddress address = new DataAddress(node_ref);
          // Check if the node is in the local cache,
          TreeNode node = local_network_cache.getNode(address);
          if (node != null) {
            result_nodes[i] = node;
          }
          else {
            // Not in the local cache so we need to bundle this up in a node
            // request on the block servers,
            // Group this node request by the block identifier
            BlockId block_id = address.getBlockId();
            int ind = unique_blocks.indexOf(block_id);
            if (ind == -1) {
              ind = unique_blocks.size();
              unique_blocks.add(block_id);
              unique_block_list.add(new ArrayList());
            }
            ArrayList<NodeReference> blist = unique_block_list.get(ind);
            blist.add(node_ref);
          }
        }
        ++i;
      }
    }

    // Exit early if no blocks,
    if (unique_blocks.size() == 0) {
      return result_nodes;
    }

    // Resolve server records for the given block identifiers,
    Map<BlockId, List<BlockServerElement>> servers_map =
                                     getServerListForBlocks(unique_blocks);

    // The result nodes list,
    ArrayList<TreeNode> nodes = new ArrayList();

    // Checksumming objects
    byte[] checksum_buf = null;
    CRC32 crc32 = null;

    // For each unique block list,
    for (ArrayList<NodeReference> blist : unique_block_list) {
      // Make a block server request for each node in the block,
      MessageStream block_server_msg =
                                  new MessageStream((4 * blist.size()) + 4);
      BlockId block_id = null;
      for (NodeReference node_ref : blist) {
        DataAddress address = new DataAddress(node_ref);
        block_server_msg.addMessage("readFromBlock");
        block_server_msg.addDataAddress(address);
        block_id = address.getBlockId();
        block_server_msg.closeMessage();
      }

      if (block_id == null) {
        throw new RuntimeException("block_id == null");
      }

      // Get the shuffled list of servers the block is stored on,
      List<BlockServerElement> servers = servers_map.get(block_id);

      // Go through the servers one at a time to fetch the block,
      boolean success = false;
      for (int z = 0; z < servers.size() && !success; ++z) {
        BlockServerElement server = servers.get(z);
//        System.out.println("CHECKING: " + server.getAddress());
//        System.out.println("  Status: " + server.isStatusUp());
//        System.out.println("  servers.size() = " + servers.size());
//        System.out.println("  z = " + z);
        // If the server is up,
        if (server.isStatusUp()) {

          // Open a connection with the block server,
          MessageProcessor block_server_proc =
                       connector.connectBlockServer(server.getAddress());
          ProcessResult message_in =
                       block_server_proc.process(block_server_msg);
          ++network_comm_count;
          ++network_fetch_comm_count;
//          System.out.println(network_fetch_comm_count);
          boolean is_error = false;
          boolean severe_error = false;
          boolean crc_error = false;
          boolean connection_error = false;

          // Turn each none-error message into a node
          for (Message m : message_in) {
            if (m.isError()) {
              // See if this error is a block read error. If it is, we don't
              // tell the manager server to lock this server out completely.
              ExternalThrowable error_et = m.getExternalThrowable();
              boolean is_block_read_error =
                           error_et.getClassName().equals(
                                       "com.mckoi.network.BlockReadException");

              // Log the error as a warning
              log.log(Level.WARNING, "Error in message from block server: {0}\n{1}",
                      new Object[] { m.getErrorMessage(),
                                     error_et.getStackTrace()
                                   });

              // If it's a connection fault,
              if (isConnectionFailMessage(m)) {
                connection_error = true;
              }
              else if (!is_block_read_error) {
                // If it's something other than a block read error or
                // connection failure, we set the severe flag,
                severe_error = true;
              }
              is_error = true;
            }
            else if (is_error == false) {
              // The reply contains the block of data read.
              NodeSet node_set = (NodeSet) m.param(0);

              DataAddress address = null;

              // Catch any IOExceptions (corrupt zips, etc)
              try {
                // Decode the node items into Java node objects,
                Iterator<NodeItemBinary> item_iterator =
                                                   node_set.getNodeSetItems();

                while (item_iterator.hasNext()) {
                  // Get the node item,
                  NodeItemBinary node_item = item_iterator.next();

                  NodeReference node_ref = node_item.getNodeId();

                  address = new DataAddress(node_ref);
                  // Wrap around a buffered DataInputStream for reading values
                  // from the store.
                  DataInputStream in =
                              new DataInputStream(node_item.getInputStream());
                  short node_type = in.readShort();

                  TreeNode read_node = null;

                  if (crc32 == null) crc32 = new CRC32();
                  crc32.reset();

                  // Is the node type a leaf node?
                  if (node_type == STORE_LEAF_TYPE) {
                    // Read the checksum,
                    in.readShort();  // For future use...
                    int checksum = in.readInt();
                    // Read the size
                    int leaf_size = in.readInt();

                    byte[] buf = node_item.asBinary();
                    if (buf == null) {
                      buf = new byte[leaf_size + 12];
                      ByteArrayUtil.setInt(leaf_size, buf, 8);
                      in.readFully(buf, 12, leaf_size);
                    }

                    // Check the checksum...
                    crc32.update(buf, 8, leaf_size + 4);
                    int calc_checksum = (int) crc32.getValue();
                    if (checksum != calc_checksum) {
                      // If there's a CRC failure, we reject his node,
                      log.log(Level.WARNING,
                              "CRC failure on node {0} @ {1}",
                              new Object[] {
                                    node_ref.toString(),
                                    server.getAddress().displayString()
                              });
                      is_error = true;
                      crc_error = true;
                      // This causes the read to retry on a different server
                      // with this block id
                    }
                    else {
                      // Create a leaf that's mapped to this data
                      TreeNode leaf = new ByteArrayTreeLeaf(node_ref, buf);
                      read_node = leaf;
                    }

                  }
                  // Is the node type a branch node?
                  else if (node_type == STORE_BRANCH_TYPE) {
                    // Read the checksum,
                    in.readShort();  // For future use...
                    int checksum = in.readInt();

                    // Check the checksum objects,
                    if (checksum_buf == null) checksum_buf = new byte[8];

                    // Note that the entire branch is loaded into memory,
                    int child_data_size = in.readInt();
                    ByteArrayUtil.setInt(child_data_size, checksum_buf, 0);
                    crc32.update(checksum_buf, 0, 4);
                    long[] data_arr = new long[child_data_size];
                    for (int n = 0; n < child_data_size; ++n) {
                      long item = in.readLong();
                      ByteArrayUtil.setLong(item, checksum_buf, 0);
                      crc32.update(checksum_buf, 0, 8);
                      data_arr[n] = item;
                    }

                    // The calculated checksum value,
                    int calc_checksum = (int) crc32.getValue();
                    if (checksum != calc_checksum) {
                      // If there's a CRC failure, we reject his node,
                      log.log(Level.WARNING,
                              "CRC failure on node {0} @ {1}",
                              new Object[] {
                                    node_ref.toString(),
                                    server.getAddress().displayString()
                              });
                      is_error = true;
                      crc_error = true;
                      // This causes the read to retry on a different server
                      // with this block id
                    }
                    else {
                      // Create the branch node,
                      TreeBranch branch =
                              new TreeBranch(node_ref, data_arr, child_data_size);
                      read_node = branch;
                    }

                  }
                  else {
                    log.log(Level.SEVERE, "Unknown node {0} type: {1}",
                            new Object[] { address.toString(), node_type });
                    is_error = true;
  //                  throw new InvalidDataState(
  //                          "Unknown node " + address.toString() +
  //                          " type: " + node_type, address);
                  }

                  // Is the node already in the list? If so we don't add it.
                  if (read_node != null && !isInNodeList(node_ref, nodes)) {
                    // Put the read node in the cache and add it to the 'nodes'
                    // list.
                    local_network_cache.putNode(address, read_node);
                    nodes.add(read_node);
                  }

                }  // while (item_iterator.hasNext())

              }
              catch (IOException e) {
                // This catches compression errors, as well as any other misc
                // IO errors.
                if (address != null) {
                  log.log(Level.SEVERE,
                          "IO Error reading node {0}", address.toString() );
                }
                log.log(Level.SEVERE, e.getMessage(), e);
                is_error = true;
              }

            }

          }  // for (Message m : message_in)

          // If there was no error while reading the result, we assume the node
          // requests were successfully read.
          if (is_error == false) {
            success = true;
          }
          else {
            // If this is a connection failure, we report the block failure.
            if (connection_error) {
              // If this is an error, we need to report the failure to the
              // manager server,
              reportBlockServerFailure(server.getAddress());
              // Remove the block id from the server list cache,
              local_network_cache.removeServersWithBlock(block_id);
            }
            else {
              String fail_type = "General";
              if (crc_error) {
                fail_type = "CRC Failure";
              }
              else if (severe_error) {
                fail_type = "Exception during process";
              }

              // Report to the first manager the block failure, so it may
              // investigate and hopefully correct.
              reportBlockIdCorruption(server.getAddress(), block_id, fail_type);

              // Otherwise, not a severe error (probably a corrupt block on a
              // server), so shuffle the server list for this block_id so next
              // time there's less chance of hitting this bad block.
              List<BlockServerElement> srvs =
                            local_network_cache.getServersWithBlock(block_id);
              if (srvs != null) {
                ArrayList<BlockServerElement> server_list = new ArrayList();
                server_list.addAll(srvs);
                Collections.shuffle(server_list);
                local_network_cache.putServersForBlock(block_id,
                                                  server_list, 15 * 60 * 1000);
              }
            }
            // We will now go retry the query on the next block server,
          }

        }
      }

      // If the nodes were not successfully read, we generate an exception,
      if (!success) {
        // Remove from the cache,
        local_network_cache.removeServersWithBlock(block_id);
        throw new RuntimeException(
                "Unable to fetch node from a block server" +
                " (block = " + block_id + ")");
      }

//      // The 'nodes' list is the fetched TreeNode nodes, ordered by the 'blist'.
//      if (blist.size() != nodes.size()) {
//        throw new RuntimeException("Assertion failed");
//      }

    }

    int sz = nodes.size();
    if (sz == 0) {
      throw new RuntimeException("Empty nodes list");
    }

//    if (node_refs.length > 1) {
//      System.out.println("node_refs.length = " + node_refs.length);
//      System.out.println("nodes.size() = " + nodes.size());
//      System.out.println("number_queries = " + unique_block_list.size());
//    }
//    else {
//      System.out.println("Single Query!");
//    }

    for (int i = 0; i < sz; ++i) {
      TreeNode node = nodes.get(i);
      NodeReference node_ref = node.getReference();
      for (int n = 0; n < node_refs.length; ++n) {
        if (node_refs[n].equals(node_ref)) {
          result_nodes[n] = node;
        }
      }
    }

    // Check the result_nodes list is completely populated,
    for (int n = 0; n < result_nodes.length; ++n) {
      if (result_nodes[n] == null) {
//        System.out.println("Requested: ");
//        for (int p = 0; p < node_refs.length; ++p) {
//          System.out.print(node_refs[p]);
//          System.out.print(", ");
//        }
//        System.out.println();
//
//        System.out.println("Nodes: ");
//        for (int p = 0; p < nodes.size(); ++p) {
//          System.out.print(nodes.get(p).getReference());
//          System.out.print(", ");
//        }
//        System.out.println();
//
//        System.out.println("Result: ");
//        for (int p = 0; p < result_nodes.length; ++p) {
//          System.out.print(result_nodes[p]);
//          System.out.print(", ");
//        }
//        System.out.println();
        throw new RuntimeException(
                "Assertion failed: result_nodes not completely populated.");
      }
    }

    return result_nodes;

  }

  /**
   * {@inhericDoc}
   */
  public boolean isNodeAvailableLocally(NodeReference node_ref) {
    // Special node ref,
    if (node_ref.isSpecial()) {
      return true;
    }
    // Check if it's in the local network cache
    DataAddress address = new DataAddress(node_ref);
    return (local_network_cache.getNode(address) != null);
  }

  /**
   * Returns true if the given reference is in the given node list.
   */
  private boolean isInNodeList(NodeReference ref, ArrayList<TreeNode> nodes) {
    for (TreeNode node : nodes) {
      if (ref.equals(node.getReference())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Internal method for writing information out to the network.
   */
  private NodeReference[] internalPerformTreeWrite(
                TreeWriteSequence sequence, int try_count) throws IOException {

    // NOTE: nodes are written in order of branches and then leaf nodes. All
    //   branch nodes and leafs are grouped together.

    // The list of nodes to be allocated,
    List<TreeNode> all_branches = sequence.getAllBranchNodes();
    List<TreeNode> all_leafs = sequence.getAllLeafNodes();
    ArrayList<TreeNode> nodes =
                       new ArrayList(all_branches.size() + all_leafs.size());
    nodes.addAll(all_branches);
    nodes.addAll(all_leafs);
    int sz = nodes.size();
    // The list of allocated referenced for the nodes,
    DataAddress[] refs = new DataAddress[sz];
    NodeReference[] out_refs = new NodeReference[sz];

    MessageStream allocate_message = new MessageStream((sz * 3) + 16);

    // Allocate the space first,
    for (int i = 0; i < sz; ++i) {
      TreeNode node = nodes.get(i);
      // Is it a branch node?
      if (node instanceof TreeBranch) {
        // Branch nodes are 1K in size,
        allocate_message.addMessage("allocateNode");
        allocate_message.addInteger(1024);
        allocate_message.closeMessage();
      }
      // Otherwise, it must be a leaf node,
      else {
        // Leaf nodes are 4k in size,
        allocate_message.addMessage("allocateNode");
        allocate_message.addInteger(4096);
        allocate_message.closeMessage();
      }
    }

    // Process a command on the manager,
    ProcessResult result_stream = processManager(allocate_message);

    // The unique list of blocks,
    ArrayList<BlockId> unique_blocks = new ArrayList();

    // Parse the result stream one message at a time, the order will be the
    // order of the allocation messages,
    int n = 0;
    for (Message m : result_stream) {
      if (m.isError()) {

        log.log(Level.SEVERE, "'internalPerformTreeWrite' command failed: {0}",
                              m.getErrorMessage());
        log.log(Level.SEVERE, m.getExternalThrowable().getStackTrace());

        throw new RuntimeException(m.getErrorMessage());
      }
      else {
        DataAddress addr = (DataAddress) m.param(0);
        refs[n] = addr;
        // Make a list of unique block identifiers,
        if (!unique_blocks.contains(addr.getBlockId())) {
          unique_blocks.add(addr.getBlockId());
        }
      }
      ++n;
    }

    // Get the block to server map for each of the blocks,

    Map<BlockId, List<BlockServerElement>> block_to_server_map =
                                       getServerListForBlocks(unique_blocks);

    // Make message streams for each unique block
    int ubid_count = unique_blocks.size();
    MessageStream[] ubid_stream = new MessageStream[ubid_count];
    for (int i = 0; i < ubid_stream.length; ++i) {
      ubid_stream[i] = new MessageStream(512);
    }

    // Scan all the blocks and create the message streams,
    for (int i = 0; i < sz; ++i) {

      byte[] node_buf;

      TreeNode node = nodes.get(i);
      // Is it a branch node?
      if (node instanceof TreeBranch) {
        TreeBranch branch = (TreeBranch) node;
        // Make a copy of the branch (NOTE; we clone() the array here).
        long[] cur_node_data = branch.getNodeData().clone();
        int cur_ndsz = branch.getNodeDataSize();
        branch = new TreeBranch(refs[i].getValue(), cur_node_data, cur_ndsz);

        // The number of children
        int chsz = branch.size();
        // For each child, if it's a heap node, look up the child id and
        // reference map in the sequence and set the reference accordingly,
        for (int o = 0; o < chsz; ++o) {
          NodeReference child_ref = branch.getChild(o);
          if (child_ref.isInMemory()) {
            // The ref is currently on the heap, so adjust accordingly
            int ref_id = sequence.lookupRef(i, o);
            branch.setChildOverride(refs[ref_id].getValue(), o);
          }
        }

        // Turn the branch into a 'node_buf' byte[] array object for
        // serialization.
        long[] node_data = branch.getNodeData();
        int ndsz = branch.getNodeDataSize();
        ByteArrayOutputStream bout = new ByteArrayOutputStream(1024);
        DataOutputStream dout = new DataOutputStream(bout);
        dout.writeShort(STORE_BRANCH_TYPE);
        dout.writeShort(0); // Reserved for future
        dout.writeInt(0);   // The crc32 checksum will be written here,
        dout.writeInt(ndsz);
        for (int o = 0; o < ndsz; ++o) {
          dout.writeLong(node_data[o]);
        }
        dout.flush();

        // Turn it into a byte array,
        node_buf = bout.toByteArray();

        // Write the crc32 of the data,
        CRC32 checksum = new CRC32();
        checksum.update(node_buf, 8, node_buf.length - 8);
        ByteArrayUtil.setInt((int) checksum.getValue(), node_buf, 4);

        // Put this branch into the local cache,
        local_network_cache.putNode(refs[i], branch);

      }
      // If it's a leaf node,
      else {

        TreeLeaf leaf = (TreeLeaf) node;
        int lfsz = leaf.getSize();

        node_buf = new byte[lfsz + 12];

        // Format the data,
        ByteArrayUtil.setShort(STORE_LEAF_TYPE, node_buf, 0);
        ByteArrayUtil.setShort((short) 0, node_buf, 2);  // Reserved for future
        ByteArrayUtil.setInt(lfsz, node_buf, 8);
        leaf.get(0, node_buf, 12, lfsz);

        // Calculate and set the checksum,
        CRC32 checksum = new CRC32();
        checksum.update(node_buf, 8, node_buf.length - 8);
        ByteArrayUtil.setInt((int) checksum.getValue(), node_buf, 4);

        // Put this leaf into the local cache,
        leaf = new ByteArrayTreeLeaf(refs[i].getValue(), node_buf);
        local_network_cache.putNode(refs[i], leaf);

      }

      // The DataAddress this node is being written to,
      DataAddress address = refs[i];
      // Get the block id,
      BlockId block_id = address.getBlockId();
      int bid = unique_blocks.indexOf(block_id);
      ubid_stream[bid].addMessage("writeToBlock");
      ubid_stream[bid].addDataAddress(address);
      ubid_stream[bid].addBuf(node_buf);
      ubid_stream[bid].addInteger(0);
      ubid_stream[bid].addInteger(node_buf.length);
      ubid_stream[bid].closeMessage();

      // Update 'out_refs' array,
      out_refs[i] = refs[i].getValue();

    }

    // A log of successfully processed operations,
    ArrayList success_process = new ArrayList(64);

    // Now process the streams on the servers,
    for (int i = 0; i < ubid_stream.length; ++i) {
      // The output message,
      MessageStream message_out = ubid_stream[i];
      // Get the servers this message needs to be sent to,
      BlockId block_id = unique_blocks.get(i);
      List<BlockServerElement> block_servers = block_to_server_map.get(block_id);
      // Format a message for writing this node out,
      int bssz = block_servers.size();
      MessageProcessor[] block_server_procs = new MessageProcessor[bssz];
      // Make the block server connections,
      for (int o = 0; o < bssz; ++o) {
        ServiceAddress address = block_servers.get(o).getAddress();
        block_server_procs[o] = connector.connectBlockServer(address);
        ProcessResult message_in = block_server_procs[o].process(message_out);
        ++network_comm_count;

        for (Message m : message_in) {
          if (m.isError()) {
            // If this is an error, we need to report the failure to the
            // manager server,
            reportBlockServerFailure(address);
            // Remove the block id from the server list cache,
            local_network_cache.removeServersWithBlock(block_id);

            // Rollback any server writes already successfully made,
            for (int p = 0; p < success_process.size(); p += 2) {
              ServiceAddress blocks_addr = (ServiceAddress) success_process.get(p);
              MessageStream to_rollback = (MessageStream) success_process.get(p + 1);

              ArrayList<DataAddress> rollback_nodes = new ArrayList(128);
              for (Message rm : to_rollback) {
                DataAddress raddr = (DataAddress) rm.param(0);
                rollback_nodes.add(raddr);
              }
              // Create the rollback message,
              MessageStream rollback_msg = new MessageStream(16);
              rollback_msg.addMessage("rollbackNodes");
              rollback_msg.addDataAddressArr(rollback_nodes.toArray(
                                   new DataAddress[rollback_nodes.size()]));
              rollback_msg.closeMessage();

              // Send it to the block server,
              ProcessResult msg_in =
                      connector.connectBlockServer(blocks_addr).process(
                                                               rollback_msg);
              ++network_comm_count;
              for (Message rbm : msg_in) {
                // If rollback generated an error we throw the error now
                // because this likely is a serious network error.
                if (rbm.isError()) {

                  log.log(Level.SEVERE, "'internalPerformTreeWrite' command failed: {0}",
                                        m.getErrorMessage());
                  log.log(Level.SEVERE, m.getExternalThrowable().getStackTrace());

                  throw new NetworkWriteException(
                        "Write failed (rollback failed): " + rbm.getErrorMessage());
                }
              }

            }

            // Retry,
            if (try_count > 0) {
              return internalPerformTreeWrite(sequence, try_count - 1);
            }
            // Otherwise we fail the write
            else {

              log.log(Level.WARNING, "'internalPerformTreeWrite' command failed: {0}",
                                     m.getErrorMessage());
              log.log(Level.WARNING, m.getExternalThrowable().getStackTrace());

              throw new NetworkWriteException(m.getErrorMessage());
            }
          }
        }

        // If we succeeded without an error, add to the log
        success_process.add(address);
        success_process.add(message_out);

      }
    }

    // Return the references,
    return out_refs;

  }

  /**
   * Performs the sequence of node write operations described by the given
   * TreeWriteSequence object. This is used to flush a complete tree write
   * operation out to the backing store. Returns an array of node_ref 64-bit
   * values that represent the address of every node written to the backing
   * media on the completion of the process.
   */
  public NodeReference[] performTreeWrite(TreeWriteSequence sequence) throws IOException {
    return internalPerformTreeWrite(sequence, 3);
  }

  /**
   * {@inhericDoc}
   */
  public boolean featureAccountForAllNodes() {
    // We don't need to account for all references and disposes to nodes in
    // this implementation.
    return false;
  }

  /**
   * {@inhericDoc}
   */
  public boolean linkLeaf(Key key, NodeReference ref) throws IOException {
    // NO-OP: A network tree system does not perform reference counting.
    //   Instead performs reachability testing and garbage collection through
    //   an external process.
    return true;
  }

  /**
   * {@inhericDoc}
   */
  public void disposeNode(NodeReference ref) throws IOException {
    // NO-OP: Nodes can not be easily disposed, therefore this can do nothing
    //   except provide a hint to the garbage collector to reclaim resources
    //   on this node in the next cycle.
  }


  
  
  private final Object reachability_lock = new Object();
  private int reachability_tree_depth;

  private void doReachCheck(PrintWriter warning_log,
                            NodeReference node,
                            OrderedList64Bit node_list,
                            int cur_depth) throws IOException {

    throw new RuntimeException("PENDING");

//    // Is the node in the list?
//    boolean inserted = node_list.insertUnique(new Long(node), node,
//                                              OrderedList64Bit.KEY_COMPARATOR);
//
//    if (inserted) {
//      // Fetch the node,
//      try {
//        TreeNode tree_node = fetchNode(new NodeReference[] { node })[0];
//        if (tree_node instanceof TreeBranch) {
//          // Get the child nodes,
//          TreeBranch branch = (TreeBranch) tree_node;
//          int children_count = branch.size();
//          for (int i = 0; i < children_count; ++i) {
//            NodeReference child_node_ref = branch.getChild(i);
//            // Recurse,
//            if (cur_depth + 1 == reachability_tree_depth) {
//              // It's a known leaf node, so insert now without traversing
//              node_list.insertUnique(new Long(child_node_ref),
//                                     child_node_ref,
//                                     OrderedList64Bit.KEY_COMPARATOR);
//            }
//            else {
//              // Recurse,
//              doReachCheck(warning_log, child_node_ref,
//                           node_list, cur_depth + 1);
//            }
//          }
//        }
//        else if (tree_node instanceof TreeLeaf) {
//          reachability_tree_depth = cur_depth;
//        }
//        else {
//          throw new IOException("Unknown node class: " + tree_node);
//        }
//      }
//      catch (InvalidDataState e) {
//        // Report the error,
//        warning_log.println("Invalid Data Set (msg: " + e.getMessage() + ")");
//        warning_log.println("Block: " + e.getAddress().getBlockId());
//        warning_log.println("Data:  " + e.getAddress().getDataId());
//      }
//    }
  }

  /**
   * Traverses a tree from the given root node adding any nodes it discovers
   * to the sorted set (including the root node). This is used to perform a
   * reachability test to determine the nodes that need to be preserved vs
   * the nodes that are not linked with any root node and therefore can be
   * removed.
   * <p>
   * If this method traverses to a node that's already stored in the list it
   * does not bother to perform the reachability determination on the children.
   */
  public void createReachabilityList(PrintWriter warning_log,
                               NodeReference node,
                               OrderedList64Bit node_list) throws IOException {
    checkCriticalStop();

    synchronized (reachability_lock) {
      reachability_tree_depth = -1;
      doReachCheck(warning_log, node, node_list, 1);
    }

  }

  /**
   * Debugging/analysis method that walks through the entire tree and generates
   * a graph of every area reference in the store that is touched by this tree
   * store.  Each tree node contains properties about the area.  Each node
   * always contains a property 'ref' which is the Long reference to the
   * area.
   * <p>
   * This method describes the tree of the given transaction, whether the
   * nodes are contained in memory or in the backing store. This method can
   * fail if the underlying network is not integral, so is of limited
   * usefulness in diagnostic for repairing a database.
   * <p>
   * Another function should be used for a more general analysis of all the
   * information stored in the network in general. The scope of information
   * available by this class is limited to the transaction tree.
   */
  public TreeReportNode createDiagnosticGraph(KeyObjectTransaction t)
                                                           throws IOException {
    checkCriticalStop();

    // The key object transaction
    NetworkTreeSystemTransaction ts = (NetworkTreeSystemTransaction) t;
    // Get the root node ref
    NodeReference root_node_ref = ts.getRootNodeRef();
    // Add the child node (the root node of the version graph).
    return createDiagnosticRootGraph(Key.HEAD_KEY, root_node_ref);
  }

  /**
   * Walks the tree from the given node returning a graph the contains
   * basic property information about the nodes.
   */
  private TreeReportNode createDiagnosticRootGraph(Key left_key,
                                        NodeReference ref) throws IOException {

    // The node being returned
    TreeReportNode node;

    // Fetch the node,
    TreeNode tree_node = fetchNode(new NodeReference[] { ref })[0];

    if (tree_node instanceof TreeLeaf) {
      TreeLeaf leaf = (TreeLeaf) tree_node;
      // The number of bytes in the leaf
      int leaf_size = leaf.getSize();

      // Set up the leaf node object
      node = new TreeReportNode("leaf", ref);
      node.setProperty("key", left_key.toString());
      node.setProperty("leaf_size", leaf_size);

    }
    else if (tree_node instanceof TreeBranch) {
      TreeBranch branch = (TreeBranch) tree_node;
      // Set up the branch node object
      node = new TreeReportNode("branch", ref);
      node.setProperty("key", left_key.toString());
      node.setProperty("branch_size", branch.size());
      // Recursively add each child into the tree
      for (int i = 0; i < branch.size(); ++i) {
        NodeReference child_ref = branch.getChild(i);
        // If the ref is a special node, skip it
        if (child_ref.isSpecial()) {
          // Should we record special nodes?
        }
        else {
          Key new_left_key = (i > 0) ? branch.getKeyValue(i) : left_key;
          TreeReportNode bn = new TreeReportNode("child_meta", ref);
          bn.setProperty("extent", branch.getChildLeafElementCount(i));
          node.addChild(bn);
          node.addChild(createDiagnosticRootGraph(new_left_key, child_ref));
        }
      }

    }
    else {
      throw new IOException("Unknown node class: " + tree_node);
    }

    return node;
  }



  

  // ---------- Inner classes ----------
  
  /**
   * A tree leaf whose data is backed by a byte array, which allows us to
   * store it in a heap, etc.
   */
  private static class ByteArrayTreeLeaf extends TreeLeaf {

    /**
     * The byte[] buffer containing the leaf data.
     */
    private final byte[] data;

    /**
     * The node reference.
     */
    private final NodeReference node_ref;

    /**
     * Constructor.
     */
    public ByteArrayTreeLeaf(NodeReference node_ref, byte[] buf) {
      super();
      this.node_ref = node_ref;
      this.data = buf;
    }

    // ---------- Implemented from TreeLeaf ----------

    public NodeReference getReference() {
      return node_ref;
    }

    public int getSize() {
      return data.length - 12;
    }

    public int getCapacity() {
      throw new RuntimeException(
                        "Immutable leaf does not have a meaningful capacity");
    }

    public byte get(int position) throws IOException {
      return data[12 + position];
    }

    public void get(int position, byte[] buf, int off, int len) throws IOException {
      System.arraycopy(data, 12 + position, buf, off, len);
    }

    public void writeDataTo(AreaWriter writer) throws IOException {
      writer.put(data, 12, getSize());
    }

    public void shift(int position, int offset) throws IOException {
      throw new IOException("Write methods not available for immutable leaf");
    }

    public void put(int position, byte[] buf, int off, int len) throws IOException {
      throw new IOException("Write methods not available for immutable leaf");
    }

    public void setSize(int size) throws IOException {
      throw new IOException("Write methods not available for immutable leaf");
    }

    public int getHeapSizeEstimate() {
      return 8 + data.length + 64;
    }

  }

  // ---------- NetworkTreeSystemTransaction ----------

  /**
   * The NetworkTreeSystemTransaction, which is a networked transaction
   * object.
   */
  private static class NetworkTreeSystemTransaction
                                               extends TreeSystemTransaction {

    NetworkTreeSystemTransaction(TreeSystem tree_system,
                                 long version_id, DataAddress root_node) {
      super(tree_system, version_id, root_node.getValue(), false);
    }

    NetworkTreeSystemTransaction(TreeSystem tree_system,
                                 long version_id) {
      super(tree_system, version_id, null, false);
      setToEmpty();
    }

    @Override
    public void checkOut() throws IOException {
      super.checkOut();
    }

    @Override
    public NodeReference getRootNodeRef() {
      return super.getRootNodeRef();
    }

    void doFullDispose() throws IOException {
      super.dispose();
    }

  }

  // ---------- Container classes ----------

//  /**
//   * A runtime exception that signifies that the data in a node has an invalid
//   * state.
//   */
//  private static class InvalidDataState extends RuntimeException {
//
//    private DataAddress address;
//
//    InvalidDataState(String msg, DataAddress address) {
//      super(msg);
//      this.address = address;
//    }
//
//    public DataAddress getAddress() {
//      return address;
//    }
//
//  }

}
