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

import com.mckoi.data.KeyObjectTransaction;
import com.mckoi.data.TreeReportNode;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * The client object for interacting with a Mckoi distributed network. This
 * interface encapsulates the primary functions needed for transactional
 * operations with a MckoiDDB network.
 * <p>
 * It is intended for implementations of this object to be long lived and
 * maintain various static information about network interactions, such as
 * node caches.
 * <p>
 * Use com.mckoi.network.MckoiDDBClientUtils to create instances of this
 * object.
 *
 * @author Tobias Downer
 */

public class MckoiDDBClient implements MckoiDDBAccess {

  /**
   * The network connector object.
   */
  private NetworkConnector network_connector;

  /**
   * The service tracker.
   */
  private ServiceStatusTracker service_tracker;

  /**
   * The location of the manager servers, ordered by access priority.
   */
  private final ServiceAddress[] manager_addresses;

  /**
   * The network password for accessing the network.
   */
  private final String network_password;

  /**
   * The network tree system manager object.
   */
  private NetworkTreeSystem tree_system;

  /**
   * The local network cache used to cache information about the network
   * on the heap.
   */
  private LocalNetworkCache local_network_cache;


  // ----- Property values -----

  private long maximum_transaction_node_cache_heap_size;


//  /**
//   * Default, constructor.
//   */
//  MckoiDDBClient(ServiceAddress[] manager_servers,
//                 String network_password) {
//    this(manager_servers, network_password,
//         JVMState.getJVMCacheForManager(manager_servers),
//         14 * 1024 * 1024);
//  }

  /**
   * Node cache constructor.
   */
  MckoiDDBClient(ServiceAddress[] manager_servers,
                 String network_password,
                 LocalNetworkCache lnc,
                 long maximum_transaction_node_cache_heap_size) {

    checkPermission(MckoiNetworkPermission.CREATE_MCKOIDDB_CLIENT);

    this.network_password = network_password;
    this.manager_addresses = manager_servers;
    this.local_network_cache = lnc;
    this.maximum_transaction_node_cache_heap_size = maximum_transaction_node_cache_heap_size;
  }

  /**
   * Security permission check.
   */
  private void checkPermission(MckoiNetworkPermission perm) {
    SecurityManager security = System.getSecurityManager();
    if (security != null) security.checkPermission(perm);
  }

  /**
   * Returns the maximum amount of heap memory in the local JVM allowed to be
   * used to cache transaction node data writes before a commit. If this heap
   * space is exhausted during a transaction interaction in which a lot of data
   * is being written, node data will be flushed from the cache to the network
   * to make additional room.
   */
  @Override
  public long getMaximumTransactionNodeCacheHeapSize() {
    return maximum_transaction_node_cache_heap_size;
  }

  /**
   * Sets the maximum amount of heap memory in the local JVM allowed to be
   * used to cache transaction node data writes before a commit. If this heap
   * space is exhausted during a transaction interaction in which a lot of data
   * is being written, node data will be flushed from the cache to the network
   * to make additional room.
   */
  @Override
  public void setMaximumTransactionNodeCacheHeapSize(long size_in_bytes) {
    maximum_transaction_node_cache_heap_size = size_in_bytes;
    if (tree_system != null) {
      tree_system.setMaximumNodeCacheHeapSize(size_in_bytes);
    }
  }

  /**
   * Returns a NetworkProfile object that allows low level inspection and
   * modification of the MckoiDDB network. Note that the network configuration
   * will still need to be set in the returned object before it can be used.
   * <p>
   * <b>Note</b>: This object is intended to be used by administrators. Do not
   * use it unless you know what you are doing.
   */
  public NetworkProfile getNetworkProfile(String su_password) {
    // SECURITY: This is not a function we should expose to user code
    //   unless there is some sort of priv check. In the current implementation
    //   the password is meaningless, but a future secure implementation should
    //   require a password or prevent the function working entirely in end-
    //   user code.
    return new NetworkProfile(network_connector, getNetworkPassword());
  }

  /**
   * Returns the network password.
   */
  String getNetworkPassword() {
    return network_password;
  }

  /**
   * Connects this client to the network.
   */
  void connectNetwork(NetworkConnector connector) {
    if (network_connector != null) {
      throw new RuntimeException("Already connected");
    }
    this.network_connector = connector;
    this.service_tracker = new ServiceStatusTracker(connector);
    this.tree_system = new NetworkTreeSystem(network_connector,
                      manager_addresses, local_network_cache, service_tracker);
    this.tree_system.setMaximumNodeCacheHeapSize(
                              getMaximumTransactionNodeCacheHeapSize());
  }

  /**
   * Disconnects this client from the network.
   */
  void disconnect() {
    if (this.network_connector != null) {
      try {
        this.network_connector.stop();
      }
      finally {
        try {
          this.service_tracker.stop();
        }
        finally {
          this.network_connector = null;
          this.service_tracker = null;
          this.tree_system = null;
        }
      }
    }
  }

  /**
   * Returns a TreeReportNode object used for diagnostic/reporting the
   * meta-data of a transaction tree..
   */
  @Override
  public TreeReportNode createDiagnosticGraph(KeyObjectTransaction t)
                                                           throws IOException {
    return tree_system.createDiagnosticGraph(t);
  }

  /**
   * Given a snapshot, traverses the nodes in the snapshot tree adding any
   * nodes that aren't already in the discovered node set into the set. This
   * method can be used for reachability determination as part of a garbage
   * collection function. For reachability determination, you would
   * incrementally call this method over all the historical roots of the
   * paths that need to be preserved. The method may also be used for some
   * diagnostic procedures.
   */
  @Override
  public void discoverNodesInSnapshot(PrintWriter warning_log,
                                      DataAddress root_node,
                                      DiscoveredNodeSet discovered_node_set) {
    try {
      tree_system.discoverNodesInTree(warning_log,
                                   root_node.getValue(), discovered_node_set);
    }
    catch (IOException e) {
      throw new RuntimeException("IO Error: " + e.getMessage());
    }
  }

  /**
   * Persists an empty database into the network and returns a DataAddress
   * object that references the root address of the empty database.
   */
  @Override
  public DataAddress createEmptyDatabase() {
    try {
      return tree_system.createEmptyDatabase();
    }
    catch (IOException e) {
      throw new NetworkWriteException(e.getMessage(), e);
    }
  }

  /**
   * Queries the manager server and returns the list of path instances
   * currently available on active root servers. This method will actively
   * query the network so the result should be cached by the user if frequent
   * use of this information is needed.
   */
  @Override
  public String[] queryAllNetworkPaths() {

    checkPermission(MckoiNetworkPermission.QUERY_ALL_NETWORK_PATHS);

    return tree_system.findAllPaths();
  }

  /**
   * Returns the name of the consensus function for the given database path.
   */
  @Override
  public String getConsensusFunction(String path_name) {
    return tree_system.getConsensusFunction(path_name);
  }

  /**
   * Returns the root node of the most current published version of the named
   * path instance in the network. If the path instance is not found on the
   * network, an exception is generated.
   */
  @Override
  public DataAddress getCurrentSnapshot(String path_name) {
    return tree_system.getPathNow(path_name);
  }

  /**
   * Given a DataAddress of a root node in the network, creates and returns a
   * new transaction object that can be used to access and modify the data
   * stored there.
   */
  @Override
  public KeyObjectTransaction createTransaction(DataAddress root_node) {
    // Create the transaction object and return it,
    return tree_system.createTransaction(root_node);
  }

  /**
   * Creates a new transaction object that contains an empty database, useful
   * for managing temporary structures that are never flushed.
   */
  @Override
  public KeyObjectTransaction createEmptyTransaction() {
    // Create the transaction object and return it,
    return tree_system.createEmptyTransaction();
  }


  /**
   * Flushes modifications made to a transaction out to the network storage
   * machines and returns a DataAddress of the root node of the flushed
   * data.
   * <p>
   * The object referenced by the DataAddress is immutable in the sense that
   * once a DataAddress has been created for a transaction it may be passed
   * around to other clients freely and they may modify the changes but their
   * modifications will not be visible to anyone else.
   */
  @Override
  public DataAddress flushTransaction(KeyObjectTransaction transaction) {
    return tree_system.flushTransaction(transaction);
  }

  /**
   * Performs a commit operation of the given path instance with the given
   * proposal. The proposal is represented as a DataAddress which should be the
   * result of a flushed transaction. The proposal is formatted in such a way
   * that the consensus processor assigned for the path on the root server will
   * understand how to interpret it.
   * <p>
   * Throws CommitFaultException if the consensus processor rejected the
   * proposal.
   */
  @Override
  public DataAddress performCommit(String path_name, DataAddress proposal)
                                                  throws CommitFaultException {
    return tree_system.performCommit(path_name, proposal);
  }

  /**
   * Disposes the KeyObjectTransaction previously created by the
   * createTransaction method of this object. This is intended as a way to
   * help reclaim resources associated with a transaction, but it is not
   * required to call this.
   */
  @Override
  public void disposeTransaction(KeyObjectTransaction transaction) {
    try {
      tree_system.disposeTransaction(transaction);
    }
    catch (IOException e) {
      throw new RuntimeException("IO Error: " + e.getMessage());
    }
  }

  /**
   * Returns an historical set of root nodes published to the root server
   * between the times given, where the time values follow the conventions of
   * System.currentTimeMillis().
   * <p>
   * Note that the returned root nodes may not be completely accurate because
   * the mechanism that records the time for each commit does not follow a
   * strict requirement for accuracy. For example, if a root server managing
   * commits for a path instance fails over to a new host, the clock on the
   * new host may be out of sync with the previous host thus it may appear
   * that some commits have happened at the same time and not in a serial
   * sequence.
   * <p>
   * Therefore consider the nodes returned by this method to be a reasonable
   * approximation of all the snapshot states committed in the given time span.
   */
  @Override
  public DataAddress[] getHistoricalSnapshots(String path_name,
                                             long time_start, long time_end) {

//    // Get the root server for the given path,
//    ServiceAddress root_address = tree_system.getRootServerFor(path_name);

    // Return the historical root nodes
    return tree_system.getPathHistorical(path_name, time_start, time_end);

  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    // Make sure to disconnect
    disconnect();
  }

}
