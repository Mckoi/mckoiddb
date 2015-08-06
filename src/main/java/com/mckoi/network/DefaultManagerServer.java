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

import com.mckoi.data.KeyObjectDatabase;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The manager server maintains a list of all active block servers on the
 * network, a database of block to server mappings, and serves to clients
 * address space allocation queries. The manager provides listeners for
 * server failure reports.
 * <p>
 * The manager manages policy for server failures by relaying reports of
 * server failures to clients that request block location information. The
 * manager counts the number of server failure reports received from servers,
 * and checks server status itself on receiving a failure report. The manager
 * periodically polls failed servers. When it is determined a server is
 * accessible, the server is no longer reported as 'down' to client requests.
 * <p>
 * A server is represented in one of the following states:
 * <p>
 * "DOWN SHUTDOWN" - the block server is marked as down by an administration
 * action. The block server may still serve client requests, however, block
 * servers marked as such should not be used by clients and are not returned
 * in new allocation requests. This state will not change until another
 * administrative action.
 * <p>
 * "DOWN CLIENT REPORT" - the block server is marked as down because a client
 * reported a failed action on the block server. This state is a temporary
 * state that reverts to "UP" if a heartbeat check on the server succeeds.
 * <p>
 * "DOWN HEARTBEAT" - this state happens when a heartbeat check on a block
 * server fails. When a heartbeat check on the server succeeds the server state
 * changes to "UP". An administrative action may also change this state.
 * <p>
 * "UP" - the server is UP when it is assumed the server is operating
 * correctly.
 *
 * @author Tobias Downer
 */

public abstract class DefaultManagerServer {

  /**
   * The network connector object.
   */
  private final NetworkConnector network;

  /**
   * The ServiceAddress to reference this manager.
   */
  private final ServiceAddress this_service;

  /**
   * A timer for scheduling background processes.
   */
  private final Timer timer;

  /**
   * The database that contains information about the blocks stored on the
   * servers in the network.
   */
  private KeyObjectDatabase block_database;

  /**
   * Write lock for the database, used to ensure update commits are serial.
   */
  private final Object block_db_write_lock = new Object();

  /**
   * The ReplicatedValueStore object.
   */
  private ReplicatedValueStore manager_db;


  /**
   * The unique network wide 8-bit id on this manager.
   */
  private int manager_unique_id = -1;


  /**
   * The map of all block servers on the network as MSBlockServer objects.
   */
  private final HashMap<Long, MSBlockServer> block_servers_map;

  /**
   * The list of all MSBlockServer objects being tracked by this manager.
   */
  private final ArrayList<MSBlockServer> block_servers_list;

  /**
   * The list of all MSRootServer objects being tracked by this manager.
   */
  private final ArrayList<MSRootServer> root_servers_list;

  /**
   * The list of all MSManagerServer objects being tracked by this manager.
   */
  private final ArrayList<MSManagerServer> manager_servers_list;


  /**
   * Tracks the availability of services on the network.
   */
  private final ServiceStatusTracker service_tracker;


  /**
   * The current end of the address space.
   */
  private DataAddress current_address_space_end;

  /**
   * A hint that indicates the last allocation is fresh and the information has
   * not been served out to any clients yet.
   */
  private volatile boolean fresh_allocation = false;

  /**
   * The servers the current block id is allocated against.
   */
  private long[] current_block_id_servers;

  /**
   * Lock for space allocation.
   */
  private final Object allocation_lock = new Object();





  /**
   * A random number generator.
   */
  private final Random rng;
  
  
  /**
   * Set to the stop state error in the case of a critical stop condition.
   */
  private volatile Throwable stop_state;

  /**
   * The logger.
   */
  private static final Logger log = Logger.getLogger("com.mckoi.network.Log");



  /**
   * Constructs the manager server on the given block database.
   */
  public DefaultManagerServer(NetworkConnector network,
                              ServiceAddress this_service,
                              Timer timer) {

    this.network = network;
    this.this_service = this_service;
    this.timer = timer;
    this.block_servers_map = new HashMap(256);
    this.block_servers_list = new ArrayList(256);
    this.root_servers_list = new ArrayList(256);
    this.manager_servers_list = new ArrayList(256);
    this.rng = new Random();

    this.service_tracker = new ServiceStatusTracker(network);

  }

  /**
   * Sets the block database object.
   */
  protected void setBlockDatabase(KeyObjectDatabase block_database) {
    this.block_database = block_database;
    // Set the manager database,
    manager_db = new ReplicatedValueStore(this_service, network,
                                          block_database, block_db_write_lock,
                                          service_tracker, timer);
  }

  /**
   * Performs the manager initialization procedure.
   */
  protected void doStart() {
    // Create a list of manager addresses,
    ServiceAddress[] managers;
    synchronized (manager_servers_list) {
      int sz = manager_servers_list.size();
      managers = new ServiceAddress[sz];
      for (int i = 0; i < sz; ++i) {
        managers[i] = manager_servers_list.get(i).address;
      }
    }

    // Perform the initialization
    manager_db.initialize();

//    // Sync with all the managers,
//    // (Action performed on timer thread).
//    for (int i = 0; i < managers.length; ++i) {
//      if (!managers[i].equals(this_service)) {
//        syncToManager(managers[i]);
//      }
//    }
//
//    // If no managers, set connected to true
//    if (managers.length == 0) {
//      connected = true;
//    }

    // Set the task where every 5 minutes we update a block service
//    timer.scheduleAtFixedRate(block_update_task, 10 * 1000, 5 * 60 * 1000);
    timer.scheduleAtFixedRate(block_update_task,
            rng.nextInt( 8 * 1000) + (15 * 1000),
            rng.nextInt(30 * 1000) + (5 * 60 * 1000) );

    // When the sync finishes, 'connected' is set to true.

  }

  /**
   * Performs the manager stop procedure.
   */
  protected void doStop() {
    // Cancel the block update task,
    block_update_task.cancel();
    // Stop the service tracker,
    this.service_tracker.stop();
  }

  /**
   * A TimerTask that notifies all the block servers of the current block id
   * being managed by this manager.
   */
  private TimerTask block_update_task = new TimerTask() {
    private boolean init = false;
    private int block_id_index = 0;
    private BlockId current_end_block = null;

    @Override
    public void run() {
      MSBlockServer block_to_check;

      // Cycle through the block servers list,
      synchronized (block_servers_map) {
        if (block_servers_list.isEmpty()) {
          return;
        }
        if (init == false) {
          block_id_index = rng.nextInt(block_servers_list.size());
        }
        block_to_check = block_servers_list.get(block_id_index);
        ++block_id_index;
        if (block_id_index >= block_servers_list.size()) {
          block_id_index = 0;
        }
        init = true;
      }

      // Notify the block server of the current block,
      BlockId current_block_id;
      synchronized (allocation_lock) {
        if (current_address_space_end == null) {
          if (current_end_block == null) {
            current_end_block = getCurrentBlockIdAlloc();
          }
          current_block_id = current_end_block;
        }
        else {
          current_block_id = current_address_space_end.getBlockId();
        }
      }

      // Notify the block server we are cycling through of the maximum block id.
      notifyBlockServerOfMaxBlockId(block_to_check.address, current_block_id);

//      // Log
//      log.log(Level.FINEST,
//              "Block server ({0}) notification, max block = {1}",
//                   new Object[] { block_to_check.address.displayString(),
//                                  current_block_id });

    }

  };


  /**
   * Returns a unique id for this manager.
   */
  private int getUniqueManagerID() {
    if (manager_unique_id == -1) {
      throw new RuntimeException(
                       "This manager has not been registered to the network");
    }
    return (manager_unique_id & 0x0FF);
  }

  /**
   * Returns the block id to which nodes will be allocated in this manager.
   * This function has no side effects.
   */
  private BlockId getCurrentBlockIdAlloc() {
    synchronized (allocation_lock) {
      if (current_address_space_end == null) {
        // Ask the manager cluster for the last block id
        BlockId block_id = manager_db.getLastBlockId();
        if (block_id == null) {
          // Initial state when the server map is empty,
          long nl = (256L & 0x0FFFFFFFFFFFFFF00L);
          nl += getUniqueManagerID();
          block_id = new BlockId(0, nl);
        }
        else {
          block_id = block_id.add(1024);
          // Clear the lower 8 bits and put the manager unique id there
          long nl = (block_id.getLowLong() & 0x0FFFFFFFFFFFFFF00L);
          nl += getUniqueManagerID();
          block_id = new BlockId(block_id.getHighLong(), nl);
        }

        return block_id;
      }
      else {
        return current_address_space_end.getBlockId();
      }
    }
  }

  /**
   * Ensures 'current_address_space_end' is set up appropriately for this
   * manager. Called before any access to the variable. If the current address
   * space end variable is not set, this function will update the manager
   * cluster db by allocating servers for the current block.
   */
  private void initCurrentAddressSpaceEnd() {
    synchronized (allocation_lock) {
      if (current_address_space_end == null) {

        // Gets the current block id being allocated against in this manager,
        BlockId block_id = getCurrentBlockIdAlloc();

        // Set the current address end,
        current_block_id_servers = allocateNewBlock(block_id);
        current_address_space_end = new DataAddress(block_id, 0);
      }
    }
  }

  /**
   * Returns true if one of the lists contains an element that isn't shared
   * between both.
   */
  private static boolean listsDifferent(long[] list1, long[] list2) {
    // Check list1 against list2,
    for (int i = 0; i < list1.length; ++i) {
      long val = list1[i];
      boolean found = false;
      for (int n = 0; n < list2.length; ++n) {
        if (list2[n] == val) {
          found = true;
          break;
        }
      }
      // If not found, return true (lists different),
      if (!found) {
        return true;
      }
    }

    // Check list2 against list1,
    for (int i = 0; i < list2.length; ++i) {
      long val = list2[i];
      boolean found = false;
      for (int n = 0; n < list1.length; ++n) {
        if (list1[n] == val) {
          found = true;
          break;
        }
      }
      // If not found, return true (lists different),
      if (!found) {
        return true;
      }
    }

    // Otherwise return false
    return false;
  }

  /**
   * Queries the given block server and creates an availability map for the
   * set of blocks given. This is used for network block discovery.
   */
  private byte[] findByteMapForBlocks(
                               ServiceAddress block_server, BlockId[] blocks) {

    MessageProcessor processor = network.connectBlockServer(block_server);

    MessageStream message_out;
    message_out = new MessageStream(16);
    message_out.addMessage("createAvailabilityMapForBlocks");
    message_out.addBlockIdArr(blocks);
    message_out.closeMessage();
    ProcessResult message_in = processor.process(message_out);

    for (Message m : message_in) {
      if (m.isError()) {
        // If the block server generates an error, return an empty array,
        return new byte[0];
      }
      else {
        // Return the availability map,
        return (byte[]) m.param(0);
      }
    }

    // We shouldn't get here,
    return new byte[0];
  }

  /**
   * Adds an already registered block server with this manager, used by the
   * 'start' function to populate the list of block servers the manager has
   * access to.
   */
  protected void addRegisteredBlockServer(
                                       long server_guid, ServiceAddress addr) {

    synchronized (block_servers_map) {
      MSBlockServer block_server = new MSBlockServer(server_guid, addr);

      // Add to the internal map/list
      block_servers_map.put(server_guid, block_server);
      block_servers_list.add(block_server);
    }

  }

  /**
   * Adds an already registered root server with this manager, used by the
   * 'start' function to populate the list of root servers the manager has
   * access to.
   */
  protected void addRegisteredRootServer(ServiceAddress addr) {

    synchronized (root_servers_list) {
      MSRootServer root_server = new MSRootServer();
      root_server.address = addr;

      // Add to the internal map/list
      root_servers_list.add(root_server);
    }

  }

  /**
   * Adds an already registered manager server with this manager, used by the
   * 'start' function to populate the list of manager servers.
   */
  protected void addRegisteredManagerServer(ServiceAddress addr) {

    synchronized (manager_servers_list) {
      MSManagerServer manager_server = new MSManagerServer();
      manager_server.address = addr;

      // Add to the internal map/list
      manager_servers_list.add(manager_server);

      // Add to the manager database
      manager_db.addMachine(addr);
    }

  }

  /**
   * Sets the unique id for this manager, used by the 'start' function to
   * instantiate this server process.
   */
  protected void setManagerUniqueId(int unique_id) {
    if (manager_unique_id != -1) {
      throw new RuntimeException("Unique id already set");
    }
    manager_unique_id = unique_id;
  }

  /**
   * Returns the MSBlockServer object of the block server on the network
   * that's registered with this manager with the given server guid value.
   */
  private MSBlockServer[] getServersInfo(long[] servers_guid) {
    ArrayList<MSBlockServer> reply;
    synchronized (block_servers_map) {
      int sz = servers_guid.length;
      reply = new ArrayList(sz);
      for (int i = 0; i < sz; ++i) {
        MSBlockServer block_server = block_servers_map.get(servers_guid[i]);
        if (block_server != null) {
          // Copy the server information into a new object.
          MSBlockServer nbs = new MSBlockServer(
                               block_server.server_guid, block_server.address);
          reply.add(nbs);
        }
      }
    }
    return reply.toArray(new MSBlockServer[reply.size()]);
  }



  /**
   * Persists the list of block servers registered with this manager, called
   * whenever 'registerBlockServer' or 'deregisterBlockServer' is called.
   */
  abstract void persistBlockServerList(List<MSBlockServer> servers_list);

  /**
   * Persists the list of root servers registered with this manager, called
   * whenever 'registerRootServer' or 'deregisterRootServer' is called.
   */
  abstract void persistRootServerList(List<MSRootServer> servers_list);

  /**
   * Persists the list of manager servers registered with this manager,
   * called whenever 'registerManagerServers' or 'deregisterManagerServer' is
   * called.
   */
  abstract void persistManagerServerList(List<MSManagerServer> servers_list);

  /**
   * Persists the unique id created for this manager server.
   */
  abstract void persistManagerUniqueId(int unique_id);


  /**
   * Informs the given root server of the managers, called when the manager
   * set or root set change.
   */
  private void informRootServerOfManagers(ServiceAddress root_server) {

    // Make the managers list
    ArrayList<ServiceAddress> managers = new ArrayList(64);
    synchronized (manager_servers_list) {
      for (MSManagerServer m : manager_servers_list) {
        managers.add(m.address);
      }
      // Add this manager to the list (manager_servers_list only contains the
      // set of other manager servers on the network).
      managers.add(this_service);
    }

    ServiceAddress[] managers_set =
                         managers.toArray(new ServiceAddress[managers.size()]);

    MessageStream message_out;
    message_out = new MessageStream(16);
    message_out.addMessage("informOfManagers");
    message_out.addServiceAddressArr(managers_set);
    message_out.closeMessage();

    // Open a connection to the root server,
    MessageProcessor processor = network.connectRootServer(root_server);
    ProcessResult message_in = processor.process(message_out);
    for (Message m : message_in) {
      if (m.isError()) {
        // If we failed, log a severe error but don't stop trying to register
        log.log(Level.SEVERE, "Couldn't inform root server of managers");
        log.log(Level.SEVERE, m.getExternalThrowable().getStackTrace());

        if (ReplicatedValueStore.isConnectionFault(m)) {
          service_tracker.reportServiceDownClientReport(root_server, "root");
        }
      }
    }

  }

  /**
   * Clears the root server of all the managers previously registered, called
   * when the manager set or root set change.
   */
  private void clearRootServerOfManagers(ServiceAddress root_server) {

    MessageStream message_out;
    message_out = new MessageStream(16);
    message_out.addMessage("clearOfManagers");
    message_out.closeMessage();

    // Open a connection to the root server,
    MessageProcessor processor = network.connectRootServer(root_server);
    ProcessResult message_in = processor.process(message_out);
    for (Message m : message_in) {
      if (m.isError()) {
        // If we failed, log a severe error but don't stop trying to register
        log.log(Level.SEVERE, "Couldn't inform root server of managers");
        log.log(Level.SEVERE, m.getExternalThrowable().getStackTrace());

        if (ReplicatedValueStore.isConnectionFault(m)) {
          service_tracker.reportServiceDownClientReport(root_server, "root");
        }
      }
    }

  }


  /**
   * Registers the replicated manager servers on the network with this manager.
   * This operation will sync the meta data from the manager servers on the
   * network to this manager.
   */
  private void registerManagerServers(
                                   ServiceAddress[] manager_server_addresses) {

    // Sanity check on number of manager servers (10 should be enough for
    // everyone !)
    if (manager_server_addresses.length > 100) {
      throw new RuntimeException("Number of manager servers > 100");
    }

    // Query all the manager servers on the network and generate a unique id
    // for this manager, if we need to create a new unique id,

    if (manager_unique_id == -1) {
      int sz = manager_server_addresses.length;
      ArrayList<Integer> blacklist_id = new ArrayList(sz);
      for (int i = 0; i < sz; ++i) {
        ServiceAddress man = manager_server_addresses[i];
        if (!man.equals(this_service)) {
          // Open a connection with the manager server,
          MessageProcessor processor = network.connectManagerServer(man);

          // Query the unique id of the manager server,
          MessageStream message_out;
          message_out = new MessageStream(16);
          message_out.addMessage("getUniqueId");
          message_out.closeMessage();
          ProcessResult message_in = processor.process(message_out);
          for (Message m : message_in) {
            if (m.isError()) {
              throw new RuntimeException(m.getErrorMessage());
            }
            else {
              long unique_id = (Long) m.param(0);
              if (unique_id == -1) {
                throw new RuntimeException("getUniqueId = -1");
              }
              // Add this to blacklist,
              blacklist_id.add((int) unique_id);
            }
          }
        }
      }

      // Find a random id not found in the blacklist,
      int gen_id;
      while (true) {
        gen_id = rng.nextInt(200);
        if (!blacklist_id.contains(gen_id)) {
          break;
        }
      }

      // Set the unique id,
      manager_unique_id = gen_id;
    }

    synchronized (manager_servers_list) {
      manager_servers_list.clear();
      manager_db.clearAllMachines();

      for (ServiceAddress m : manager_server_addresses) {
        if (!m.equals(this_service)) {
          MSManagerServer manager_server = new MSManagerServer();
          // Set the status and guid
          manager_server.address = m;

          manager_servers_list.add(manager_server);
          // Add to the manager database
          manager_db.addMachine(manager_server.address);
        }
      }
      persistManagerServerList(manager_servers_list);
      persistManagerUniqueId(manager_unique_id);
    }

    // Perform initialization on the manager
    manager_db.initialize();

    // Wait for initialization to complete,
    manager_db.waitUntilInitializeComplete();

    // Add a manager server entry,
    for (ServiceAddress manager_addr : manager_server_addresses) {
      manager_db.setValue("ms." + manager_addr.formatString(), "");
    }

    // Tell all the root servers of the new manager set,
    ArrayList<ServiceAddress> root_servers_set = new ArrayList(64);
    synchronized (root_servers_list) {
      for (MSRootServer rs : root_servers_list) {
        root_servers_set.add(rs.address);
      }
    }
    for (ServiceAddress r : root_servers_set) {
      informRootServerOfManagers(r);
    }

  }

  /**
   * Deregisters all the replicated manager servers from this manager.
   */
  private void deregisterManagerServer(
                   ServiceAddress manager_server_address) throws IOException {
    // Create a list of servers to be deregistered,
    ArrayList<MSManagerServer> to_remove;
    synchronized (manager_servers_list) {
      to_remove = new ArrayList(32);
      for (MSManagerServer item : manager_servers_list) {
        if (item.address.equals(manager_server_address)) {
          to_remove.add(item);
        }
      }
    }

    // Remove the entries and persist
    synchronized (manager_servers_list) {
      // Remove the entries that match,
      for (MSManagerServer item : to_remove) {
        manager_servers_list.remove(item);
        // Add to the manager database
        manager_db.removeMachine(item.address);
      }
      persistManagerServerList(manager_servers_list);

      // Clear the unique id if we are deregistering this service,
      if (manager_server_address.equals(this_service)) {
        manager_unique_id = -1;
        persistManagerUniqueId(manager_unique_id);
      }
    }

    // Perform initialization on the manager
    manager_db.initialize();

    // Wait for initialization to complete,
    manager_db.waitUntilInitializeComplete();

    // Remove the manager server entry,
    manager_db.setValue("ms." + manager_server_address.formatString(), null);

    // Tell all the root servers of the new manager set,
    ArrayList<ServiceAddress> root_servers_set = new ArrayList(64);
    synchronized (root_servers_list) {
      for (MSRootServer rs : root_servers_list) {
        root_servers_set.add(rs.address);
      }
    }
    for (ServiceAddress r : root_servers_set) {
      informRootServerOfManagers(r);
    }

  }

  /**
   * Registers a root server with this manager. A Root server managers a group
   * of consensus processors.
   */
  private void registerRootServer(ServiceAddress root_server_address) {

    // The root server object,
    MSRootServer root_server = new MSRootServer();
    root_server.address = root_server_address;

    // Add it to the map
    synchronized (root_servers_list) {
      root_servers_list.add(root_server);
      persistRootServerList(root_servers_list);
    }

    // Add the root server entry,
    manager_db.setValue("rs." + root_server_address.formatString(), "");

    // Tell root server about the managers.
    informRootServerOfManagers(root_server_address);

  }

  /**
   * Deregisters a root server from this manager and removes it from the pool
   * of servers. Deregistration takes the root server out of the network,
   * and is intended as an administration function.
   * <p>
   * If the deregistration fails for any reason, the server will not be
   * deregistered and the operation must be retried. If deregistration succeeds
   * then the root server address removal is persisted with this manager.
   */
  private void deregisterRootServer(ServiceAddress root_server_address) {

    // Remove it from the map and persist
    synchronized (root_servers_list) {
      // Find the server to remove,
      Iterator<MSRootServer> i = root_servers_list.iterator();
      while (i.hasNext()) {
        MSRootServer server = i.next();
        if (server.address.equals(root_server_address)) {
          i.remove();
        }
      }
      persistRootServerList(root_servers_list);
    }

    // Remove the root server entry,
    manager_db.setValue("rs." + root_server_address.formatString(), null);

    // Tell root server about the managers.
    clearRootServerOfManagers(root_server_address);

  }

  /**
   * Deregisters all root servers from the pool of servers managed by this
   * server. Deregistration takes the root server out of the network,
   * and is intended as an administration function.
   * <p>
   * If the deregistration fails for any reason, the server will not be
   * deregistered and the operation must be retried. If deregistration succeeds
   * then the root server address removal is persisted with this manager.
   */
  private void deregisterAllRootServers() throws IOException {

    // Create a list of servers to be deregistered,
    ArrayList<MSRootServer> to_remove;
    synchronized (root_servers_list) {
      to_remove = new ArrayList(root_servers_list.size());
      to_remove.addAll(root_servers_list);
    }

    // Remove the entries from the map and persist
    synchronized (root_servers_list) {
      // Remove the entries that match,
      for (MSRootServer item : to_remove) {
        root_servers_list.remove(item);
      }
      persistRootServerList(root_servers_list);
    }

    // Clear the managers set from all the root servers,
    for (MSRootServer item : to_remove) {
      clearRootServerOfManagers(item.address);
    }

  }

  /**
   * Registers a block server with this manager and adds it to the pool of
   * servers. When a block server is registered, the manager asks the server
   * for a report of all the blocks it holds.
   * <p>
   * If the registration fails for any reason, the server will not be
   * registered and must be retried. If registration succeeds then the block
   * server address is persisted with this manager.
   */
  private void registerBlockServer(ServiceAddress block_server_address) {

    // Get the block server uid,
    MessageStream message_out = new MessageStream(16);
    message_out.addMessage("serverGUID");
    message_out.closeMessage();

    // Connect to the block server,
    MessageProcessor processor =
                           network.connectBlockServer(block_server_address);
    ProcessResult message_in = processor.process(message_out);
    Message rm = null;
    for (Message m : message_in) {
      if (m.isError()) {
        throw new RuntimeException(m.getErrorMessage());
      }
      else {
        rm = m;
      }
    }
    long server_guid = (Long) rm.param(0);

    // Add lookup for this server_guid <-> service address to the db,
    manager_db.setValue("block.sguid." + Long.toString(server_guid),
                            block_server_address.formatString());
    manager_db.setValue("block.addr." + block_server_address.formatString(),
                            Long.toString(server_guid));

    // TODO: Block discovery on the introduced machine,




    // Set the status and guid
    MSBlockServer block_server =
                          new MSBlockServer(server_guid, block_server_address);
    // Add it to the map
    synchronized (block_servers_map) {
      block_servers_map.put(server_guid, block_server);
      block_servers_list.add(block_server);
      persistBlockServerList(block_servers_list);
    }

//    // Check that we aren't allocating against servers no longer in
//    // the list. If so, fix the error.
//    checkAndFixAllocationServers();
//    // Update the address space end variable,
//    updateAddressSpaceEnd();

  }

  /**
   * Deregisters a block server from this manager and removes it from the pool
   * of servers. Deregistration takes the block server out of the network,
   * and is intended as an administration function.
   * <p>
   * If the deregistration fails for any reason, the server will not be
   * deregistered and the operation must be retried. If deregistration succeeds
   * then the block server address removal is persisted with this manager.
   */
  private void deregisterBlockServer(ServiceAddress block_server_address) {

    // Remove from the db,
    final String block_addr_key =
                           "block.addr." + block_server_address.formatString();
    String server_sguid_str = manager_db.getValue(block_addr_key);
    if (server_sguid_str != null) {
      manager_db.setValue("block.sguid." + server_sguid_str, null);
      manager_db.setValue(block_addr_key, null);
    }

    // Remove it from the map and persist
    synchronized (block_servers_map) {
      // Find the server to remove,
      ArrayList<MSBlockServer> to_remove = new ArrayList();
      for (MSBlockServer server : block_servers_list) {
        if (server.address.equals(block_server_address)) {
          to_remove.add(server);
        }
      }
      // Remove the entries that match,
      for (MSBlockServer item : to_remove) {
        block_servers_map.remove(item.server_guid);
        block_servers_list.remove(item);
      }
      persistBlockServerList(block_servers_list);
    }

    // Check that we aren't allocating against servers no longer in
    // the list. If so, fix the error.
    checkAndFixAllocationServers();

  }

  /**
   * Deregisters all block servers from the pool of servers managed by this
   * server. Deregistration takes the block server out of the network,
   * and is intended as an administration function.
   * <p>
   * If the deregistration fails for any reason, the server will not be
   * deregistered and the operation must be retried. If deregistration succeeds
   * then the block server address removal is persisted with this manager.
   */
  private void deregisterAllBlockServers() throws IOException {

    // Create a list of servers to be deregistered,
    ArrayList<MSBlockServer> to_remove;
    synchronized (block_servers_map) {
      to_remove = new ArrayList(block_servers_list.size());
      to_remove.addAll(block_servers_list);
    }

    // Remove all items in the to_remove from the db,
    for (MSBlockServer item : to_remove) {
      ServiceAddress block_server_address = item.address;
      final String block_addr_key =
                           "block.addr." + block_server_address.formatString();
      String server_sguid_str = manager_db.getValue(block_addr_key);
      if (server_sguid_str != null) {
        manager_db.setValue("block.sguid." + server_sguid_str, null);
        manager_db.setValue(block_addr_key, null);
      }
    }

    // Remove the entries from the map and persist
    synchronized (block_servers_map) {
      // Remove the entries that match,
      for (MSBlockServer item : to_remove) {
        block_servers_map.remove(item.server_guid);
        block_servers_list.remove(item);
      }
      persistBlockServerList(block_servers_list);
    }

    // Check that we aren't allocating against servers no longer in
    // the list. If so, fix the error.
    checkAndFixAllocationServers();

  }





  /**
   * Returns the list of all path names managed by this manager server.
   */
  private String[] getAllPaths() {

    String prefix = "path.info.";

    // Get all the keys with prefix 'path.info.'
    String[] path_keys = manager_db.getAllKeys(prefix);

    // Remove the prefix
    for (int i = 0; i < path_keys.length; ++i) {
      path_keys[i] = path_keys[i].substring(prefix.length());
    }

    return path_keys;
  }

  /**
   * Returns the PathInfo object stored in the local database for the given
   * path name, or null if nothing stored.
   */
  private PathInfo getPathInfoForPath(String path_name) {

    String path_info_content = manager_db.getValue("path.info." + path_name);

    if (path_info_content == null) {
      return null;
    }

    // Create and return the path info object,
    return PathInfo.parseString(path_name, path_info_content);

  }

//  /**
//   * Returns the ServiceAddress that is the current root leader for the path.
//   */
//  private ServiceAddress getRootLeaderForPath(String path_name) {
//
//    String path_info_content = manager_db.getValue("path.info." + path_name);
//
//    // Create the path info object,
//    PathInfo path_info = PathInfo.parseString(path_name, path_info_content);
//
//    // Return the root leader,
//    return path_info.getRootLeader();
//
//  }

  /**
   * Adds a new path to the network with the given consensus function and
   * sets it up on the given root server. If the path already exists an
   * exception is generated. An exception is also generated if a majority
   * of managers is not currently available.
   */
  private void addPathToNetwork(String path_name, String consensus_fun,
                  ServiceAddress root_leader, ServiceAddress[] root_servers) {

    if (consensus_fun.contains(",")) {
      throw new RuntimeException("Invalid consensus function string");
    }
    if (path_name.contains(",")) {
      throw new RuntimeException("Invalid path name string");
    }

    String key = "path.info." + path_name;
    // Check the map doesn't already exist
    if (manager_db.getValue(key) != null) {
      throw new RuntimeException("Path already assigned");
    }

    // Set the first path info version for this path name
    PathInfo mpath_info = new PathInfo(
                      path_name, consensus_fun, 1, root_leader, root_servers);

    // Add the path to the manager db cluster.
    manager_db.setValue(key, mpath_info.formatString());

  }

  /**
   * Removes a path from the network.
   */
  private void removePathFromNetwork(String path_name) {

    String key = "path.info." + path_name;

    // Remove the path from the manager db cluster,
    manager_db.setValue(key, null);

  }



  /**
   * Given a block id, allocates the servers in the network where the block
   * is to be stored.
   * <p>
   * The server allocation rules, by default, are simple. Of the online
   * servers currently connected, randomly picks three unique servers
   * from the list. If only one or two servers are currently available, those
   * are picked.
   * <p>
   * Returns an empty array if no servers are available for allocating this
   * block.
   * <p>
   * PENDING: Configurable server allocation rules such as prioritizing
   * servers with the least blocks stored, avoiding servers with a lot of
   * blocks, etc. However, block distribution rules should probably be handled
   * by a background process.
   */
  private long[] allocateOnlineServerNodesForBlock(BlockId block_id) {
    // Fetch the list of all online servers,
    ArrayList<MSBlockServer> serv_set;
    synchronized (block_servers_map) {
      serv_set = new ArrayList(block_servers_list.size());
      for (MSBlockServer server : block_servers_list) {
        // Add the servers with status 'up'
        if (service_tracker.isServiceUp(server.address, "block")) {
          serv_set.add(server);
        }
      }
    }

    // PENDING: This is a simple random server picking method for a block.
    //   We should prioritize servers picked based on machine specs, etc.

    int sz = serv_set.size();
    // If serv_set is 3 or less, we return the servers available,
    if (sz <= 3) {
      long[] return_val = new long[sz];
      for (int i = 0; i < sz; ++i) {
        MSBlockServer block_server = serv_set.get(i);
        return_val[i] = block_server.server_guid;
      }
      return return_val;
    }

    // Randomly pick three servers from the list,
    long[] return_val = new long[3];
    for (int i = 0; i < 3; ++i) {
      // java.util.Random is specced to be thread-safe,
      int random_i = rng.nextInt(serv_set.size());
      MSBlockServer block_server = serv_set.remove(random_i);
      return_val[i] = block_server.server_guid;
    }

    // Return the array,
    return return_val;
  }

//  /**
//   * A listener for changes to the status of tracked events.
//   */
//  private class ManagerStatusListener implements ServiceStatusListener {
//
//    public void statusChange(ServiceAddress address,
//                  String service_type, String old_status, String new_status) {
//      // If a tracked manager status has changed to UP
//      if (service_type.equals("manager") &&
//          new_status.startsWith("UP")) {
//
//        log.log(Level.INFO, "Manager {0} status changed to UP", address);
//
//        // Sync the data in this manager with the given manager,
//        syncToManager(address);
//
//      }
//
//      // Replay queued events
//      comm.retryMessagesFor(address);
//
//    }
//
//  }

//  /**
//   * Called when a manager server is polled and found to be available.
//   */
//  private class ManagerServiceAvailableAction implements Runnable {
//    private final ServiceAddress manager;
//    ManagerServiceAvailableAction(ServiceAddress manager) {
//      this.manager = manager;
//    }
//    public void run() {
//      // Sync the data in this manager with the given manager,
//      syncToManager(manager);
//    }
//  }

//  /**
//   * Queries the other manager servers on the network for information about the
//   * given block_id. This happens when the data the managers hold goes out
//   * of sync because of failures.
//   */
//  private long[] delegateGetOnlineServersWithBlock(BlockId block_id) {
//
//    // The current list of manager servers registered,
//    ArrayList<MSManagerServer> managers_list;
//    synchronized (manager_servers_list) {
//      int sz = manager_servers_list.size();
//      managers_list = new ArrayList(sz);
//      for (MSManagerServer server : manager_servers_list) {
//        managers_list.add(server);
//      }
//    }
//
//    // Log this,
//    log.log(Level.FINER,
//            "delegateGetOnlineServers (managers_list.size() = {0})",
//            new Object[] { managers_list.size() });
//
//    // Create the message,
//    MessageStream message_out = new MessageStream(16);
//    message_out.addMessage("delegateGetServerList");
//    message_out.addBlockId(block_id);
//    message_out.closeMessage();
//
//    // For each manager server,
//    for (MSManagerServer server : managers_list) {
//      // Only notify managers that are currently tracked as 'up'
//      if (service_tracker.isServiceUp(server.address, "manager")) {
//        // Open a connection with the manager server,
//        MessageProcessor processor =
//                                 network.connectManagerServer(server.address);
//
//        // Send the command to the manager.
//        ProcessResult message_in = processor.process(message_out);
//
//        // Log this,
//        log.log(Level.FINER,
//                "delegateGetOnlineServers {0} @ {1}", new Object[] {
//                       block_id.toString(), server.address.displayString() });
//
//        for (Message m : message_in) {
//          if (m.isError()) {
//            log.info("Failed: Unable to connect to manager for delegate block query.");
//            log.info("block: " + block_id +
//                     " manager: " + server.address.displayString());
//
//            // Report it down to the tracker,
//            service_tracker.reportServiceDownClientReport(
//                                                    server.address, "manager");
//
//          }
//          else {
//            long[] servers = (long[]) m.param(0);
//
//            log.log(Level.FINER, "servers.length = {0}", servers.length);
//
//            if (servers.length > 0) {
//              return servers;
//            }
//          }
//        }
//      }
//    }
//
//    // Return empty array if all managers failed to resolve block,
//    return new long[0];
//
//  }


  /**
   * Notifies the block server of the current top block_id being managed by
   * this manager.
   */
  private void notifyBlockServerOfMaxBlockId(
                               ServiceAddress block_server, BlockId block_id) {

    if (service_tracker.isServiceUp(block_server, "block")) {
      MessageStream msg_out = new MessageStream(8);
      msg_out.addMessage("notifyCurrentBlockId");
      msg_out.addBlockId(block_id);
      msg_out.closeMessage();
      // Connect to the block server,
      MessageProcessor processor = network.connectBlockServer(block_server);
      ProcessResult message_in = processor.process(msg_out);
      // If the block server is down, report it to the tracker,
      for (Message m : message_in) {
        if (m.isError()) {
          if (ReplicatedValueStore.isConnectionFault(m)) {
            service_tracker.reportServiceDownClientReport(
                                                     block_server, "block");
          }
        }
      }
    }
  }

  /**
   * Informs all the block servers with an sguid from 'block_servers_notify'
   * that the given block id is the current block id being allocated against.
   */
  private void notifyBlockServersOfCurrentBlockId(
                               long[] block_servers_notify, BlockId block_id) {

    // Copy the block servers list for concurrency safety,
    ArrayList<MSBlockServer> block_servers_list_copy = new ArrayList(64);
    synchronized (block_servers_map) {
      block_servers_list_copy.addAll(block_servers_list);
    }

    // For each block server
    for (MSBlockServer block_server : block_servers_list_copy) {
      // Is it in the block_servers_notify list?
      boolean found = false;
      for (long bsn : block_servers_notify) {
        if (block_server.server_guid == bsn) {
          found = true;
          break;
        }
      }

      // If found and the service is up,
      if (found) {
        notifyBlockServerOfMaxBlockId(block_server.address, block_id);
      }

    }
  }

  /**
   * Assigns the servers that will hold the given block id, updates the
   * internal database, and synchronizes the information with other manager
   * servers on the network. Will return an empty server list if there are
   * no block servers.
   * <p>
   * NOTE: This must happen when synchronized on 'allocation_lock'.
   */
  private long[] allocateNewBlock(final BlockId block_id) {

    synchronized (allocation_lock) {

      // Schedule a task that informs all the current block servers what the
      // new block being allocated against is. This notifies them that they
      // can perform maintenance on all blocks preceding, such as compression.
      final long[] block_servers_notify = current_block_id_servers;
      if (block_servers_notify != null && block_servers_notify.length > 0) {
        timer.schedule(new TimerTask() {
          @Override
          public void run() {
            notifyBlockServersOfCurrentBlockId(block_servers_notify, block_id);
          }
        }, 500);
      }

      // Assert the block isn't already allocated,
      long[] current_servers = manager_db.getBlockIdServerMap(block_id);
      if (current_servers.length > 0) {
        throw new NetworkWriteException(
                                     "Block already allocated: " + block_id);
      }

      // Allocate a group of servers from the pool of block servers for the
      // given block_id
      long[] servers = allocateOnlineServerNodesForBlock(block_id);

      // Update the database if we have servers to allocate the block id,
      if (servers.length > 0) {
        manager_db.setBlockIdServerMap(block_id, servers);
      }

      // Return the list,
      return servers;

    }
  }

  /**
   * Given a block identifer, returns the servers that the block is found
   * on according to the manager server database. If there are no servers
   * registered with the given block then an empty array is returned.
   */
  private long[] queryBlockServersWithBlock(final BlockId block_id) {

    // Fetch the server map for the block from the db cluster,
    long[] server_guids = manager_db.getBlockIdServerMap(block_id);
    return server_guids;


//    log.log(Level.FINER, "queryBlockServersWithBlock {0}", block_id.toString());
//
//    long[] servers;
//    // Note; we perform these operations inside a lock because we may need to
//    //  provision servers to contain certain blocks which requires a database
//    //  update.
//    synchronized (block_db_write_lock) {
//      // Create a transaction
//      KeyObjectTransaction transaction = block_database.createTransaction();
//      try {
//        // Get the map,
//        BlockServerMap block_server_map = new BlockServerMap(
//                             transaction.getDataFile(BLOCK_SERVER_KEY, 'w'));
//
//        // Get the servers list,
//        servers = block_server_map.get(block_id);
//      }
//      finally {
//        block_database.dispose(transaction);
//      }
//    }
//
//    log.log(Level.FINER, "local db says {0} for {1}", new Object[] {
//                        servers.length, block_id.toString()
//    });
//
//    // If we don't know where the block is stored, we ask if any of the
//    // registered managers have knowledge about it,
//
//    if (servers.length == 0) {
//      servers = delegateGetOnlineServersWithBlock(block_id);
//
//      if (servers.length > 0) {
//        log.log(Level.FINER, "updating local db {0}", new Object[] {
//                            block_id.toString()
//        });
//        // Update the db,
//        synchronized (block_db_write_lock) {
//          // Create a transaction
//          KeyObjectTransaction transaction = block_database.createTransaction();
//          try {
//            // Get the map,
//            BlockServerMap block_server_map = new BlockServerMap(
//                               transaction.getDataFile(BLOCK_SERVER_KEY, 'w'));
//
//            // Create the mapping for the block to the servers,
//            int sz = servers.length;
////            if (sz < 3) {
////              log.log(Level.SEVERE, "PUTTING less than 3 entries in map!", new Error());
////            }
//            for (int i = 0; i < sz; ++i) {
//              block_server_map.put(block_id, servers[i]);
//            }
//
//            // Get the servers list,
//            servers = block_server_map.get(block_id);
//          }
//          finally {
//            block_database.dispose(transaction);
//          }
//        }
//      }
//    }
//
//    return servers;

  }



  /**
   * Used to manually add a block_id to server_guid mapping to the manager
   * database. This is used when moving data blocks around in the network and
   * updating the manager db state.
   */
  private void internalAddBlockServerMapping(
                                      BlockId block_id, long[] server_guids) {

    long[] current_server_guids = manager_db.getBlockIdServerMap(block_id);
    ArrayList<Long> server_list = new ArrayList(64);
    for (long s : current_server_guids) {
      server_list.add(s);
    }

    // Add the servers to the list,
    for (long s : server_guids) {
      if (!server_list.contains(s)) {
        server_list.add(s);
      }
    }

    // Set the new list
    if (!server_list.isEmpty()) {
      long[] new_server_guids = new long[server_list.size()];
      for (int i = 0; i < server_list.size(); ++i) {
        new_server_guids[i] = server_list.get(i);
      }

      manager_db.setBlockIdServerMap(block_id, new_server_guids);
    }

  }

  /**
   * Used to manually remove a block_id to server_guid mapping from the
   * manager database. This is used when moving data blocks around in the
   * network and updating the manager db state.
   */
  private void internalRemoveBlockServerMapping(
                                      BlockId block_id, long[] server_guids) {

    long[] current_server_guids = manager_db.getBlockIdServerMap(block_id);
    ArrayList<Long> server_list = new ArrayList(64);
    for (long s : current_server_guids) {
      server_list.add(s);
    }

    // Remove the servers from the list
    for (long s : server_guids) {
      int index = server_list.indexOf(s);
      if (index >= 0) {
        server_list.remove(index);
      }
    }

    // Set the new list
    long[] new_server_guids = new long[server_list.size()];
    for (int i = 0; i < server_list.size(); ++i) {
      new_server_guids[i] = server_list.get(i);
    }

    manager_db.setBlockIdServerMap(block_id, new_server_guids);

  }




  /**
   * Checks if any of the servers assigned as allocation servers are currently
   * assigned as 'down'. If so, forces a new allocation set of servers.
   */
  private void checkAndFixAllocationServers() {

    // If the failure report is on a block server that is servicing allocation
    // requests, we push the allocation requests to the next block.
    final BlockId current_block_id;
    synchronized (allocation_lock) {
      // Check address_space_end is initialized
      initCurrentAddressSpaceEnd();
      current_block_id = current_address_space_end.getBlockId();
    }
    long[] bservers = queryBlockServersWithBlock(current_block_id);
    int ok_server_count = 0;

    synchronized (block_servers_map) {
      // For each server that stores the block,
      for (int i = 0; i < bservers.length; ++i) {
        long server_guid = bservers[i];
        // Is the status of this server UP?
        for (MSBlockServer block_server : block_servers_list) {
          // If this matches the guid, and is up, we add to 'ok_server_count'
          if (block_server.server_guid == server_guid &&
              service_tracker.isServiceUp(block_server.address, "block")) {
            ++ok_server_count;
          }
        }
      }
    }

    // If the count of ok servers for the allocation set size is not
    // the same then there are one or more servers that are inoperable
    // in the allocation set. So, we increment the block id ref of
    // 'current_address_space_end' by 1 to force a reevaluation of the
    // servers to allocate the current block.
    if (ok_server_count != bservers.length) {
      log.log(Level.FINE,
              "Moving current_address_space_end past unavailable block");

      boolean next_block = false;
      BlockId block_id;
      synchronized (allocation_lock) {
        block_id = current_address_space_end.getBlockId();
        int data_id = current_address_space_end.getDataId();
        DataAddress new_address_space_end = null;
        if (current_block_id.equals(block_id)) {
          block_id = block_id.add(256);
          data_id = 0;
          new_address_space_end = new DataAddress(block_id, data_id);
          next_block = true;
        }

        // Allocate a new block (happens under 'allocation_lock')
        if (next_block) {
          current_block_id_servers = allocateNewBlock(block_id);
          current_address_space_end = new_address_space_end;
        }

      }

    }

  }

  /**
   * Notifies that a client operation on a block server failed, and that the
   * state of the server should be changed to reflect this status.
   */
  private void notifyBlockServerFailure(ServiceAddress server_address) {

    // If the server currently recorded as up,
    if (service_tracker.isServiceUp(server_address, "block")) {
      // Report the block service down to the service tracker,
      service_tracker.reportServiceDownClientReport(server_address, "block");
    }

    // Change the allocation point if we are allocating against servers that
    // have failed,
    checkAndFixAllocationServers();
  }

  /**
   * Notifies that a client was unable to read from a block on a block server,
   * because of an exception, a CRC failure, or a node data format error.
   */
  private void notifyBlockIdCorruption(ServiceAddress server_address,
          BlockId block_id, String failure_type) {

    // PENDING

  }

  /**
   * Returns a processor of messages on this server.
   */
  public MessageProcessor getProcessor() {
    // Check for stop state,
    checkStopState();

    return new DefaultManagerServerProcessor();
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





  
  

  // ---------- Inner classes ---------

  /**
   * Block server status information maintained by the manager server.
   */
  static class MSBlockServer {
    // The unique id given this block server,
    final long server_guid;
    // The address/port of the block server
    final ServiceAddress address;

    MSBlockServer(long server_guid, ServiceAddress address) {
      this.server_guid = server_guid;
      this.address = address;
    }

    public boolean equals(Object ob) {
      return this == ob;
    }
  }

  /**
   * Root server status information maintained by the manager server.
   */
  static class MSRootServer {
    // The address/port of the root server
    ServiceAddress address;

    public boolean equals(Object ob) {
      return this == ob;
    }
  }

  /**
   * Manager server status information maintained by the manager server.
   */
  static class MSManagerServer {
    // The address/port of the manager server
    ServiceAddress address;

    public boolean equals(Object ob) {
      return this == ob;
    }
  }


  /**
   * Client communication with the manager server.
   */
  private class DefaultManagerServerProcessor extends AbstractProcessor {

    /**
     * {@inheritDoc }
     */
    @Override
    public ProcessResult process(MessageStream message_stream) {
      // The reply message,
      MessageStream reply_message = new MessageStream(32);

      // The messages in the stream,
      Iterator<Message> iterator = message_stream.iterator();
      while (iterator.hasNext()) {
        Message m = iterator.next();
        try {
          // Check the server isn't in a stop state,
          checkStopState();
          String cmd = m.getName();

          // getServerList(BlockId block_id)
          if (cmd.equals("getServerList")) {
            MSBlockServer[] servers = getServerList((BlockId) m.param(0));
            reply_message.addMessage("R");
            reply_message.addInteger(servers.length);
            for (int i = 0; i < servers.length; ++i) {
              reply_message.addServiceAddress(servers[i].address);
              reply_message.addString(
                      service_tracker.getServiceCurrentStatus(
                                                servers[i].address, "block"));
            }
            reply_message.closeMessage();
          }
//          // delegateGetServerList(BlockId block_id)
//          else if (cmd.equals("delegateGetServerList")) {
//            long[] servers = delegateGetServerList((BlockId) m.param(0));
//            reply_message.addMessage("R");
//            reply_message.addLongArray(servers);
//            reply_message.closeMessage();
//          }
          // allocateNode(int node_size)
          else if (cmd.equals("allocateNode")) {
            DataAddress address = allocateNode((Integer) m.param(0));
            reply_message.addMessage("R");
            reply_message.addDataAddress(address);
            reply_message.closeMessage();
          }
          // registerBlockServer(ServiceAddress service_address)
          else if (cmd.equals("registerBlockServer")) {
            registerBlockServer((ServiceAddress) m.param(0));
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }
          // deregisterBlockServer(ServiceAddress service_address)
          else if (cmd.equals("deregisterBlockServer")) {
            deregisterBlockServer((ServiceAddress) m.param(0));
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }
          // deregisterAllBlockServers()
          else if (cmd.equals("deregisterAllBlockServers")) {
            deregisterAllBlockServers();
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }

          // registerManagerServers(ServiceAddress[] managers)
          else if (cmd.equals("registerManagerServers")) {
            registerManagerServers((ServiceAddress[]) m.param(0));
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }
          // deregisterAllManagerServers()
          else if (cmd.equals("deregisterManagerServer")) {
            deregisterManagerServer((ServiceAddress) m.param(0));
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }

//          // setRootLeaderForPath(String path_name, ServiceAddress root)
//          else if (cmd.equals("setRootLeaderForPath")) {
//            setRootLeaderForPath((String) m.param(0),
//                                 (ServiceAddress) m.param(1));
//            reply_message.addMessage("R");
//            reply_message.addInteger(1);
//            reply_message.closeMessage();
//          }
//          // addPathRootMapping(String path_name, ServiceAddress root)
//          else if (cmd.equals("addPathRootMapping")) {
//            checkConnected();
//            addPathRootMapping((String) m.param(0),
//                               (ServiceAddress) m.param(1));
//            reply_message.addMessage("R");
//            reply_message.addInteger(1);
//            reply_message.closeMessage();
//          }
//          // removePathRootMapping(String path_name, ServiceAddress root)
//          else if (cmd.equals("removePathRootMapping")) {
//            checkConnected();
//            removePathRootMapping((String) m.param(0),
//                                  (ServiceAddress) m.param(1));
//            reply_message.addMessage("R");
//            reply_message.addInteger(1);
//            reply_message.closeMessage();
//          }
          // addPathToNetwork(String path_name, String consensus,
          //                  ServiceAddress root,
          //                  ServiceAddress[] root_servers)
          else if (cmd.equals("addPathToNetwork")) {
            addPathToNetwork((String) m.param(0), (String) m.param(1),
                             (ServiceAddress) m.param(2),
                             (ServiceAddress[]) m.param(3));
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }
          // removePathFromNetwork(String path_name)
          else if (cmd.equals("removePathFromNetwork")) {
            removePathFromNetwork((String) m.param(0));
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }

//          // getServerGUIDList(BlockId block_id)
//          else if (cmd.equals("getServerGUIDList")) {
//            long[] server_guids = getServerGUIDList((BlockId) m.param(0));
//            reply_message.addMessage("R");
//            reply_message.addLongArray(server_guids);
//            reply_message.closeMessage();
//          }
          // addBlockServerMapping(BlockId block_id, long[] server_guids)
          else if (cmd.equals("internalAddBlockServerMapping")) {
            internalAddBlockServerMapping(
                                   (BlockId) m.param(0), (long[]) m.param(1));
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }
          // removeBlockServerMapping(BlockId block_id, long[] server_guids)
          else if (cmd.equals("internalRemoveBlockServerMapping")) {
            internalRemoveBlockServerMapping(
                                   (BlockId) m.param(0), (long[]) m.param(1));
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }
//          // internalAssignBlock(BlockId block_id, long[] server_guids)
//          else if (cmd.equals("internalAssignBlock")) {
//            internalAssignBlock((BlockId) m.param(0), (long[]) m.param(1));
//            reply_message.addMessage("R");
//            reply_message.addLong(1);
//            reply_message.closeMessage();
//          }
//          // internalRetireBlock(BlockId block_id, long[] server_guids)
//          else if (cmd.equals("internalRetireBlock")) {
//            internalRetireBlock((BlockId) m.param(0), (long[]) m.param(1));
//            reply_message.addMessage("R");
//            reply_message.addLong(1);
//            reply_message.closeMessage();
//          }


//          // internalAddPathRootMapping
//          else if (cmd.equals("internalAddPathRootMapping")) {
//            internalAddPathRootMapping((String) m.param(0),
//                                       (ServiceAddress) m.param(1));
//            reply_message.addMessage("R");
//            reply_message.addInteger(1);
//            reply_message.closeMessage();
//          }
//          else if (cmd.equals("internalRemovePathRootMapping")) {
//            internalRemovePathRootMapping((String) m.param(0),
//                                          (ServiceAddress) m.param(1));
//            reply_message.addMessage("R");
//            reply_message.addInteger(1);
//            reply_message.closeMessage();
//          }
//          else if (cmd.equals("internalSetRootLeaderForPath")) {
//            internalSetRootLeaderForPath((String) m.param(0),
//                                         (ServiceAddress) m.param(1));
//            reply_message.addMessage("R");
//            reply_message.addInteger(1);
//            reply_message.closeMessage();
//          }


          // --- Consensus processors ---

          // registerRootServer(ServiceAddress root_server)
          else if (cmd.equals("registerRootServer")) {
            registerRootServer((ServiceAddress) m.param(0));
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }
          // deregisterRootServer(ServiceAddress root_server)
          else if (cmd.equals("deregisterRootServer")) {
            deregisterRootServer((ServiceAddress) m.param(0));
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }
          // deregisterAllConsensusProcessors()
          else if (cmd.equals("deregisterAllRootServers")) {
            deregisterAllRootServers();
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }

          // PathInfo getPathInfoForPath(String path_name)
          else if (cmd.equals("getPathInfoForPath")) {
            PathInfo path_info = getPathInfoForPath((String) m.param(0));
            reply_message.addMessage("R");
            reply_message.addPathInfo(path_info);
            reply_message.closeMessage();
          }
//          // ServiceAddress getRootLeaderForPath(String path_name)
//          else if (cmd.equals("getRootLeaderForPath")) {
//            ServiceAddress addr = getRootLeaderForPath((String) m.param(0));
//            reply_message.addMessage("R");
//            reply_message.addServiceAddress(addr);
//            reply_message.closeMessage();
//          }
          // String[] getAllPaths()
          else if (cmd.equals("getAllPaths")) {
            String[] path_set = getAllPaths();
            reply_message.addMessage("R");
            reply_message.addStringArr(path_set);
            reply_message.closeMessage();
          }
//          // getRootLeadersForPaths(String[] path_names)
//          else if (cmd.equals("getRootLeadersForPaths")) {
//            ServiceAddress[] roots =
//                               getRootLeadersForPaths((String[]) m.param(0));
//            reply_message.addMessage("R");
//            reply_message.addServiceAddressArr(roots);
//            reply_message.closeMessage();
//          }

          // getRegisteredServerList()
          else if (cmd.equals("getRegisteredServerList")) {
            getRegisteredServerList(reply_message);
          }
          // getRegisteredBlockServers()
          else if (cmd.equals("getRegisteredBlockServers")) {
            getRegisteredBlockServers(reply_message);
          }
          // getRegisteredRootServers()
          else if (cmd.equals("getRegisteredRootServers")) {
            getRegisteredRootServers(reply_message);
          }

          // notifyBlockServerFailure(ServiceAddress service_address)
          else if (cmd.equals("notifyBlockServerFailure")) {
            notifyBlockServerFailure((ServiceAddress) m.param(0));
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }

          // notifyBlockIdCorruption(
          //    ServiceAddress root_server, BlockId block_id, String fail_type)
          else if (cmd.equals("notifyBlockIdCorruption")) {
            notifyBlockIdCorruption(
                    (ServiceAddress) m.param(0), (BlockId) m.param(1),
                    (String) m.param(2));
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }

          // getUniqueId()
          else if (cmd.equals("getUniqueId")) {
            long unique_id = manager_unique_id;
            reply_message.addMessage("R");
            reply_message.addLong(unique_id);
            reply_message.closeMessage();
          }

          // poll(String poll_msg)
          else if (m.getName().equals("poll")) {
            manager_db.checkConnected();
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }

          else {

            // Defer to the manager db process command,
            manager_db.process(m, reply_message);

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
     * Returns the address and current status of registered servers with this
     * manager.
     */
    private void getRegisteredServerList(MessageStream msg_out) {
      // Populate the list of registered servers
      ServiceAddress[] srvs;
      String[] status_codes;
      synchronized (block_servers_map) {
        int sz = block_servers_list.size();
        srvs = new ServiceAddress[sz];
        status_codes = new String[sz];
        int i = 0;
        for (MSBlockServer m : block_servers_list) {
          srvs[i] = m.address;
          status_codes[i] =
                  service_tracker.getServiceCurrentStatus(m.address, "block");
          ++i;
        }
      }
      // Populate the reply message,
      msg_out.addMessage("R");
      msg_out.addServiceAddressArr(srvs);
      msg_out.addStringArr(status_codes);
      msg_out.closeMessage();
    }

    /**
     * Gets the list of block servers registered to this manager.
     */
    private void getRegisteredBlockServers(MessageStream msg_out) {
      // Populate the list of registered block servers
      long[] guids;
      ServiceAddress[] srvs;
      synchronized (block_servers_map) {
        int sz = block_servers_list.size();
        guids = new long[sz];
        srvs = new ServiceAddress[sz];
        int i = 0;
        for (MSBlockServer m : block_servers_list) {
          guids[i] = m.server_guid;
          srvs[i] = m.address;
          ++i;
        }
      }
      // The reply message,
      msg_out.addMessage("R");
      msg_out.addLongArray(guids);
      msg_out.addServiceAddressArr(srvs);
      msg_out.closeMessage();
    }

    /**
     * Gets the list of root servers registered to this manager.
     */
    private void getRegisteredRootServers(MessageStream msg_out) {
      // Populate the list of registered root servers
      ServiceAddress[] srvs;
      synchronized (root_servers_list) {
        int sz = root_servers_list.size();
        srvs = new ServiceAddress[sz];
        int i = 0;
        for (MSRootServer m : root_servers_list) {
          srvs[i] = m.address;
          ++i;
        }
      }
      // The reply message,
      msg_out.addMessage("R");
      msg_out.addServiceAddressArr(srvs);
      msg_out.closeMessage();
    }

    /**
     * Given a block_id, returns the list of servers that hold copies of the
     * block according to the manager server database.
     */
    private MSBlockServer[] getServerList(BlockId block_id) {

      // Query the local database for the server list of the block.  If the
      // block doesn't exist in the database then it provisions it over the
      // network.

      long[] server_ids = queryBlockServersWithBlock(block_id);

      // Resolve the server ids into server names and parse it as a reply
      int sz = server_ids.length;

      // No online servers contain the block
      if (sz == 0) {
        throw new RuntimeException("No online servers for block: " + block_id);
      }

      MSBlockServer[] reply = getServersInfo(server_ids);

      log.log(Level.FINER, "getServersInfo replied {0} for {1}",
                          new Object[] { reply.length, block_id.toString() });

      return reply;
    }

    /**
     * Allocates an amount of space out of the global address space to store
     * a node of data of the given size, and returns a DataAddress object to
     * reference the node. The size of the allocated area must be
     * smaller than the maximum block container size.
     */
    private DataAddress allocateNode(int node_size) {

      if (node_size >= 65536) {
        throw new IllegalArgumentException("node_size too large");
      }
      else if (node_size < 0) {
        throw new IllegalArgumentException("node_size too small");
      }

      final BlockId block_id;
      final int data_id;
      boolean next_block = false;
      BlockId next_block_id;

      synchronized (allocation_lock) {

        // Check address_space_end is initialized
        initCurrentAddressSpaceEnd();

        // Set fresh allocation to false because we allocated off the
        // current address space,
        fresh_allocation = false;

        // Fetch the current block of the end of the address space,
        block_id = current_address_space_end.getBlockId();
        // Get the data identifier,
        data_id = current_address_space_end.getDataId();

        // The next position,
        int next_data_id = data_id;
        next_block_id = block_id;
        ++next_data_id;
        if (next_data_id >= 16384) {
          next_data_id = 0;
          next_block_id = next_block_id.add(256);
          next_block = true;
        }

        // Before we return this allocation, if we went to the next block we
        // sync the block allocation with the other managers.

        if (next_block) {
          long[] next_block_id_servers = allocateNewBlock(next_block_id);
          if (next_block_id_servers.length == 0) {
            throw new RuntimeException("No block servers available.");
          }
          current_block_id_servers = next_block_id_servers;
        }

        // Update the address space end,
        current_address_space_end =
                              new DataAddress(next_block_id, next_data_id);

      }

      // Return the data address,
      return new DataAddress(block_id, data_id);
    }

  }

}
