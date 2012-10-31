/**
 * com.mckoi.network.ReplicatedValueStore  Jun 25, 2010
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

import com.mckoi.data.*;
import java.io.*;
import java.security.SecureRandom;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A replicated server_uids store is a basic fault-tolerant replicated block_id/server_uids
 * database for managing meta-data in a MckoiDDB network. This system is
 * intended to handle the following functions; a) To manage block/server
 * information to support block lookups, b) To manage a database of path
 * information (servers, leaders and consensus function), c) To handle the
 * election of new path leaders atomically.
 * <p>
 * A secondary goal is to support a mechanism for distributed fault-tolerant
 * locking.
 * <p>
 * The distributed mechanism is described as follows;
 * <p>
 * State is automatically replicated over all the machines. If a machine is
 * not available, the message is added to a message queue and periodically
 * retried until the machine becomes available. When a machine goes from
 * being unavailable to available, it asks all the machines in the cluster for
 * any pending messages before being made available.
 * <p>
 * A change to the database can only complete when a majority of the machines
 * in the cluster are available. If less than a majority are available, update
 * operations will fail however current data can still be read.
 * <p>
 * Any machine can be queried for the current state of a data item, however,
 * there may be a brief moment where a data item is in transition between
 * two states and all the machines do not agree.
 *
 * @author Tobias Downer
 */

public class ReplicatedValueStore {

  /**
   * This service address.
   */
  private final ServiceAddress this_service;

  /**
   * The network connector.
   */
  private final NetworkConnector network;

  /**
   * The local database object.
   */
  private final KeyObjectDatabase block_database;

  /**
   * The lock object when writing to the database.
   */
  private final Object block_db_write_lock;

  /**
   * The service status tracker.
   */
  private final ServiceStatusTracker tracker;

  /**
   * The timer.
   */
  private final Timer timer;

  /**
   * The message communicator for this store.
   */
  private final MessageCommunicator comm;

  /**
   * The set of servers in the cluster.
   */
  private final ArrayList<ServiceAddress> cluster;

  /**
   * A secure random.
   */
  private final static SecureRandom RANDOM = new SecureRandom();

  /**
   * The last completed uid proposal.
   */
  private volatile long[] last_completed_uid = null;


  /**
   * The connected flag.
   */
  private volatile boolean connected = false;

  /**
   * Set to true when the initialization procedure is called.
   */
  private boolean init_val_boolean = false;

  /**
   * A lock used for initialization.
   */
  private final Object init_val_lock = new Object();



  // ---------- Statics ----------

  /**
   * The logger.
   */
  private static final Logger log = Logger.getLogger("com.mckoi.network.Log");

  /**
   * The com.mckoi.data.Key object for the UIDList.
   * (128-bit uid (sorted))
   */
  private static Key UID_LIST_KEY = new Key((short) 12, 0, 40);

  /**
   * The com.mckoi.data.Key object for the block id map.
   * (128-bit block_id -> 128-bit uid (sorted on block_id))
   */
  private static Key BLOCKID_UID_MAP_KEY = new Key((short) 12, 0, 50);

  /**
   * The com.mckoi.data.Key object for the key to uid map.
   * (PropertySet (key -> stringified 128-bit uid))
   */
  private static Key KEY_UID_MAP_KEY = new Key((short) 12, 0, 60);

  /**
   * Constructor.
   */
  ReplicatedValueStore(ServiceAddress this_service,
                       NetworkConnector network,
                       KeyObjectDatabase local_db, Object db_write_lock,
                       ServiceStatusTracker tracker, Timer timer) {

    this.this_service = this_service;
    this.network = network;
    this.block_database = local_db;
    this.block_db_write_lock = db_write_lock;
    this.tracker = tracker;
    this.timer = timer;
    this.comm = new MessageCommunicator(network, tracker, timer);

    this.cluster = new ArrayList(9);
    clearAllMachines();

    // Add listener for service status updates,
    tracker.addListener(new ServiceStatusListener() {
      @Override
      public void statusChange(ServiceAddress address, String service_type,
                               String old_status, String new_status) {
        if (service_type.equals("manager")) {
          // If we detected that a manager is now available,
          if (new_status.startsWith("UP")) {
            // If this is not connected, initialize,
            if (!connected) {
              initialize();
            }
            else {
              // Retry any pending messages on this service,
              comm.retryMessagesFor(address);
            }
          }
          else if (new_status.startsWith("DOWN")) {
            // If it's connected,
            if (connected) {
              int cluster_size;
              synchronized (cluster) {
                cluster_size = cluster.size();
              }
              // Set connected to false if availability check fails,
              checkClusterAvailability(cluster_size);
            }
          }
          // If a manager server goes down,
        }
      }
    });

//    TimerTask task = new TimerTask() {
//      @Override
//      public void run() {
//        synchronized (System.out) {
//          System.out.println("Machine: " +
//                      ReplicatedValueStore.this.this_service.displayString());
//          PrintWriter pout = new PrintWriter(System.out);
//          debugOutput(pout);
//          pout.flush();
//        }
//      }
//    };
//    timer.scheduleAtFixedRate(task, 2000, 20000);

  }


  private final Object init_lock = new Object();

  /**
   * Initializes the state of this store. This contacts all available servers
   * and asks to replay any pending messages on this manager. After the
   * messages have been replayed, we compare checksums of the stores. If they
   * don't compare we go through a discovery process to synchronize the data.
   * <p>
   * When initialization is finished, 'connected' is set to true.
   * <p>
   * This method returns immediately, the initialization is scheduled on the
   * timer thread.
   */
  void initialize() {
    synchronized (init_val_lock) {
      init_val_boolean = true;
    }

    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        doInitialize();
      }
    }, 500);
  }

  /**
   * The initialization procedure.
   */
  private void doInitialize() {
    try {
      synchronized (init_lock) {

        // If already connected, return
        if (connected) {
          return;
        }

        ArrayList<ServiceAddress> machines = new ArrayList(17);
        synchronized (cluster) {
          machines.addAll(cluster);
        }

//        // Poll all the machines,
//        pollAllMachines(machines);

        ArrayList<ServiceAddress> synchronized_on = new ArrayList(17);

        // For each machine in the cluster,
        for (ServiceAddress machine : machines) {
          if (!machine.equals(this_service)) {

            // Request all messages from the machines log from the given last
            // update time.

            LogEntryIterator messages =
                                   requestMessagesFrom(machine, getLatestUID());

            if (messages != null) {
              log.log(Level.FINE,
                      "Synchronizing with {0}", machine.displayString());

              try {
                long synchronize_count = playbackMessages(messages);

                log.log(Level.FINE,
                      "{0} synching with {1} complete, message count = {2}",
                            new Object[] { this_service.displayString(),
                                           machine.displayString(),
                                           synchronize_count });

                // Add the machine to the synchronized_on list
                synchronized_on.add(machine);

              }
              catch (RuntimeException e) {
                // If we failed to sync, report the manager down
                log.log(Level.FINE, "Failed synchronizing with {0}",
                                    machine.displayString());
                log.log(Level.FINE, "Sync Fail Error", e);
                tracker.reportServiceDownClientReport(machine, "manager");
              }
            }
          }
        }

        // If we synchronized on a majority of the machines, set the connected
        // flag to true,
        if ((synchronized_on.size() + 1) > machines.size() / 2) {
          log.log(Level.FINE, "  **** Setting connected to true {0}",
                  this_service.displayString());
          connected = true;
          return;
        }

      }

    }
    finally {
      synchronized (init_val_lock) {
        init_val_boolean = false;
        init_val_lock.notifyAll();
      }
    }

  }

  void waitUntilInitializeComplete() {
    try {
      synchronized (init_val_lock) {
        while (init_val_boolean == true) {
          init_val_lock.wait();
        }
      }
    }
    catch (InterruptedException e) {
      log.log(Level.SEVERE, "InterruptedException", e);
      throw new Error("Interrupted", e);
    }
  }

  /**
   * Clears the machine list.
   */
  final void clearAllMachines() {
    synchronized (cluster) {
      cluster.clear();
      cluster.add(this_service);
      connected = false;
      log.log(Level.FINE, "  **** Setting connected to false {0}",
                          this_service.displayString());
    }
  }

  /**
   * Check availability.
   */
  private void checkClusterAvailability(int size) {
    synchronized (cluster) {
      // For all the machines in the cluster,
      int available_count = 0;
      for (ServiceAddress m : cluster) {
        if (tracker.isServiceUp(m, "manager")) {
          ++available_count;
        }
      }
      if (available_count <= size / 2) {
        connected = false;
        log.log(Level.FINE, "  **** Setting connected to false {0}",
                            this_service.displayString());
      }
    }
  }

  /**
   * Adds a machine to the cluster.
   */
  void addMachine(ServiceAddress addr) {
    synchronized (cluster) {
      int orig_size = cluster.size();

      if (cluster.contains(addr)) {
        throw new RuntimeException("Machine already in cluster");
      }
      cluster.add(addr);

      checkClusterAvailability(orig_size);
    }
  }

  /**
   * Removes a machine from the cluster.
   */
  void removeMachine(ServiceAddress addr) {
    boolean removed;
    synchronized (cluster) {
      removed = cluster.remove(addr);
      checkClusterAvailability(cluster.size());
    }
    if (!removed) {
      throw new RuntimeException("Machine not found in cluster");
    }
  }

  /**
   * Returns true if the error message is a connection failure message.
   */
  public static boolean isConnectionFault(Message m) {
    ExternalThrowable et = m.getExternalThrowable();
    // If it's a connect exception,
    String ex_type = et.getClassName();
    if (ex_type.equals("java.net.ConnectException")) {
      return true;
    }
    else if (ex_type.equals("com.mckoi.network.ServiceNotConnectedException")) {
      return true;
    }
    return false;
  }

  /**
   * Returns true if a majority of machines in the cluster are connected. This
   * checks the status of a service via the service tracker object.
   */
  private boolean isMajorityConnected() {
    // Check a majority of servers in the cluster are up.
    ArrayList<ServiceAddress> machines = new ArrayList(17);
    synchronized (cluster) {
      machines.addAll(cluster);
    }
    int connect_count = 0;
    // For each machine
    for (ServiceAddress machine : machines) {
      // Check it's up
      if (tracker.isServiceUp(machine, "manager")) {
        ++connect_count;
      }
    }
    return connect_count > machines.size() / 2;
  }

  /**
   * Checks that this store is currently connected. If not, throws an
   * exception.
   */
  void checkConnected() {
    // If less than a majority of servers are connected then throw an
    // exception,
    boolean m_connect;
    if (!connected || !isMajorityConnected()) {
      throw new ServiceNotConnectedException(
              "Manager service " + this_service.displayString() +
              " is not connected (majority = " + isMajorityConnected() + ")");
    }
  }

  /**
   * Contacts the given machine and requests an iterator over all log entries
   * that have happened since the given latest uid. Note that this may return
   * more messages than requested - duplicates should be detected and dropped
   * by the callee. If a null 'latest_uid' is given, all completed messages
   * are returned.
   * <p>
   * The returned iterator does not support 'remove'.
   */
  private LogEntryIterator requestMessagesFrom(
                                  ServiceAddress machine, long[] latest_uid) {

    // Return the iterator,
    return new LogEntryIterator(machine, latest_uid);

//    // Open up a log entry stream with the machine,
//    // Create the message,
//    MessageStream msg_out = new MessageStream(16);
//    msg_out.addMessage("internalOpenLogStream");
//    msg_out.addLongArray(latest_uid);
//    msg_out.closeMessage();
//
//    // The id
//    long request_stream_id = -1;
//
//    // Send the open stream command.
//    MessageStream msg_in;
//    // If the service is up,
//    if (tracker.isServiceUp(machine, "manager")) {
//      // Send to the service,
//      MessageProcessor processor = network.connectManagerServer(machine);
//      msg_in = processor.process(msg_out);
//
//      // If it's a connection error, return null,
//      Iterator<Message> msgs = msg_in.iterator();
//      while (msgs.hasNext()) {
//        Message m = msgs.next();
//        if (m.isError()) {
//          if (!isConnectionFault(m)) {
//            throw new RuntimeException(m.getErrorMessage());
//          }
//        }
//        else {
//          request_stream_id = (Long) m.param(0);
//        }
//      }
//    }
//
//    // If no request stream id found,
//    if (request_stream_id == -1) {
//      return null;
//    }
//
  }

  /**
   * Plays back all the log entries in the iterator.
   */
  private long playbackMessages(LogEntryIterator messages) {

    long count = 0;
    ArrayList<LogEntry> bundle = new ArrayList(32);

    while (true) {

      boolean done = false;
      bundle.clear();
      for (int i = 0; i < 32 && !done; ++i) {
        LogEntry entry = messages.nextLogEntry();
        if (entry == null) {
          done = true;
        }
        else {
          bundle.add(entry);
        }
      }

      // Finished,
      if (bundle.isEmpty() && done) {
        break;
      }

      synchronized(block_db_write_lock) {
        KeyObjectTransaction transaction = block_database.createTransaction();
        try {

          for (LogEntry entry : bundle) {

            // Deserialize the entry,

            long[] uid = entry.getUID();
            byte[] buf = entry.getBuf();

//            System.out.println("###### Applying: " + toUIDString(uid));

            // If this hasn't applied the uid then we apply it,
            if (!hasAppliedUID(transaction, uid)) {

              ByteArrayInputStream bin = new ByteArrayInputStream(buf);
              DataInputStream din = new DataInputStream(bin);

              byte m = din.readByte();
              if (m == 18) {
                // Block_id to server map
                long block_id_h = din.readLong();
                long block_id_l = din.readLong();
                BlockId block_id = new BlockId(block_id_h, block_id_l);
                int sz = din.readInt();
                long[] servers = new long[sz];
                for (int i = 0; i < sz; ++i) {
                  servers[i] = din.readLong();
                }

                // Replay this command,
                insertBlockIdServerEntry(transaction, uid, block_id, servers);

              }
              else if (m == 19) {
                // Key/Value pair
                String key = din.readUTF();
                String value = null;
                byte vb = din.readByte();
                if (vb == 1) {
                  value = din.readUTF();
                }

                // Replay this command,
                insertKeyValueEntry(transaction, uid, key, value);

              }
              else {
                throw new RuntimeException("Unknown entry type: " + m);
              }

              // Increment the count,
              ++count;

            }

          }  // For each entry in the bundle

          // Commit and check point the update,
          block_database.publish(transaction);
          block_database.checkPoint();

        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
        finally {
          block_database.dispose(transaction);
        }

      }  // synchronized

    }  // while true

    return count;

  }

  /**
   * Sends a command to all the machines in the cluster. Returns the number
   * of successful machine contacted. Unsuccessful messages are added to the
   * pending queue.
   */
  private int sendCommand(ArrayList<ServiceMessageQueue> pending_queue,
                          ArrayList<ServiceAddress> machines,
                          MessageStream msg_out) {

    // Send the messages,
    ProcessResult[] in = new MessageStream[machines.size()];

    // For each machine in the cluster, send the process commands,
    for (int i = 0; i < machines.size(); ++i) {
      ServiceAddress machine = machines.get(i);
      ProcessResult msg_in = null;
      // If the service is up,
      if (tracker.isServiceUp(machine, "manager")) {
        // Send to the service,
        MessageProcessor processor = network.connectManagerServer(machine);
        msg_in = processor.process(msg_out);
      }
      in[i] = msg_in;
    }

    // Now read in the results.

    int send_count = 0;

    int i = 0;
    while (i < in.length) {
      boolean msg_sent = true;
      // Null indicates not sent,
      if (in[i] == null) {
        msg_sent = false;
      }
      else {
        Iterator<Message> msgs = in[i].iterator();
        while (msgs.hasNext()) {
          Message m = msgs.next();
          if (m.isError()) {
            // If it's not a comm fault, we throw the error now,
            if (!isConnectionFault(m)) {
              throw new RuntimeException(m.getErrorMessage());
            }
            // Inform the tracker of the fault,
            tracker.reportServiceDownClientReport(machines.get(i), "manager");
            msg_sent = false;
          }
        }
      }

      // If not sent, queue the message,
      if (!msg_sent) {
        // The machine that is not available,
        ServiceAddress machine = machines.get(i);

        // Get the queue for the machine,
        ServiceMessageQueue queue = comm.createServiceMessageQueue();
        queue.addMessageStream(machine, msg_out, "manager");

        // Add this queue to the pending queue list
        pending_queue.add(queue);
      }
      else {
        // Otherwise we sent the message with no error,
        ++send_count;
      }

      ++i;
    }

    return send_count;
  }

  /**
   * Sends the given proposal to all the machines in the cluster. If a machine
   * is not available, the message to that machine is put in a queue, which is
   * enqueued if the proposal is accepted by a majority.
   */
  private int sendProposalToNetwork(
          ArrayList<ServiceMessageQueue> pending_queue,
          long[] uid, String key, String value) {

    ArrayList<ServiceAddress> machines = new ArrayList(17);
    synchronized (cluster) {
      machines.addAll(cluster);
    }

    // Create the message,
    MessageStream msg_out = new MessageStream(16);
    msg_out.addMessage("internalKVProposal");
    msg_out.addLongArray(uid);
    msg_out.addString(key);
    msg_out.addString(value);
    msg_out.closeMessage();

    // Send the proposal command out to the machines on the network,
    int send_count = sendCommand(pending_queue, machines, msg_out);

    // If we sent to a majority, return 1
    if (send_count > machines.size() / 2) {
      return 1;
    }
    // Otherwise return 2, (majority of machines in the cluster not available).
    else {
      return 2;
    }

  }

  /**
   * Send the proposal complete message out to the cluster. In addition to
   * sending out the proposal complete operations, enqueues any pending
   * messages.
   */
  private void sendProposalComplete(
          ArrayList<ServiceMessageQueue> pending_queue,
          long[] uid, String key, String value) {

    ArrayList<ServiceAddress> machines = new ArrayList(17);
    synchronized (cluster) {
      machines.addAll(cluster);
    }

    // Create the message,
    MessageStream msg_out = new MessageStream(16);
    msg_out.addMessage("internalKVComplete");
    msg_out.addLongArray(uid);
    msg_out.addString(key);
    msg_out.addString(value);
    msg_out.closeMessage();

    // Send the complete proposal message out to the machines on the network,
    sendCommand(pending_queue, machines, msg_out);

    // Enqueue all pending messages,
    for (ServiceMessageQueue queue : pending_queue) {
      queue.enqueue();
    }

  }

  /**
   * Sends the given proposal to all the machines in the cluster. If a machine
   * is not available, the message to that machine is put in a queue, which is
   * enqueued if the proposal is accepted by a majority.
   */
  private int sendProposalToNetwork(
          ArrayList<ServiceMessageQueue> pending_queue,
          long[] uid, BlockId block_id, long[] block_server_uids) {

    ArrayList<ServiceAddress> machines = new ArrayList(17);
    synchronized (cluster) {
      machines.addAll(cluster);
    }

    // Create the message,
    MessageStream msg_out = new MessageStream(16);
    msg_out.addMessage("internalBSProposal");
    msg_out.addLongArray(uid);
    msg_out.addBlockId(block_id);
    msg_out.addLongArray(block_server_uids);
    msg_out.closeMessage();

    // Send the proposal command out to the machines on the network,
    int send_count = sendCommand(pending_queue, machines, msg_out);

    // If we sent to a majority, return 1
    if (send_count > machines.size() / 2) {
      return 1;
    }
    // Otherwise return 2, (majority of machines in the cluster not available).
    else {
      return 2;
    }

  }

  /**
   * Send the proposal complete message out to the cluster. In addition to
   * sending out the proposal complete operations, enqueues any pending
   * messages.
   */
  private void sendProposalComplete(
          ArrayList<ServiceMessageQueue> pending_queue,
          long[] uid, BlockId block_id, long[] block_server_uids) {

    ArrayList<ServiceAddress> machines = new ArrayList(17);
    synchronized (cluster) {
      machines.addAll(cluster);
    }

    // Create the message,
    MessageStream msg_out = new MessageStream(16);
    msg_out.addMessage("internalBSComplete");
    msg_out.addLongArray(uid);
    msg_out.addBlockId(block_id);
    msg_out.addLongArray(block_server_uids);
    msg_out.closeMessage();

    // Send the complete proposal message out to the machines on the network,
    sendCommand(pending_queue, machines, msg_out);

    // Enqueue all pending messages,
    for (ServiceMessageQueue queue : pending_queue) {
      queue.enqueue();
    }

  }

  /**
   * Returns a human readable UID string from the 128-bit value.
   */
  private static String toUIDString(long uid_high, long uid_low) {
    String tms = Long.toString(uid_high, 32);
    StringBuilder b = new StringBuilder();
    b.append(tms);
    b.append("-");
    b.append(Long.toString(uid_low, 32));
    return b.toString();
  }

  private static String toUIDString(long[] uid) {
    return toUIDString(uid[0], uid[1]);
  }

  private static long[] parseUIDString(String str) {
    int delim = str.indexOf("-");
    if (delim == -1) {
      throw new RuntimeException("Format error");
    }
    long hv = Long.parseLong(str.substring(0, delim), 32);
    long lv = Long.parseLong(str.substring(delim + 1), 32);
    return new long[] { hv, lv };
  }

  /**
   * Generates a unique id. The id string has a timestamp component and a
   * random component, separated by a deliminator character.
   */
  private static long[] generateUID() {
    long time_ms = System.currentTimeMillis();
    long rv = RANDOM.nextLong();
    if (rv < 0) {
      rv = -rv;
    }

    return new long[] { time_ms, rv };
  }

  /**
   * RPC from a client in the cluster for proposal a server_uids to be made to the
   * database.
   */
  private void internalKVProposal(long[] uid, String key, String value) {

    // Check this store is connected. Throws an exception if not.
    checkConnected();

  }

  /**
   * RPC for a proposal complete notification. This makes the block_id/server_uids change
   * in the database.
   */
  private void internalKVComplete(long[] uid, String key, String value) {

    // Check this store is connected. Throws an exception if not.
    checkConnected();

    // Perform this under a lock. This lock is also active for block queries
    // and administration updates.
    synchronized (block_db_write_lock) {
      // Create a transaction
      KeyObjectTransaction transaction = block_database.createTransaction();
      try {
        // We must handle the case when multiple identical proposals come in,
        if (!hasAppliedUID(transaction, uid)) {
          // Add the serialization to the transaction log,
          insertKeyValueEntry(transaction, uid, key, value);

          // Commit and check point the update,
          block_database.publish(transaction);
          block_database.checkPoint();
        }
      }
      finally {
        block_database.dispose(transaction);
      }
    }


  }

  /**
   * RPC from a client in the cluster for proposal a server_uids to be made to the
   * database.
   */
  private void internalBSProposal(long[] uid,
                                  BlockId block_id, long[] server_uids) {

    // Check this store is connected. Throws an exception if not.
    checkConnected();

  }

  /**
   * RPC for a proposal complete notification. This makes the block_id/server_uids change
   * in the database.
   */
  private void internalBSComplete(long[] uid,
                                  BlockId block_id, long[] server_uids) {


    // Check this store is connected. Throws an exception if not.
    checkConnected();

    // Perform this under a lock. This lock is also active for block queries
    // and administration updates.
    synchronized (block_db_write_lock) {
      // Create a transaction
      KeyObjectTransaction transaction = block_database.createTransaction();
      try {
        // We must handle the case when multiple identical proposals come in,
        if (!hasAppliedUID(transaction, uid)) {
          // Insert the block id server mapping,
          insertBlockIdServerEntry(transaction, uid, block_id, server_uids);

          // Commit and check point the update,
          block_database.publish(transaction);
          block_database.checkPoint();
        }
      }
      finally {
        block_database.dispose(transaction);
      }
    }

  }

  /**
   * Records a block_id/server_uids pair in the database. This will broadcast the
   * assignment to the entire cluster of machines in the network. If less than
   * a majority of the machines in the cluster are currently available, the
   * operation fails.
   */
  void setValue(String key, String value) {

    // If the given value is the same as the current value stored, return.
    String in_value = getValue(key);
    if ((in_value == null && value == null) ||
        (in_value != null && value != null && in_value.equals(value))
       ) {
      return;
    }

//    System.out.println("##### setValue(" + key + ", " + value + ")");

    // Sets a server_uids in the network. This performs the following operations;
    //
    // 1) Sends an 'internalProposeValue(uid, block_id, server_uids)' server_uids to all the
    //    currently available machines in the cluster.
    // 2) When a majority of machines have accepted the proposal, sends a
    //    complete message out to the network.

    // Each machine in the network does the following;
    // 1) When an 'internalProposeValue' command is received, puts the message
    //    in a queue.
    // 2) When a complete message is received, the message is written to the
    //    database.

    // Generate a UID string,
    long[] uid = generateUID();

    // Pending messages from connection faults,
    ArrayList<ServiceMessageQueue> pending_queue = new ArrayList(7);

    // Send the proposal out to the network,
    int status = sendProposalToNetwork(pending_queue, uid, key, value);

    // If a majority of machines accepted the proposal, send the complete
    // operation.
    if (status == 1) {
      sendProposalComplete(pending_queue, uid, key, value);
    }
    else {
      // Otherwise generate the exception,
      throw new RuntimeException("A majority of the cluster is not available");
    }

  }

  /**
   * Returns the currently assigned server_uids for the given block_id. This queries the
   * local database only. The returned server_uids is not guarenteed to be
   * consistent with all the other values in the cluster, however a lot of
   * effort goes to ensure that the server_uids is current (you should never be able
   * to receive a vastly out of date server_uids).
   * <p>
   * If this replicated server_uids store is not 'connected' (it has not recently
   * successfully communicated with a majority of machines in the cluster), an
   * exception will be generated.
   */
  String getValue(String key) {

    // Check this store is connected. Throws an exception if not.
    checkConnected();

    // Perform this under a lock. This lock is also active for block queries
    // and administration updates.
    synchronized (block_db_write_lock) {

      // Create a transaction
      KeyObjectTransaction transaction = block_database.createTransaction();

      try {

        long[] uid = getUIDForKey(transaction, key);
        if (uid == null) {
          // Return null if not found,
          return null;
        }
        byte[] buf = getValueFromUID(transaction, uid);
        if (buf == null) {
          return null;
        }

        ByteArrayInputStream bin = new ByteArrayInputStream(buf);
        DataInputStream din = new DataInputStream(bin);

        byte m = din.readByte();
        String in_key = din.readUTF();
        String in_value = null;
        byte vb = din.readByte();
        if (vb == 1) {
          in_value = din.readUTF();
        }

        if (m != 19) {
          throw new RuntimeException("Unexpected value marker");
        }
        if (!in_key.equals(key)) {
          throw new RuntimeException("Keys don't match");
        }

        return in_value;
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
      finally {
        block_database.dispose(transaction);
      }
    }

  }

  /**
   * Records a BlockId to server id set of maps in the database. The mapping
   * is replicated over all the machines in the cluster. If less than
   * a majority of the machines in the cluster are currently available, the
   * operation fails.
   */
  void setBlockIdServerMap(BlockId block_id,
                           long[] block_server_uids) {

    // Generate a UID string,
    long[] uid = generateUID();

    // Pending messages from connection faults,
    ArrayList<ServiceMessageQueue> pending_queue = new ArrayList(7);

    // Send the proposal out to the network,
    int status = sendProposalToNetwork(pending_queue, uid,
                                       block_id, block_server_uids);

    // If a majority of machines accepted the proposal, send the complete
    // operation.
    if (status == 1) {
      sendProposalComplete(pending_queue, uid, block_id, block_server_uids);
    }
    else {
      // Otherwise generate the exception,
      throw new RuntimeException("A majority of the cluster is not available");
    }

  }

  /**
   * Returns the list of servers recorded in the database that are managing
   * the given block id. This queries the local database only. The returned
   * server_uids is not guaranteed to be consistent with all the other values in the
   * cluster, however a lot of effort goes to ensure that the server_uids is current
   * (you should never be able to receive a vastly out of date server_uids).
   * <p>
   * If this replicated server_uids store is not 'connected' (it has not recently
   * successfully communicated with a majority of machines in the cluster), an
   * exception will be generated.
   * <p>
   * Returns empty array if the block id is associated with no servers.
   */
  long[] getBlockIdServerMap(BlockId block_id) {

    // Check this store is connected. Throws an exception if not.
    checkConnected();

    // Perform this under a lock. This lock is also active for block queries
    // and administration updates.
    synchronized (block_db_write_lock) {

      // Create a transaction
      KeyObjectTransaction transaction = block_database.createTransaction();

      try {

        long[] uid = getUIDForBlock(transaction, block_id);
        if (uid == null) {
          return new long[0];
        }
        byte[] buf = getValueFromUID(transaction, uid);

        ByteArrayInputStream bin = new ByteArrayInputStream(buf);
        DataInputStream din = new DataInputStream(bin);

        // Deserialize the value,
        byte m = din.readByte();
        long block_id_h = din.readLong();
        long block_id_l = din.readLong();
        BlockId val_block_id = new BlockId(block_id_h, block_id_l);
        int sz = din.readInt();
        long[] val_servers = new long[sz];
        for (int i = 0; i < sz; ++i) {
          val_servers[i] = din.readLong();
        }

        // Sanity checks,
        if (m != 18) {
          throw new RuntimeException("Unexpected value marker");
        }
        if (!val_block_id.equals(block_id)) {
          throw new RuntimeException("Block IDs don't match");
        }

        return val_servers;

      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
      finally {
        block_database.dispose(transaction);
      }
    }

  }

  /**
   * Returns the last block id stored in this manager.
   */
  BlockId getLastBlockId() {

    // Check this store is connected. Throws an exception if not.
    checkConnected();

    // Perform this under a lock. This lock is also active for block queries
    // and administration updates.
    synchronized (block_db_write_lock) {

      // Create a transaction
      KeyObjectTransaction transaction = block_database.createTransaction();

      try {

        // Return the last block id
        return getLastBlockId(transaction);

      }
      finally {
        block_database.dispose(transaction);
      }
    }

  }

  /**
   * Returns an iterator over all key values in the key/value map set, with
   * the given prefix.
   */
  String[] getAllKeys(String prefix) {

    // Check this store is connected. Throws an exception if not.
    checkConnected();

    synchronized (block_db_write_lock) {
      KeyObjectTransaction t = block_database.createTransaction();

      try {
        DataFile key_list_df = t.getDataFile(KEY_UID_MAP_KEY, 'w');
        PropertySet key_list = new PropertySet(key_list_df);
        SortedSet<String> key_set = key_list.keySet();

        if (prefix.length() > 0) {
          // Reduction,
          String first_item = prefix;
          String last_item = prefix.substring(0, prefix.length() - 1) +
                                     (prefix.charAt(prefix.length() - 1) + 1);

          key_set = key_set.subSet(first_item, last_item);
        }

        // Make the array and return
        ArrayList<String> str_keys_list = new ArrayList(64);
        Iterator<String> i = key_set.iterator();
        while (i.hasNext()) {
          str_keys_list.add(i.next());
        }

        return str_keys_list.toArray(new String[str_keys_list.size()]);

      }
      finally {
        block_database.dispose(t);
      }
    }







  }

  /**
   * Returns the last UID that was stored here.
   */
  private long[] getLatestUID() {
    // Perform this under a lock. This lock is also active for block queries
    // and administration updates.
    synchronized (block_db_write_lock) {
      // Create a transaction
      KeyObjectTransaction transaction = block_database.createTransaction();

      try {

        // Create the UIDList object,
        DataFile uid_list_df = transaction.getDataFile(UID_LIST_KEY, 'w');
        UIDList uid_list = new UIDList(uid_list_df);

        return uid_list.getLastUID();

      }
      finally {
        block_database.dispose(transaction);
      }
    }
  }

  /**
   * Fetches a bundle of uids from the log bundle.
   */
  private void internalFetchLogBundle(MessageStream reply_message,
                                      long[] uid, boolean initial) {

//    synchronized (System.out) {
//      System.out.println("internalFetchLogBundle");
//      System.out.println(this_service.displayString());
//      if (uid != null) {
//        System.out.println(toUIDString(uid[0], uid[1]));
//      }
//      else {
//        System.out.println("UID: null");
//      }
//    }

    // Perform this under a lock. This lock is also active for block queries
    // and administration updates.
    synchronized (block_db_write_lock) {

//      System.out.println("1 " + this_service.displayString());

      // Create a transaction
      KeyObjectTransaction transaction = block_database.createTransaction();

      try {
        // Create the UIDList object,
        DataFile uid_list_df = transaction.getDataFile(UID_LIST_KEY, 'w');
        UIDList uid_list = new UIDList(uid_list_df);

        // Go to the position of the uid,
        long pos = 0;
        if (uid != null) {
          pos = uid_list.positionOfUID(uid);
        }
        long end = Math.min(pos + 32, uid_list.size());
        if (pos < 0) {
          pos = -(pos + 1);
        }

        // If initial is true, we go back a bit
        if (initial) {
          // Go back 16 entries in the log (but don't go back before the first)
          pos = Math.max(0, (pos - 16));
        }
        else {
          // Go to the next entry,
          pos = pos + 1;
        }

        // Send the bundle out to the stream,
        for (long i = pos; i < end; ++i) {
          long[] in_uid = uid_list.getUIDAt(i);
          byte[] buf = getValueFromUID(transaction, in_uid);

          reply_message.addMessage("R");
          reply_message.addLongArray(in_uid);
          reply_message.addBuf(buf);
          reply_message.closeMessage();
        }

      }
      finally {
        block_database.dispose(transaction);
      }

    }

//    synchronized (System.out) {
//      System.out.println("END internalFetchLogBundle");
//      System.out.println(this_service.displayString());
//      if (uid != null) {
//        System.out.println(toUIDString(uid[0], uid[1]));
//      }
//      else {
//        System.out.println("UID: null");
//      }
//    }

  }



  /**
   * Processes an incoming RPC message. The reply is put on the
   * 'reply_message'.
   */
  public void process(Message m, MessageStream reply_message) {

    String cmd = m.getName();

    if (cmd.equals("internalKVProposal")) {
      long[] uid = (long[]) m.param(0);
      String key = (String) m.param(1);
      String value = (String) m.param(2);
      internalKVProposal(uid, key, value);
      reply_message.addMessage("R");
      reply_message.addInteger(1);
      reply_message.closeMessage();
    }
    else if (cmd.equals("internalKVComplete")) {
      long[] uid = (long[]) m.param(0);
      String key = (String) m.param(1);
      String value = (String) m.param(2);
      internalKVComplete(uid, key, value);
      reply_message.addMessage("R");
      reply_message.addInteger(1);
      reply_message.closeMessage();
    }
    else if (cmd.equals("internalBSProposal")) {
      long[] uid = (long[]) m.param(0);
      BlockId block_id = (BlockId) m.param(1);
      long[] server_uids = (long[]) m.param(2);
      internalBSProposal(uid, block_id, server_uids);
      reply_message.addMessage("R");
      reply_message.addInteger(1);
      reply_message.closeMessage();
    }
    else if (cmd.equals("internalBSComplete")) {
      long[] uid = (long[]) m.param(0);
      BlockId block_id = (BlockId) m.param(1);
      long[] server_uids = (long[]) m.param(2);
      internalBSComplete(uid, block_id, server_uids);
      reply_message.addMessage("R");
      reply_message.addInteger(1);
      reply_message.closeMessage();
    }

    else if (cmd.equals("internalFetchLogBundle")) {
      long[] uid = (long[]) m.param(0);
      boolean initial = ((Integer) m.param(1)) != 0;
      internalFetchLogBundle(reply_message, uid, initial);
    }

    else if (cmd.equals("debugString")) {
      StringWriter str_out = new StringWriter();
      PrintWriter pout = new PrintWriter(str_out);
      debugOutput(pout);
      pout.flush();
      reply_message.addMessage("R");
      reply_message.addString(str_out.toString());
      reply_message.closeMessage();
    }

    else {
      throw new RuntimeException("Unknown command: " + m.getName());
    }

  }





  // ------ Storage ------

  /**
   * Outputs the content of the db for diagnostic purposes.
   */
  public void debugOutput(PrintWriter out) {
    // Perform this under a lock. This lock is also active for block queries
    // and administration updates.
    synchronized (block_db_write_lock) {
      // Create a transaction
      KeyObjectTransaction t = block_database.createTransaction();

      try {

        DataFile bid_uid_list_df = t.getDataFile(BLOCKID_UID_MAP_KEY, 'w');
        BlockIdUIDList blockid_uid_list = new BlockIdUIDList(bid_uid_list_df);
        DataFile key_list_df = t.getDataFile(KEY_UID_MAP_KEY, 'w');
        PropertySet key_list = new PropertySet(key_list_df);
        DataFile uid_list_df = t.getDataFile(UID_LIST_KEY, 'w');
        UIDList uid_list = new UIDList(uid_list_df);

        long size = blockid_uid_list.size();
        out.println("BlockID -> UID");
        out.println();
        for (long i = 0; i < size; ++i) {
          out.print("  ");
          BlockId block_id = blockid_uid_list.getBlockIdAt(i);
          long[] uid = blockid_uid_list.getUIDAt(i);
          out.print(block_id);
          out.print(" -> ");
          out.println(toUIDString(uid[0], uid[1]));
        }
        out.println();

        out.println("Key(String) -> UID");
        out.println();
        SortedSet<String> keys = key_list.keySet();
        for (String key : keys) {
          out.print("  ");
          String uid_string = key_list.getProperty(key);
          out.print(key);
          out.print(" -> ");
          out.println(uid_string);
        }
        out.println();

        size = uid_list.size();
        out.println("UID");
        out.println();
        for (long i = 0; i < size; ++i) {
          out.print("  ");
          long[] uid = uid_list.getUIDAt(i);
          out.println(toUIDString(uid[0], uid[1]));
        }
        out.println();

      }
      finally {
        block_database.dispose(t);
      }
    }
  }

  /**
   * Returns the UID from the stored block id.
   */
  private long[] getUIDForBlock(KeyObjectTransaction t, BlockId block_id) {
    DataFile bid_uid_list_df = t.getDataFile(BLOCKID_UID_MAP_KEY, 'w');
    BlockIdUIDList blockid_uid_list = new BlockIdUIDList(bid_uid_list_df);

    long pos = blockid_uid_list.positionOfBlockId(block_id);
    if (pos < 0) {
      return null;
    }
    else {
      return blockid_uid_list.getUIDAt(pos);
    }
  }

  /**
   * Returns the UID from the stored key value.
   */
  private long[] getUIDForKey(KeyObjectTransaction t, String key) {
    DataFile key_list_df = t.getDataFile(KEY_UID_MAP_KEY, 'w');
    PropertySet key_list = new PropertySet(key_list_df);

    String val = key_list.getProperty(key);
    if (val == null) {
      return null;
    }
    return parseUIDString(val);
  }

  /**
   * Returns true if the uid has been applied to this value store.
   */
  private boolean hasAppliedUID(KeyObjectTransaction t, long[] uid) {
    // Make a hash value,
    long hash_code = uid[0] / 16;

    // Turn it into a key object,
    Key hash_key = new Key((short) 13, 0, hash_code);

    // The DataFile
    DataFile dfile = t.getDataFile(hash_key, 'w');

    long pos = 0;
    long size = dfile.size();
    while (pos < size) {
      dfile.position(pos);
      int sz = dfile.getInt();

      // Get the stored uid,
      long inuid_h = dfile.getLong();
      long inuid_l = dfile.getLong();

      // If the uid matches,
      if (inuid_h == uid[0] && inuid_l == uid[1]) {
        // Match, so return true
        return true;
      }

      pos = pos + sz;
    }

    // Not found, return false
    return false;
  }

  /**
   * Returns the value serialization of the given uid.
   */
  private byte[] getValueFromUID(KeyObjectTransaction t, long[] uid) {

    // Make a hash value,
    long hash_code = uid[0] / 16;

    // Turn it into a key object,
    Key hash_key = new Key((short) 13, 0, hash_code);

    // The DataFile
    DataFile dfile = t.getDataFile(hash_key, 'w');

    long pos = 0;
    long size = dfile.size();
    while (pos < size) {
      dfile.position(pos);
      int sz = dfile.getInt();

      // Get the stored uid,
      long inuid_h = dfile.getLong();
      long inuid_l = dfile.getLong();

      // If the uid matches,
      if (inuid_h == uid[0] && inuid_l == uid[1]) {
        // Match, so put the serialization of the record in the file,
        int buf_sz = sz - 20;
        byte[] buf = new byte[buf_sz];
        dfile.get(buf, 0, buf_sz);
        // If buf is empty return null,
        if (buf.length == 0) {
          return null;
        }
        return buf;
      }

      pos = pos + sz;
    }

    // Not found, return null
    return null;
  }

  /**
   * Returns the last BlockId stored in the manager cluster. Returns null if
   * no block id's stored.
   */
  private BlockId getLastBlockId(KeyObjectTransaction t) {

    DataFile bid_uid_list_df = t.getDataFile(BLOCKID_UID_MAP_KEY, 'r');
    BlockIdUIDList blockid_uid_list = new BlockIdUIDList(bid_uid_list_df);

    long pos = blockid_uid_list.size() - 1;
    if (pos < 0) {
      return null;
    }

    return blockid_uid_list.getBlockIdAt(pos);
  }

  /**
   * Inserts a log UID entry into sorted position in the list.
   */
  private void insertToUIDList(KeyObjectTransaction t, long[] uid) {
    // Create the UIDList object,
    DataFile uid_list_df = t.getDataFile(UID_LIST_KEY, 'w');
    UIDList uid_list = new UIDList(uid_list_df);

    // Inserts the uid in the list
    uid_list.addUID(uid);
  }

  /**
   * Inserts a value into the log uid entry. Values are sorted by uid key
   * which puts them in timestamp order.
   */
  private void insertValue(KeyObjectTransaction t, long[] uid, byte[] value) {

    // Make a hash value,
    long hash_code = uid[0] / 16;

    // Insert the uid into a list,
    insertToUIDList(t, uid);

    // Turn it into a key object,
    Key hash_key = new Key((short) 13, 0, hash_code);

    // The DataFile
    DataFile dfile = t.getDataFile(hash_key, 'w');
    // The size of the entry being added,
    int sz = 20 + value.length;

    // Position at the end of the file,
    dfile.position(dfile.size());
    // Insert the entry,
    // Put the size of the value entry,
    dfile.putInt(sz);
    // The 128-bit uid,
    dfile.putLong(uid[0]);
    dfile.putLong(uid[1]);
    // The value content,
    dfile.put(value, 0, value.length);

  }

  /**
   * Inserts a block_id to log uid ref, so we can determine the last entry for
   * the given block id.
   */
  private void insertBlockIdRef(KeyObjectTransaction t,
                                BlockId block_id, long[] uid) {

    DataFile bid_uid_list_df = t.getDataFile(BLOCKID_UID_MAP_KEY, 'w');
    BlockIdUIDList blockid_uid_list = new BlockIdUIDList(bid_uid_list_df);

    blockid_uid_list.addBlockIdRef(block_id, uid);
  }

  /**
   * Inserts a key to log uid ref, so we can determine the last value entry
   * for the given key.
   */
  private void insertKeyRef(KeyObjectTransaction t,
                            String key, String value, long[] uid) {

    DataFile key_list_df = t.getDataFile(KEY_UID_MAP_KEY, 'w');
    PropertySet key_list = new PropertySet(key_list_df);

    // Put it in the property set,
    if (value == null) {
      key_list.setProperty(key, null);
    }
    else {
      key_list.setProperty(key, toUIDString(uid[0], uid[1]));
    }
  }

  /**
   * Insert a blockid -> server_uids entry, and inserts an 128 bit log 'uid'
   * entry referencing the entry into the transaction log.
   */
  private void insertBlockIdServerEntry(KeyObjectTransaction t,
                               long[] uid, BlockId block_id, long[] servers) {

    byte[] buf;

    // Put this proposal in a local log,
    try {
      ByteArrayOutputStream bout = new ByteArrayOutputStream(64);
      DataOutputStream dout = new DataOutputStream(bout);
      dout.writeByte(18);
      dout.writeLong(block_id.getHighLong());
      dout.writeLong(block_id.getLowLong());
      dout.writeInt(servers.length);
      for (int i = 0; i < servers.length; ++i) {
        dout.writeLong(servers[i]);
      }

      buf = bout.toByteArray();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    // Inserts the value
    insertValue(t, uid, buf);
    // Inserts a reference
    insertBlockIdRef(t, block_id, uid);
  }

  /**
   * Insert a string key -> string value entry, and inserts an 128 bit log
   * 'uid' entry referencing the entry into the transaction log.
   */
  private void insertKeyValueEntry(KeyObjectTransaction t,
                                   long[] uid, String key, String value) {

    byte[] buf = null;

    // Put this proposal in a local log,
    try {
      ByteArrayOutputStream bout = new ByteArrayOutputStream(256);
      DataOutputStream dout = new DataOutputStream(bout);
      dout.writeByte(19);
      dout.writeUTF(key);
      if (value == null) {
        dout.writeByte(0);
      }
      else {
        dout.writeByte(1);
        dout.writeUTF(value);
      }

      buf = bout.toByteArray();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    // Inserts the value
    insertValue(t, uid, buf);
    // Inserts a reference
    insertKeyRef(t, key, value, uid);
  }


  // ----------

  /**
   * A sorted list of 128-Bit UID values.
   */
  private static class UIDList extends FixedSizeSerialSet {

    public UIDList(DataFile data) {
      super(data, 16);
    }

    /**
     * Returns the position of the UID in the list. Returns a negative value
     * if the uid was not found in the list.
     */
    public long positionOfUID(long[] uid) {
      Integer128Bit sv = new Integer128Bit(uid[0], uid[1]);
      return searchForRecord(sv);
    }

    /**
     * Returns the UID at the given position.
     */
    public long[] getUIDAt(long pos) {
      positionOn(pos);
      long high_v = getDataFile().getLong();
      long low_v = getDataFile().getLong();
      return new long[] { high_v, low_v };
    }

    /**
     * Returns the last UID stored.
     */
    public long[] getLastUID() {
      long pos = size() - 1;
      if (pos < 0) {
        return null;
      }
      else {
        return getUIDAt(pos);
      }
    }

    /**
     * Inserts a uid entry into the list. Generates an error if the entry
     * already in the list.
     */
    public void addUID(long[] uid) {
      // Returns the position of the uid in the list
      long pos = positionOfUID(uid);
      if (pos < 0) {
        pos = -(pos + 1);
        insertEmpty(pos);
        positionOn(pos);
        getDataFile().putLong(uid[0]);
        getDataFile().putLong(uid[1]);
      }
      else {
        throw new RuntimeException("UID already in list");
      }
    }

    private Integer128Bit getInt128BitKey(long record_pos) {
      positionOn(record_pos);
      long high_v = getDataFile().getLong();
      long low_v = getDataFile().getLong();
      return new Integer128Bit(high_v, low_v);
    }

    @Override
    protected Object getRecordKey(long record_pos) {
      return getInt128BitKey(record_pos);
    }

    @Override
    protected int compareRecordTo(long record_pos, Object record_key) {
      Integer128Bit v1 = getInt128BitKey(record_pos);
      Integer128Bit v2 = (Integer128Bit) record_key;
      return v1.compareTo(v2);
    }

  }

  /**
   * A sorted list of block id to UID values.
   */
  private static class BlockIdUIDList extends FixedSizeSerialSet {

    public BlockIdUIDList(DataFile data) {
      super(data, 32);
    }

    /**
     * Returns the position of the BlockId in the list. Returns a negative
     * value if the block id not found in the list.
     */
    public long positionOfBlockId(BlockId block_id) {
      return searchForRecord(block_id);
    }

    /**
     * Inserts a block_id to UID map.
     */
    public void addBlockIdRef(BlockId block_id, long[] uid) {
      // Returns the position of the uid in the list
      long pos = positionOfBlockId(block_id);
      if (pos < 0) {
        pos = -(pos + 1);
        // Insert space for a new entry
        insertEmpty(pos);
        positionOn(pos);
      }
      else {
        // Go to position to overwrite current value
        positionOn(pos);
      }
      DataFile df = getDataFile();
      df.putLong(block_id.getHighLong());
      df.putLong(block_id.getLowLong());
      df.putLong(uid[0]);
      df.putLong(uid[1]);
    }

    public BlockId getBlockIdAt(long record_pos) {
      positionOn(record_pos);
      DataFile df = getDataFile();
      long high_v = df.getLong();
      long low_v = df.getLong();
      return new BlockId(high_v, low_v);
    }

    public long[] getUIDAt(long record_pos) {
      positionOn(record_pos);
      DataFile df = getDataFile();
      df.position(df.position() + 16);
      long high_v = df.getLong();
      long low_v = df.getLong();
      return new long[] { high_v, low_v };
    }

    @Override
    protected Object getRecordKey(long record_pos) {
      return getBlockIdAt(record_pos);
    }

    @Override
    protected int compareRecordTo(long record_pos, Object record_key) {
      BlockId v1 = getBlockIdAt(record_pos);
      BlockId v2 = (BlockId) record_key;
      return v1.compareTo(v2);
    }

  }

  /**
   * An iterator for log entries on a remote machine.
   */
  private class LogEntryIterator {

    private final ServiceAddress machine;
    private long[] first_uid;          // The first UID of the next block
    private boolean initial;
    private final ArrayList<LogEntry> log_entries;
    private int index;

    /**
     * Constructor.
     */
    LogEntryIterator(ServiceAddress machine, long[] initial_uid) {
      this.machine = machine;
      this.first_uid = initial_uid;
      this.initial = true;
      this.log_entries = new ArrayList(64);
      this.index = 0;
    }

    private void fetchNextBlock() {
      MessageStream msg_out = new MessageStream(16);
      msg_out.addMessage("internalFetchLogBundle");
      msg_out.addLongArray(first_uid);
      msg_out.addInteger(initial ? 1 : 0);
      msg_out.closeMessage();

      // Clear the log entries,
      log_entries.clear();
      index = 0;

      // Send the open stream command.
      ProcessResult msg_in;
      // If the service is up,
      if (tracker.isServiceUp(machine, "manager")) {
        // Send to the service,
        MessageProcessor processor = network.connectManagerServer(machine);
        msg_in = processor.process(msg_out);

        // If it's a connection error, return null,
        Iterator<Message> msgs = msg_in.iterator();
        while (msgs.hasNext()) {
          Message m = msgs.next();
          if (m.isError()) {
            // Report the service down if connection failure
            if (isConnectionFault(m)) {
              tracker.reportServiceDownClientReport(machine, "manager");
            }
            log.log(Level.WARNING,
                    "Manager service External exception; {0}",
                    new Object[] { m.getExternalThrowable().getStackTrace() });
            throw new RuntimeException(m.getErrorMessage());
          }
          else {
            long[] uid = (long[]) m.param(0);
            byte[] buf = (byte[]) m.param(1);
            log_entries.add(new LogEntry(uid, buf));
          }
        }
      }
      else {
        throw new RuntimeException("Service down");
      }

      // Update the first uid of the next block,
      if (log_entries.size() > 0) {
        LogEntry last_entry = log_entries.get(log_entries.size() - 1);
        first_uid = last_entry.getUID();
      }

    }

    /**
     * Returns the next log entry, or null if the end is reached.
     */
    LogEntry nextLogEntry() {
      // The end state,
      if (initial == false && index >= log_entries.size()) {
        return null;
      }

      if (initial == true) {
        fetchNextBlock();
        initial = false;
        // End reached?
        if (index >= log_entries.size()) {
          return null;
        }
      }

      // Get the entry from the bundle,
      LogEntry entry = log_entries.get(index);

      ++index;
      // If we reached the end fetch the next bundle,
      if (index >= log_entries.size()) {
        fetchNextBlock();
      }

      return entry;
    }

  }

  /**
   * A log entry.
   */
  private static class LogEntry {

    private final long[] uid;
    private final byte[] buf;

    LogEntry(long[] uid, byte[] buf) {
      this.uid = uid;
      this.buf = buf;
    }

    long[] getUID() {
      return uid;
    }

    byte[] getBuf() {
      return buf;
    }

  }

}
