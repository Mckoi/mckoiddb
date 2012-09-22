/**
 * com.mckoi.network.LocalFileSystemBlockServer  Nov 23, 2008
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

import com.mckoi.data.NodeReference;
import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A block storage component that works over files stored in the local file
 * system using the standard Java IO API.
 *
 * @author Tobias Downer
 */

public class LocalFileSystemBlockServer {

  /**
   * The path where the block information is stored.
   */
  private final File path;

  /**
   * The connector for talking with the other machines in the network.
   */
  private final NetworkConnector connector;

  /**
   * The map of BlockContainer objects managed by this store.
   */
  private final HashMap<BlockId, BlockContainer> block_container_map;

  /**
   * Linked list of last accessed BlockContainer's, the top of the list being
   * the last one accessed.
   */
  private final LinkedList<BlockContainer> block_container_access_list;

  /**
   * A Timer object for scheduling file sync events on a block container
   * after a write.
   */
  private final Timer event_timer;

  /**
   * A lock object for process tasks.
   */
  private final Object process_lock = new Object();

  /**
   * A lock for when a block part is being uploaded into this server.
   */
  private final Object block_upload_lock = new Object();

  /**
   * The current process_id sequence generator.
   */
  private long process_id_seq = 10;

  /**
   * A list of block containers that have sync events coming up.
   */
  private final LinkedList<BlockContainer> blocks_pending_sync;

  /**
   * Global lock for this file system used for ensuring reads on the path
   * are consistant.
   */
  private final Object path_lock = new Object();

  /**
   * The thread that compresses blocks in the background.
   */
  private CompressionThread compression_thread = new CompressionThread();

  /**
   * The list of BlockContainer objects recently created, used by the
   * compression thread.
   */
  private final ArrayList<BlockContainer> compression_add_list = new ArrayList();

  /**
   * Set to the stop state error in the case of a critical stop condition.
   */
  private volatile Throwable stop_state;

  /**
   * The bind flag, either true if the block server is bound to a manager, or
   * false if the block server doesn't have a manager.
   */
  private volatile boolean is_bound;
  private final Object bind_lock = new Object();

  /**
   * The globally unique identifier of this server.
   */
  private long server_guid;

  /**
   * The number of blocks stored in this instance.
   */
  private final AtomicLong block_count = new AtomicLong(0);

  /**
   * The max known block id for the manager key.
   */
  private final HashMap<Integer, BlockId> max_known_block_id;

//  /**
//   * The last block id.
//   */
//  private volatile BlockId last_block_id = null;

  /**
   * The logger.
   */
  private final static Logger log = Logger.getLogger("com.mckoi.network.Log");


  /**
   * Constructor.
   */
  public LocalFileSystemBlockServer(NetworkConnector connector, File path,
                                    Timer timer) {
    this.connector = connector;
    this.path = path;
    block_container_map = new HashMap(5279);
    block_container_access_list = new LinkedList();
    event_timer = timer;
    blocks_pending_sync = new LinkedList();
    max_known_block_id = new HashMap(16);
    stop_state = null;
//    is_bound = false;

  }

  /**
   * Starts and initializes the block store.
   */
  public void start() throws IOException {
    // Open the guid file,
    File guid_file = new File(path, "block_server_guid");
    // If the guid file exists,
    if (guid_file.exists()) {
      // Get the contents,
      FileReader in_reader = new FileReader(guid_file);
      BufferedReader reader = new BufferedReader(in_reader);
      String first_line = reader.readLine();
      // Set the server guid
      server_guid = Long.parseLong(first_line.trim());
      in_reader.close();
    }
    else {
      // The guid file doesn't exist, so create one now,
      boolean created = guid_file.createNewFile();
      if (created) {
        // Create a unique server_guid
        Random r = new Random();
        int v1 = r.nextInt();
        long v2 = System.currentTimeMillis();
        server_guid = (v2 << 16) ^ (v1 & 0x0FFFFFFF);

        // Write it out to the guid file,
        FileWriter guid_writer = new FileWriter(guid_file);
        PrintWriter writer = new PrintWriter(guid_writer);
        writer.println(server_guid);
        writer.flush();
        guid_writer.close();
      }
      else {
        throw new RuntimeException("Unable to create guid server file");
      }
    }

    // Read in all the blocks in and populate the map,
    BlockId[] blocks = fetchBlockList();
//    BlockId in_last_block_id = null;

    synchronized (path_lock) {
      for (BlockId block_id : blocks) {
        BlockContainer container = loadBlock(block_id);
        block_container_map.put(block_id, container);

//        if (in_last_block_id == null) {
//          in_last_block_id = block_id;
//        }
//        else if (block_id.compareTo(in_last_block_id) > 0) {
//          in_last_block_id = block_id;
//        }
      }
    }

    // Discover the block count,
    block_count.set(blocks.length);
//    // The latest block on this server,
//    this.last_block_id = in_last_block_id;

    // Start the compression thread,
    compression_thread = new CompressionThread();
    compression_thread.start();

  }

  /**
   * Stops the block store.
   */
  public void stop() {
    compression_thread.finish();
    compression_thread = null;

    synchronized (path_lock) {
      block_container_map.clear();
      blocks_pending_sync.clear();
      block_container_access_list.clear();
    }
    synchronized (compression_add_list) {
      compression_add_list.clear();
    }

    block_count.set(0);
//    last_block_id = null;
  }



  /**
   * Given a BlockId, returns a file name string for the block (not including
   * the extention).
   */
  private String formatFileName(BlockId block_id) {
    long block_id_h = block_id.getHighLong();
    long block_id_l = block_id.getLowLong();

    StringBuilder b = new StringBuilder();
    b.append(Long.toHexString(block_id_h));
    String l = Long.toHexString(block_id_l);
    int pad = 16 - l.length();
    b.append("X");
    for (int i = 0; i < pad; ++i) {
      b.append("0");
    }
    b.append(l);
    return b.toString();
  }

  /**
   * Given a filename string, parses it to a BlockId value.
   */
  private BlockId parseFileName(String fname_str) {
    int p = fname_str.indexOf("X");
    if (p == -1) {
      throw new RuntimeException("file name format error: " + fname_str);
    }
    String h = fname_str.substring(0, p);
    String l = fname_str.substring(p + 1);

    // Return as a BlockId
    return new BlockId(Long.parseLong(h, 16), Long.parseLong(l, 16));
  }

  /**
   * Loads the store into the block container map. This should only be called
   * once during the entire session per block.
   */
  private BlockContainer loadBlock(BlockId block_id) {
    // If it's not found in the map,
    // Turn the block id into a filename,
    String block_fname = formatFileName(block_id);
    File block_file_name = new File(path, block_fname + ".mcd");
    BlockStore block_store;
    if (!block_file_name.exists()) {
      block_file_name = new File(path, block_fname);
//      // If this file doesn't exist,
//      if (!block_file_name.exists()) {
//        // We check if the block_id is less than maximum id. If it is we
//        // generate an exception indicating this block doesn't exist on this
//        // server. This means something screwed up, either the manager server
//        // was erroneously told the block was located on this server but it
//        // isn't, or the file was deleted by the user.
//        if (last_block_id != null) {
//          int cmp = block_id.compareTo(last_block_id);
//          if (cmp < 0) {
//            throw new BlockReadException(
//                             "Block " + block_id + " not stored on server");
//          }
//        }
//      }
      block_store = new MutableBlockStore(block_id, block_file_name);
    }
    else {
      block_store = new CompressedBlockStore(block_id, block_file_name);
    }
    // Make the block container object,
    BlockContainer container = new BlockContainer(block_id, block_store);
    // Add the new container to the control list (used by the compression
    // thread).
    synchronized (compression_add_list) {
      compression_add_list.add(container);
    }

    return container;
  }

  /**
   * Returns a processor for commands on this server.
   */
  public MessageProcessor getProcessor() {
    // Check for stop state,
    checkStopState();

    return new LFSBlockServerProcessor();
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
   * Try and bind the block server to a manager, returns true if the bind was
   * successful and false otherwise.
   */
  private boolean tryBind() {
    synchronized (bind_lock) {
      if (is_bound) {
        return false;
      }
      else {
        is_bound = true;
        return true;
      }
    }
  }

  /**
   * Try and unbind the block server from a manager, returns true if the
   * unbind was successful and false otherwise.
   */
  private boolean tryUnbind() {
    synchronized (bind_lock) {
      if (is_bound) {
        is_bound = false;
        return true;
      }
      else {
        return false;
      }
    }
  }

  /**
   * Generates an error if this block server isn't bound to a manager.
   */
  private void checkBindState() {
    if (!is_bound) {
      throw new RuntimeException("Block server is not bound to a manager");
    }
  }

  /**
   * Fetches a block container for the given block identifier.
   */
  private BlockContainer fetchBlockContainer(BlockId block_id) throws IOException {
    // Check for stop state,
    checkStopState();

    BlockContainer container;

    synchronized (path_lock) {
      // Look up the block container in the map,
      container = (BlockContainer) block_container_map.get(block_id);
      // If the container not in the map, create it and put it in there,
      if (container == null) {

        container = loadBlock(block_id);
        // Put it in the map,
        block_container_map.put(block_id, container);

//        // If it's not found in the map,
//        // Turn the block id into a filename,
//        String block_fname = String.valueOf(block_id);
//        File block_file_name = new File(path, block_fname + ".mcd");
//        BlockStore block_store;
//        if (!block_file_name.exists()) {
//          block_file_name = new File(path, block_fname);
//          block_store = new MutableBlockStore(block_id, block_file_name);
//        }
//        else {
//          block_store = new CompressedBlockStore(block_id, block_file_name);
//        }
//        // Make the block container object,
//        container = new BlockContainer(block_store);
//        // Put it in the map,
//        block_container_map.put(block_id, container);
      }

      // We manage a small list of containers that have been accessed ordered
      // by last access time.

      // Iterate through the list. If we discover the BlockContainer recently
      // accessed we move it to the front.
      Iterator<BlockContainer> i = block_container_access_list.iterator();
      while (i.hasNext()) {
        BlockContainer bc = i.next();
        if (bc == container) {
          // Found, so move it to the front,
          i.remove();
          block_container_access_list.addFirst(container);
          // Return the container,
          return container;
        }
      }

      // The container isn't found on the access list, so we need to add
      // it.

      // If the size of the list is over some threshold, we clear out the
      // oldest entry and close it,
      int list_size = block_container_access_list.size();
      if (list_size > 32) {
        block_container_access_list.getLast().close();
        block_container_access_list.removeLast();
      }

      // Add to the list,
      container.open();
      block_container_access_list.addFirst(container);

      // And return the container,
      return container;
    }

  }

  /**
   * Schedules a block store synchronization after the given delay provided a
   * file sync for the block hasn't already been scheduled.
   */
  private void scheduleFileSync(final BlockContainer container, int delay) {
    synchronized (path_lock) {
      if (!blocks_pending_sync.contains(container)) {
        blocks_pending_sync.addFirst(container);
        event_timer.schedule(new TimerTask() {
          @Override
          public void run() {
            synchronized (path_lock) {
              blocks_pending_sync.remove(container);
              try {
                container.fsync();
              }
              catch (IOException e) {
                log.log(Level.WARNING, "Sync error: {0}", e.getMessage());
                // We log the warning, but otherwise ignore any IO Error on a
                // file synchronize.
              }
            }
          }
        }, (long) delay);
      }
    }
  }

  /**
   * Fetches the list of all blocks stored on this server.
   */
  private BlockId[] fetchBlockList() {
    File[] dir = path.listFiles();
    HashSet<BlockId> blocks = new HashSet(dir.length);
    for (File f : dir) {
      if (f.isFile()) {
        String fname_str = f.getName();
        if (!fname_str.equals("block_server_guid") &&
            !fname_str.endsWith(".tempc") &&
            !fname_str.endsWith(".tmpc1") &&
            !fname_str.endsWith(".tmpc2")) {
          if (fname_str.endsWith(".mcd")) {
            fname_str = fname_str.substring(0, fname_str.length() - 4);
          }
          BlockId block_id = parseFileName(fname_str);
          blocks.add(block_id);
        }
      }
    }

    // Turn it into an array,
    int sz = blocks.size();
    BlockId[] ls = new BlockId[sz];
    int i = 0;
    for (BlockId b : blocks) {
      ls[i] = b;
      ++i;
    }
    // Return the list,
    return ls;
  }

  /**
   * Returns the number of blocks stored on this server, used for server
   * summary reports only (may not be completely accurate).
   */
  long getBlockCount() {
    return block_count.get();
  }
  

  /**
   * Writes a part of the given block_id into a temporary file. If the position
   * doesn't align correctly with the end of the file, then an exception is
   * generated (this would happen if the same block was being updated by
   * multiple processes).
   */
  void writeBlockPart(BlockId block_id, long pos, int file_type,
                      byte[] buf, int buf_size) {

    String tmpext;
    if (file_type == 1) {
      tmpext = ".tmpc1";
    }
    else if (file_type == 2) {
      tmpext = ".tmpc2";
    }
    else {
      throw new RuntimeException("Unknown file_type: " + file_type);
    }

    // Make sure this process is exclusive
    synchronized (block_upload_lock) {
      try {
        File f = new File(path, formatFileName(block_id) + tmpext);
        if (pos == 0) {
          if (f.exists()) {
            throw new RuntimeException("File exists.");
          }
          else {
            f.createNewFile();
          }
        }
        if (f.length() != pos) {
          throw new RuntimeException("Block sync issue on block file.");
        }

        // Everything ok, we can write the file,
        FileOutputStream fout = new FileOutputStream(f, true);
        fout.write(buf, 0, buf_size);
        fout.close();

      }
      catch (IOException e) {
        throw new RuntimeException("IO Error: " + e.getMessage());
      }
    }

  }

  /**
   * Completes the write of the block on this server and updates the state
   * as appropriate.
   */
  void writeBlockComplete(BlockId block_id, int file_type) {
    String tmpext;
    if (file_type == 1) {
      tmpext = ".tmpc1";
    }
    else if (file_type == 2) {
      tmpext = ".tmpc2";
    }
    else {
      throw new RuntimeException("Unknown file_type: " + file_type);
    }

    // Make sure this process is exclusive
    synchronized (block_upload_lock) {

      String block_fname = formatFileName(block_id);
      File f = new File(path, block_fname + tmpext);

      if (!f.exists() || !f.isFile()) {
        throw new RuntimeException("File doesn't exist");
      }
      // Check the file we are renaming to doesn't exist,
      File f_normal = new File(path, block_fname);
      File f_compress = new File(path, block_fname + ".mcd");
      if (f_normal.exists() || f_compress.exists()) {
        throw new RuntimeException("Block file exists already");
      }

      // Does exist and is a file,
      // What we will rename the file to,
      if (file_type == 1) {
        f.renameTo(f_normal);
      }
      else if (file_type == 2) {
        f.renameTo(f_compress);
      }
      else {
        throw new RuntimeException();
      }

    }

    // Update internal state as appropriate,
    synchronized (path_lock) {
      BlockContainer container = loadBlock(block_id);
      block_container_map.put(block_id, container);
//      if (last_block_id == null) {
//        last_block_id = block_id;
//      }
//      else {
//        int cmp = block_id.compareTo(last_block_id);
//        if (cmp > 0) {
//          last_block_id = block_id;
//        }
//      }
      block_count.incrementAndGet();
    }

  }

  /**
   * Creates an availability map for the given array of block ids. For each
   * block id in the list that is managed by this block service, the
   * corresponding entry in the byte array is set to 1.
   */
  private byte[] createAvailabilityMapForBlocks(BlockId[] blocks) {

    byte[] result = new byte[blocks.length];

    // Use the OS filesystem file name lookup to determine if the block is
    // stored here or not.

    for (int i = 0; i < blocks.length; ++i) {
      boolean found = true;
      // Turn the block id into a filename,
      String block_fname = formatFileName(blocks[i]);
      // Check for the compressed filename,
      File block_file_name = new File(path, block_fname + ".mcd");
      if (!block_file_name.exists()) {
        // Check for the none-compressed filename
        block_file_name = new File(path, block_fname);
        // If this file doesn't exist,
        if (!block_file_name.exists()) {
          found = false;
        }
      }

      // Set the value in the map
      result[i] = found ? (byte) 1 : (byte) 0;
    }

    return result;
  }

  /**
   * Notifies this block server that the given nodes in the block must be
   * preserved, and any nodes in the block not in the node array may be
   * reclaimed at some point. This is a resource reclamation function used
   * by the garbage collector to free up resources.
   * <p>
   * This implementation will make a backup of the existing block and then
   * rewrite the block with the given node set preserved. This method will
   * make a reasonable attempt to clean up the block however it may not
   * perform the operation for the following reasons;
   * <p>
   * 1. There are not enough nodes being reclaimed in the block to make it
   *  worthwhile.
   * <p>
   * 2. The block has recently had a garbage collection operation performed on
   *  it.
   * <p>
   * 3. The block was updated too recently.
   * <p>
   * This method will return immediately and schedule the resource reclamation
   * for a later time. Returns the process_id of the background task or -1
   * if the process was not scheduled.
   */
  private long preserveNodesInBlock(
                           final BlockId block_id, final DataAddress[] nodes) {
    synchronized (process_lock) {
      long process_id = process_id_seq;
      process_id_seq = process_id_seq + 1;
      // Schedule the process to happen within a second.
      event_timer.schedule(new TimerTask() {
        @Override
        public void run() {
          doBlockRewrite(block_id, nodes);
        }
      }, 1000);
      return process_id;
    }
  }

  /**
   * Rewrites the nodes in the block preserving the given nodes in the block
   * and deleting any nodes that are in the block but not in the given array.
   */
  private void doBlockRewrite(BlockId block_id,
                              DataAddress[] nodes_to_preserve) {
    
    BlockContainer block_container = null;
    try {

      // Fetch the block container,
      block_container = fetchBlockContainer(block_id);

      // If the block container is not compressed then fail,
      if (!block_container.isCompressed()) {
        // FAIL;
        log.log(Level.WARNING,
                "Failed block rewrite because source block is not compressed.");
        return;
      }

      // The last time the block was written to,
      long block_last_write = block_container.getLastWrite();
      // Check the last modified time,
      if (block_last_write == 0) {
        // This signifies an IO error,
        // FAIL;
        log.log(Level.WARNING,
                "Failed block rewrite because last modified time is 0.");
        return;
      }
      // If the block file was updated within 7 days then fail to update,
      // NOTE: 7 days?
      long time_7_days_ago = System.currentTimeMillis() -
                                                  ( 7 * 24 * 60 * 60 * 1000 );
      if (block_last_write > time_7_days_ago) {
        // Block file written too soon,
        // FAIL;
        log.log(Level.WARNING,
              "Failed block rewrite because block was created within 7 days.");
        return;
      }

      int[] data_id_arr = new int[nodes_to_preserve.length];

      int i = 0;
      // For each node being preserved,
      for (DataAddress address : nodes_to_preserve) {
        // The block being written to,
        BlockId node_block_id = address.getBlockId();
        // The data identifier,
        int node_data_id = address.getDataId();
        // Check the block id is the same,
        if (!block_id.equals(node_block_id)) {
          // FAIL;
          log.log(Level.WARNING,
                  "Failed block rewrite because node to preserve does not have same block id.");
          return;
        }

        // Record the node_data_id into a list,
        data_id_arr[i] = node_data_id;
        ++i;

      }

      // Sort the data id array,
      Arrays.sort(data_id_arr);
      
      // Check there are no duplicates,
      int len = data_id_arr.length;
      int previous_id = -1;
      for (int n = 0; n < len; ++n) {
        int data_id = data_id_arr[n];
        if (data_id == previous_id) {
          // FAIL;
          log.log(Level.WARNING,
                  "Failed block rewrite because duplicate nodes in input node set.");
          return;
        }
        previous_id = data_id;
      }

      // The file to write to,
      String block_fname = formatFileName(block_id);
      File rewrite_file = new File(path, block_fname + ".rew");
      // Fail if the file exists,
      if (rewrite_file.exists()) {
        // FAIL;
        log.log(Level.WARNING,
                "Failed block rewrite because file exists: {0}",
                new Object[] { rewrite_file.getAbsoluteFile() });
        return;
      }
      // The rewrite store,
      MutableBlockStore block_rewrite_store =
                                 new MutableBlockStore(block_id, rewrite_file);
      int nodes_disposed_count = 0;
      long nodes_disposed_size = 0;
      try {
        // Open the rewrite block store,
        block_rewrite_store.open();

        // Get the maximum data id stored in the container,
        int max_data_id = block_container.getMaxDataId();
        // Read through all the blocks,
        int data_id = 0;
        while (data_id <= max_data_id) {
          // Fetch the node,
          NodeSet node_set = block_container.read(data_id);

          // For each node reference,
          Iterator<NodeItemBinary> node_items = node_set.getNodeSetItems();
          while (node_items.hasNext()) {
            NodeItemBinary bin = node_items.next();
            DataInputStream in = new DataInputStream(bin.getInputStream());
            // HACK:
            // We must use contextual node information here to work out the
            // size of the node to copy.
            byte[] node_buf = NetworkTreeSystem.readSingleNodeData(in);

            // Write the node if it's in the preserve list,
            int in_data_id = new DataAddress(bin.getNodeId()).getDataId();
            // If the data id of this node is in the data_id_arr set then this
            // node needs to be preserved.
            if (Arrays.binarySearch(data_id_arr, in_data_id) >= 0) {
              block_rewrite_store.putData(data_id, node_buf, 0, node_buf.length);
            }
            else {
              ++nodes_disposed_count;
              nodes_disposed_size += node_buf.length;
            }

            ++data_id;
          }

        }
      }
      finally {
        // Close the block rewrite store,
        block_rewrite_store.close();
      }

      // Rewrite done,
      log.log(Level.INFO,
              "Node rewrite complete. Nodes disposed = {0}, Rewrite block file = {1}",
              new Object[] { nodes_disposed_count,
                             rewrite_file.getAbsoluteFile() });

      // If we disposed less than about 50k of nodes then we don't bother
      // HACK: This value is very arbitrary!
      if (nodes_disposed_size < 51200) {
        // Delete the rewrite file,
        log.log(Level.SEVERE,
            "PENDING - Not enough nodes disposed so delete the rewrite block");
      }
      // If nodes disposed, push this rewrite file as the new block file and
      // rename the old.
      else {
        log.log(Level.SEVERE,
            "PENDING - Push the rewritten block file as current.");
      }

    }
    catch (IOException e) {
      log.log(Level.WARNING, "IO Error", e);
    }
    // Make sure we close the container,
    finally {
      try {
        block_container.close();
      }
      catch (IOException e) {
        log.log(Level.WARNING, "IOException closing block container", e);
      }
    }

  }

  /**
   * Notification generated by a manager server to inform this block server
   * the maximum block being allocated against. This allows the block server
   * to perform maintenance on its block set (such as compression).
   */
  private void notifyCurrentBlockId(BlockId block_id) {

//    log.log(Level.FINEST, "notifyCurrentBlockId {0}", block_id);

    // Extract the low byte out of the block_id, which is a key for the manager
    // server that generated this block chain.
    int manager_key = ((int) block_id.getLowLong() & 0x0FF);

    // Update the map for this key,
    synchronized (max_known_block_id) {
      max_known_block_id.put(manager_key, block_id);
    }

  }


  /**
   * True if the given BlockContainer is a known static block that will not
   * change. A static block can be copied or compressed. This looks at the
   * known max block id map to determine if the block is static.
   */
  private boolean isKnownStaticBlock(BlockContainer block) {

    // If the block was written to less than 3 minutes ago, return false
    if (block.getLastWrite() > System.currentTimeMillis() - (3 * 60 * 1000)) {
      return false;
    }
    // Extract the server key from the block id
    BlockId block_id = block.block_id;
    int server_id = ((int) block_id.getLowLong() & 0x0FF);

    // Look up the max known block id for the manager server,
    BlockId max_block_id;
    synchronized (max_known_block_id) {
      max_block_id = max_known_block_id.get(server_id);
    }

    // If the block is less than the max, the block can be compressed!
    if (max_block_id != null && block_id.compareTo(max_block_id) < 0) {
      return true;
    }
    // Otherwise update the last write flag (so we only check the max block id
    // every 3 mins).
    block.touchLastWrite();
    return false;
  }


  // ---------- Inner classes ----------





  /**
   * A thread that continues to run in the background performing the compress
   * function.
   */
  private class CompressionThread extends Thread {

    boolean finished = false;
    boolean has_finished = false;

    @Override
    public void run() {
      try {
        synchronized (this) {

          // Wait 2 seconds,
          wait(2000);

          // Any new block containers added, we need to process,
          ArrayList<BlockContainer> new_items = new ArrayList();

          while (true) {

            synchronized (compression_add_list) {
              for (BlockContainer container : compression_add_list) {
                new_items.add(container);
              }
              compression_add_list.clear();
            }

            // Sort the container list,
            Collections.sort(new_items);

            int sz = new_items.size();
            Iterator<BlockContainer> it = new_items.iterator();
            for (int i = 0; i < sz; ++i) {
              BlockContainer container = it.next();

              // If it's already compressed, remove it from the list
              if (container.isCompressed()) {
                it.remove();
              }
              // Don't compress if written to less than 3 minutes ago,
              // and we confirm it can be compressed,
              else if (isKnownStaticBlock(container)) {

                MutableBlockStore mblock_store =
                                      (MutableBlockStore) container.block_store;
                final File sourcef = mblock_store.getFile();
                File destf = new File(sourcef.getParent(),
                                      sourcef.getName() + ".tempc");
                try {
                  destf.delete();
                  log.log(Level.FINE, "Compressing block: {0}", container.block_id);
                  log.log(Level.FINE, "Current block size = {0}", sourcef.length());

                  // Compress the file,
                  CompressedBlockStore.compress(sourcef, destf);
  //                // Compress it,
  //                System.out.println("Compressed: " + container.block_id);
                  // Rename the file,
                  File compressedf = new File(sourcef.getParent(),
                                              sourcef.getName() + ".mcd");
                  destf.renameTo(compressedf);

                  // Switch the block container,
                  container.changeStore(
                      new CompressedBlockStore(container.block_id, compressedf));

                  log.log(Level.FINE, "Compression of block {0} finished.", container.block_id);
                  log.log(Level.FINE, "Compressed block size = {0}", compressedf.length());
                  // Wait a little bit and delete the original file,
                  if (finished) {
                    return;
                  }
                  wait(1000);

                  // Delete the file after 5 minutes,
                  event_timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                      log.log(Level.FINE, "Deleting file {0}", sourcef.getName());
                      sourcef.delete();
                    }
                  }, 5 * 60 * 1000);

                  // Remove it from the new_items list
                  it.remove();
                }
                catch (IOException e) {
                  log.log(Level.SEVERE, "IO Error in compression thread", e);
                }
              }

              if (finished) {
                return;
              }
              wait(200);

            }

            if (finished) {
              return;
            }
            wait(3000);
          }
        }
      }
      catch (InterruptedException e) {
        // InterruptedException causes the thread to end,
      }
      // Make sure this is called on thread termination,
      finally {
        synchronized (this) {
          has_finished = true;
          notifyAll();
        }
      }
    }

    public void finish() {
      synchronized (this) {
        finished = true;
        while (has_finished == false) {
          try {
            notifyAll();
            wait();
          }
          catch (InterruptedException e) {
            throw new Error("Interrupted", e);
          }
        }
      }
    }

  }








  /**
   * The container for a block.
   */
  private static class BlockContainer implements Comparable {
    
    /**
     * The MutableBlockStore object that represents the contents of the block.
     */
    private BlockStore block_store;

    /**
     * Whether the block store is compressed or not.
     */
    private boolean is_compressed;

    private final BlockId block_id;

    /**
     * Timestamp of the last update on the block container.
     */
    private volatile long last_write = 0;


    private int lock_count = 0;

    /**
     * Constructs the block container.
     */
    BlockContainer(BlockId block_id, BlockStore block_store) {
      this.block_id = block_id;
      if (block_store instanceof MutableBlockStore) {
        is_compressed = false;
      }
      else if (block_store instanceof CompressedBlockStore) {
        is_compressed = true;
      }
      else {
        throw new RuntimeException("Unknown block_store type");
      }
      this.block_store = block_store;
    }

    /**
     * Returns true if the block store is compressed.
     */
    boolean isCompressed() {
      return is_compressed;
    }

    /**
     * Touches the 'last_write' variable to the current time.
     */
    void touchLastWrite() {
      last_write = System.currentTimeMillis();
    }

    /**
     * Returns the last time this container was written to.
     */
    long getLastWrite() {
      return last_write;
    }

    /**
     * Returns the MutableBlockStore backing this block container.
     */
    private BlockStore getBlockStore() {
      return block_store;
    }

    /**
     * Returns the time the block store was last modified. This is either the
     * time a node was written to the store if it's a mutable store, or the
     * time the store was created if it's an immutable store.
     */
    private long getLastModified() {
      return block_store.getLastModified();
    }

    /**
     * Opens the block store.
     */
    boolean open() throws IOException {
      synchronized (this) {
        if (lock_count == 0) {
          ++lock_count;
          return block_store.open();
        }
        ++lock_count;
        return false;
      }
    }

    /**
     * Close the block store.
     */
    void close() throws IOException {
      synchronized (this) {
        --lock_count;
        if (lock_count == 0) {
          block_store.close();
        }
      }
    }

    /**
     * Switch the block store.
     */
    void changeStore(BlockStore new_store) throws IOException {
      synchronized (this) {
        if (lock_count > 0) {
          block_store.close();
          new_store.open();
        }
        if (new_store instanceof MutableBlockStore) {
          is_compressed = false;
        }
        else if (new_store instanceof CompressedBlockStore) {
          is_compressed = true;
        }
        else {
          throw new RuntimeException("Unknown block_store type");
        }
        block_store = new_store;
      }
    }

    /**
     * Writes a node to the block store.
     */
    void write(int data_id, byte[] buf, int off, int len) throws IOException {
      touchLastWrite();
      synchronized (this) {
        block_store.putData(data_id, buf, off, len);
      }
    }

    /**
     * Reads a node from the block store.
     */
    NodeSet read(int data_id) throws IOException {
      synchronized (this) {
        return block_store.getData(data_id);
      }
    }

    /**
     * Returns the maximum data id stored in the block.
     */
    int getMaxDataId() throws IOException {
      synchronized (this) {
        return block_store.getMaxDataId();
      }
    }

    /**
     * Removes a node from the block store.
     */
    void remove(int data_id) throws IOException {
      touchLastWrite();
      synchronized (this) {
        block_store.removeData(data_id);
      }
    }

    /**
     * Returns a 64-bit checksum of all node data recorded in this block.
     */
    long createChecksumValue() throws IOException {
      synchronized (this) {
        return block_store.createChecksumValue();
      }
    }

//    /**
//     * Returns the list of data objects stored in this block.
//     */
//    int[] getDataList() throws IOException {
//      synchronized (this) {
//        return block_store.getDataList();
//      }
//    }

    /**
     * Attempts to perform a file synchronization operation.
     */
    void fsync() throws IOException {
      synchronized (this) {
        block_store.fsync();
      }
    }


    @Override
    public boolean equals(Object ob) {
      return this == ob;
    }

    @Override
    public String toString() {
      return block_store.toString();
    }

    @Override
    public int compareTo(Object o) {
      BlockContainer dc = (BlockContainer) o;
      return block_id.compareTo(dc.block_id);
    }

  }
  
  
  
  
  /**
   * A local file system command stream with this server. Note that this object
   * is not thread safe, however concurrent command streams are thread safe.
   */
  private class LFSBlockServerProcessor extends AbstractProcessor {



    /**
     * Fetch the block. If it's in the touched map get from that, otherwise
     * fetch from the parent and add to the touched map.
     */
    private BlockContainer getBlock(HashMap<BlockId, BlockContainer> touched,
                                    BlockId block_id) throws IOException {
      BlockContainer b = touched.get(block_id);
      if (b == null) {
        b = fetchBlockContainer(block_id);
        boolean created = b.open();
        if (created) {
          block_count.incrementAndGet();
//          last_block_id = block_id;
        }
        touched.put(block_id, b);
//        System.out.println("LocalFileSystemBlockServer OPENING: " + block_id);
      }
      return b;
    }

    /**
     * Close any touched containers.
     */
    private void closeContainers(Map<BlockId, BlockContainer> touched)
                                                   throws IOException {
      Set<BlockId> keys = touched.keySet();
      for (BlockId block_id : keys) {
        BlockContainer c = touched.get(block_id);
//        System.out.println("LocalFileSystemBlockServer CLOSING: " + block_id);
        c.close();
      }
    }





    /**
     * {@inheritDoc }
     */
    @Override
    public MessageStream process(MessageStream message_stream) {

      // The map of containers touched,
      HashMap<BlockId, BlockContainer> containers_touched = new HashMap();
      // The reply message,
      MessageStream reply_message = new MessageStream(32);
      // The nodes fetched in this message,
      ArrayList<NodeReference> read_nodes = null;

      // The messages in the stream,
      Iterator<Message> iterator = message_stream.iterator();
      while (iterator.hasNext()) {
        Message m = iterator.next();
        try {
          // Check for stop state,
          checkStopState();

          // writeToBlock(DataAddress address, byte[] buf, int off, int len)
          if (m.getName().equals("writeToBlock")) {
            writeToBlock(containers_touched,
                         (DataAddress) m.param(0),
                         (byte[]) m.param(1), (Integer) m.param(2),
                         (Integer) m.param(3));
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }
          // readFromBlock(DataAddress address)
          else if (m.getName().equals("readFromBlock")) {
            if (read_nodes == null) {
              read_nodes = new ArrayList();
            }
            DataAddress addr = (DataAddress) m.param(0);
            if (!read_nodes.contains(addr.getValue())) {
              NodeSet node_set = readFromBlock(containers_touched, addr);
              reply_message.addMessage("R");
              reply_message.addNodeSet(node_set);
              reply_message.closeMessage();
              NodeReference[] nodes_out = node_set.getNodeIdSet();
              for (NodeReference node : nodes_out) {
                read_nodes.add(node);
              }
            }
          }
          // rollbackNodes(DataAddress[] addresses)
          else if (m.getName().equals("rollbackNodes")) {
            removeNodes(containers_touched, (DataAddress[]) m.param(0));
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }
          // deleteBlock(BlockId block_id)
          else if (m.getName().equals("deleteBlock")) {
            deleteBlock((BlockId) m.param(0));
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }
          // serverGUID()
          else if (m.getName().equals("serverGUID")) {
            reply_message.addMessage("R");
            reply_message.addLong(server_guid);
            reply_message.closeMessage();
          }
          // blockSetReport()
          else if (m.getName().equals("blockSetReport")) {
            BlockId[] arr = blockSetReport();
            reply_message.addMessage("R");
            reply_message.addLong(server_guid);
            reply_message.addBlockIdArr(arr);
            reply_message.closeMessage();
          }
          // poll(String poll_msg)
          else if (m.getName().equals("poll")) {
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }

          // notifyCurrentBlockId(BlockId block_id)
          else if (m.getName().equals("notifyCurrentBlockId")) {
            notifyCurrentBlockId((BlockId) m.param(0));
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }

          // blockChecksum(BlockId block_id)
          else if (m.getName().equals("blockChecksum")) {
            long checksum = blockChecksum(containers_touched,
                                          (BlockId) m.param(0));
            reply_message.addMessage("R");
            reply_message.addLong(checksum);
            reply_message.closeMessage();
          }
          // sendBlockTo(BlockId block_id, ServiceAddress block_address,
          //             long dest_server_sguid,
          //             ServerAddress[] manager_addresses)
          else if (m.getName().equals("sendBlockTo")) {
            // Returns immediately. There's currently no way to determine
            // when this process will happen or if it will happen.
            BlockId block_id = (BlockId) m.param(0);
            ServiceAddress destination_address = (ServiceAddress) m.param(1);
            long dest_server_sguid = (Long) m.param(2);
            ServiceAddress[] manager_servers = (ServiceAddress[]) m.param(3);
            long process_id = sendBlockTo(block_id,
                                          destination_address,
                                          dest_server_sguid,
                                          manager_servers);
            reply_message.addMessage("R");
            reply_message.addLong(process_id);
            reply_message.closeMessage();
          }
          // sendBlockPart(BlockId block_id, long pos, int file_type,
          //               byte[] buf, int size)
          else if (m.getName().equals("sendBlockPart")) {
            BlockId block_id = (BlockId) m.param(0);
            long pos = (Long) m.param(1);
            int file_type = (Integer) m.param(2);
            byte[] buf = (byte[]) m.param(3);
            int buf_size = (Integer) m.param(4);
            writeBlockPart(block_id, pos, file_type, buf, buf_size);
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }
          // sendBlockComplete(BlockId block_id, int file_type)
          else if (m.getName().equals("sendBlockComplete")) {
            BlockId block_id = (BlockId) m.param(0);
            int file_type = (Integer) m.param(1);
            writeBlockComplete(block_id, file_type);
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }

          // preserveNodesInBlock(BlockId block_id, DataAddress[] nodes)
          else if (m.getName().equals("preserveNodesInBlock")) {
            BlockId block_id = (BlockId) m.param(0);
            DataAddress[] nodes = (DataAddress[]) m.param(1);
            long process_id = preserveNodesInBlock(block_id, nodes);
            reply_message.addMessage("R");
            reply_message.addLong(process_id);
            reply_message.closeMessage();
          }

          // createAvailabilityMapForBlocks(BlockId[] block_ids)
          else if (m.getName().equals("createAvailabilityMapForBlocks")) {
            BlockId[] block_ids = (BlockId[]) m.param(0);
            byte[] map = createAvailabilityMapForBlocks(block_ids);
            reply_message.addMessage("R");
            reply_message.addBuf(map);
            reply_message.closeMessage();
          }

          // bindWithManager()
          else if (m.getName().equals("bindWithManager")) {
            bindWithManager();
            reply_message.addMessage("R");
            reply_message.addInteger(1);
            reply_message.closeMessage();
          }
          // unbindWithManager()
          else if (m.getName().equals("unbindWithManager")) {
            unbindWithManager();
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

      // Release any containers touched,
      try {
        closeContainers(containers_touched);
      }
      catch (IOException e) {
        log.log(Level.SEVERE, "IOError when closing containers", e);
      }

      return reply_message;
    }

    
    
    
    
    

    /**
     * Currently not used.
     */
    private void bindWithManager() {
      
//      // Available to be bound?
//      if (!tryBind()) {
//        throw new RuntimeException("Server already bound");
//      }

    }

    /**
     * Currently not used.
     */
    private void unbindWithManager() {
      
//      // Available to be bound?
//      if (!tryUnbind()) {
//        throw new RuntimeException("Server not bound");
//      }

    }

    /**
     * Returns a 64-bit checksum value calculated over the block data.
     */
    private long blockChecksum(HashMap<BlockId, BlockContainer> containers_touched,
                               BlockId block_id) throws IOException {

      // Fetch the block container,
      BlockContainer container = getBlock(containers_touched, block_id);
      // Calculate the checksum value,
      return container.createChecksumValue();
    }

    /**
     * Writes node information to the given block referenced by the DataAddress.
     * buf, off and len contain the node information to be written.
     */
    private void writeToBlock(HashMap<BlockId, BlockContainer> containers_touched,
                             DataAddress address,
                             byte[] buf, int off, int len) throws IOException {

      // The block being written to,
      BlockId block_id = address.getBlockId();
      // The data identifier,
      int data_id = address.getDataId();

      // Fetch the block container,
      BlockContainer container = getBlock(containers_touched, block_id);
      // Write the data,
      container.write(data_id, buf, off, len);

      // Schedule the block to be file synch'd 5 seconds after a write
      scheduleFileSync(container, 5000);
    }

    /**
     * Reads a node at the given DataAddress stored by this server.
     */
    private NodeSet readFromBlock(HashMap<BlockId, BlockContainer> containers_touched,
                                  DataAddress address) throws IOException {

      // The block being written to,
      BlockId block_id = address.getBlockId();
      // The data identifier,
      int data_id = address.getDataId();

      // Fetch the block container,
      BlockContainer container = getBlock(containers_touched, block_id);
      // Read the data,
      return container.read(data_id);

    }

    /**
     * Removes all the nodes represented by the given array of DataAddress
     * objects.
     */
    private void removeNodes(HashMap<BlockId, BlockContainer> containers_touched,
                             DataAddress[] addresses) throws IOException {
      for (DataAddress address : addresses) {
        // The block being removed from,
        BlockId block_id = address.getBlockId();
        // The data identifier,
        int data_id = address.getDataId();

        // Fetch the block container,
        BlockContainer container = getBlock(containers_touched, block_id);
        // Remove the data,
        container.remove(data_id);
        // Schedule the block to be file synch'd 5 seconds after a write
        scheduleFileSync(container, 5000);
      }
    }

    /**
     * Deletes the given block_id from this server. The removal of this block
     * may not happen immediately. If this server goes down before the block
     * is removed, it will not get deleted when the server comes back up.
     */
    private void deleteBlock(BlockId block_id) {

    }

    /**
     * Returns a report of blocks stored on this block server.
     */
    private BlockId[] blockSetReport() {
      return fetchBlockList();
//      // The list of block_ids currently accessible,
//      synchronized (path_lock) {
//        Set<Long> key_set = block_container_map.keySet();
//        long[] arr = new long[key_set.size()];
//        int p = 0;
//        for (long v : key_set) {
//          arr[p] = v;
//          ++p;
//        }
//        return arr;
//      }
    }

    /**
     * Schedules a background process that sends a block from this block server
     * to the destination block server. Returns the process_id for the process.
     */
    private long sendBlockTo(BlockId block_id, ServiceAddress destination,
                             long dest_server_sguid,
                             ServiceAddress[] manager_servers) {
      synchronized (process_lock) {
        long process_id = process_id_seq;
        process_id_seq = process_id_seq + 1;
        SendBlockProcess p_task =
                     new SendBlockProcess(process_id, block_id,
                                          destination,
                                          dest_server_sguid,
                                          manager_servers);
        // Schedule the process to happen immediately (or as immediately as
        // possible).
        event_timer.schedule(p_task, 0);

        return process_id;
      }
    }

  }



  /**
   * A process for sending a block from this machine to the destination block
   * server.
   * <p>
   * Note that this process could take time to complete depending on network
   * conditions and the size of the block.
   */
  private class SendBlockProcess extends TimerTask {

    final long process_id;
    final BlockId block_id;
    final ServiceAddress destination;
    final long dest_server_sguid;
    final ServiceAddress[] manager_servers;

    public SendBlockProcess(long process_id, BlockId block_id,
                            ServiceAddress destination,
                            long dest_server_sguid,
                            ServiceAddress[] manager_servers) {
      this.process_id = process_id;
      this.block_id = block_id;
      this.destination = destination;
      this.dest_server_sguid = dest_server_sguid;
      this.manager_servers = manager_servers;
    }

    @Override
    public void run() {
      // Connect to the destination service address,
      MessageProcessor p = connector.connectBlockServer(destination);
      // Get the block file,
      String block_file_name = formatFileName(block_id);
      int file_type = 1;
      File f = new File(path, block_file_name);
      if (!f.exists()) {
        file_type = 2;
        f = new File(path, block_file_name + ".mcd");
      }
      // If the file doesn't exist, exit,
      if (!f.exists()) {
        return;
      }
      BlockContainer block_container = null;
      try {

        block_container = fetchBlockContainer(block_id);
        // If the block was written to less than 6 minutes ago, we don't allow
        // the copy to happen,
        if (!isKnownStaticBlock(block_container)) {
          // This will happen if this block server has not be notified
          // recently by the managers the maximum block id they are managing.
          log.log(Level.INFO, "Can't copy last block_id ( {0} ) on server, it's not a known static block.",
                              block_id);
          return;
        }
//        else if (block_container.getLastWrite() >
//                 System.currentTimeMillis() - (6 * 60 * 1000)) {
//          // Won't copy a block that was written to within the last 6 minutes,
//          log.log(Level.INFO,
//               "Can't copy block ( {0} ) written to within the last 6 minutes.",
//               block_id);
//          return;
//        }

        // If the file does exist, push it over,
        byte[] buf = new byte[16384];
        int pos = 0;
        FileInputStream fin = new FileInputStream(f);

        while (true) {
          int read = fin.read(buf, 0, buf.length);
          // Exit if we reached the end of the file,
          if (read == -1) {
            break;
          }
          MessageStream msg_out = new MessageStream(8);
          msg_out.addMessage("sendBlockPart");
          msg_out.addBlockId(block_id);
          msg_out.addLong(pos);
          msg_out.addInteger(file_type);
          msg_out.addBuf(buf);
          msg_out.addInteger(read);
          msg_out.closeMessage();
          // Process the message,
          ProcessResult msg_in = p.process(msg_out);
          // Get the input iterator,
          Iterator<Message> i = msg_in.iterator();
          while (i.hasNext()) {
            Message m = i.next();
            if (m.isError()) {
              log.log(Level.INFO, "'sendBlockPart' command error: {0}",
                                  m.getErrorMessage());
//              System.out.println(m.getExternalThrowable().getStackTrace());
              return;
            }
          }

          pos += read;
        }

        // Close,
        fin.close();

        // Send the 'complete' command,
        MessageStream msg_out = new MessageStream(8);
        msg_out.addMessage("sendBlockComplete");
        msg_out.addBlockId(block_id);
        msg_out.addInteger(file_type);
        msg_out.closeMessage();
        // Process the message,
        ProcessResult msg_in = p.process(msg_out);
        // Get the input iterator,
        Iterator<Message> i = msg_in.iterator();
        while (i.hasNext()) {
          Message m = i.next();
          if (m.isError()) {
            log.log(Level.INFO, "'sendBlockCommand' command error: {0}",
                                m.getErrorMessage());
//            System.out.println(m.getExternalThrowable().getStackTrace());
            return;
          }
        }

        // Tell the manager server about this new block mapping,
        msg_out = new MessageStream(8);
        msg_out.addMessage("internalAddBlockServerMapping");
        msg_out.addBlockId(block_id);
        msg_out.addLongArray(new long[] { dest_server_sguid });
        msg_out.closeMessage();
        log.log(Level.INFO, "Adding block_id->server mapping ({0} -> {1})",
                            new Object[] { block_id, dest_server_sguid });
//        System.out.println("Add mapping: " + block_id + " to " + dest_server_sguid);

        for (int n = 0; n < manager_servers.length; ++n) {
          // Process the message,
          MessageProcessor mp =
                            connector.connectManagerServer(manager_servers[n]);
          msg_in = mp.process(msg_out);
          // Get the input iterator,
          i = msg_in.iterator();
          while (i.hasNext()) {
            Message m = i.next();
            if (m.isError()) {
              log.log(Level.INFO,
                      "'internalAddBlockServerMapping' command error: @ {0} - {1}",
                      new Object[] { manager_servers[n].displayString(),
                                     m.getErrorMessage() });
//            System.out.println(m.getExternalThrowable().getStackTrace());
              break;
            }
          }
        }

      }
      catch (IOException e) {
        log.log(Level.WARNING, "IO Error", e);
      }
      // Make sure we close the container,
      finally {
        try {
          block_container.close();
        }
        catch (IOException e) {
          log.log(Level.WARNING, "IOException closing block container", e);
        }
      }

    }

  }

}
