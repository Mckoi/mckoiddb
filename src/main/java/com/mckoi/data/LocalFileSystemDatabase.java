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

package com.mckoi.data;

import com.mckoi.debug.DefaultDebugLogger;
import com.mckoi.store.Area;
import com.mckoi.store.AreaWriter;
import com.mckoi.store.JournalledFileStore;
import com.mckoi.store.LoggingBufferManager;
import com.mckoi.store.MutableArea;
import java.io.File;
import java.io.IOException;

/**
 * A database implementation that uses the com.mckoi.database.store API to
 * persist data structures in the local file system with a simple
 * configuration.  This object implements a full range of features including
 * journaling and a heap cache.
 * <p>
 * The directory specified in the local file system will
 * contain one or more 'data.koi' files, journal files 'jnl*' and a lock
 * file.  The name of the file resources may not be changed, therefore
 * storing multiple database repositories in the same directory is not
 * permitted.
 * <p>
 * By default, this object operates with conservative cache options allocating
 * 8MB for the page cache (maximum of 1,024 8KB pages).  File rollover is set
 * to 512MB (the maximum size a file will grow to before new data blocks are
 * stored in a new file).
 * <p>
 * Node and branch cache size is set to 14MB and 2MB respectively.  In the
 * default configuration, the object may use as much as 30MB of heap on cached
 * content alone.
 * <p>
 * All cache sizes can be configured as necessary.
 * <p>
 * This object is useful for creating embedded database instances inside a
 * Java application. It does not have any networking or concurrency control
 * features.
 *
 * @author Tobias Downer
 */

public final class LocalFileSystemDatabase implements KeyObjectDatabase {

  /**
   * The path where the database is stored in the local filesystem.
   */
  private final File path;
  
  /**
   * The handler for debugging messages.
   */
  private DefaultDebugLogger debug;
  
  /**
   * The LoggingBufferManager handles the paging mechanism for translating
   * the data address space into file in the local filesystem, and the page
   * cache.
   */
  private LoggingBufferManager buffer_manager;
  
  /**
   * The JournalledFileStore handles the block allocation and deallocation
   * system.
   */
  private JournalledFileStore file_store;

  /**
   * The StoreBackedTreeSystem object that is the database.
   */
  private StoreBackedTreeSystem database_ob;
  
  /**
   * True if the database has started and is accepting transaction requests.
   */
  private boolean database_started;

  /**
   * The file size rollover value (the number of bytes stored in a data file
   * before expansion of the file is rolled over into a new file).
   */
  private long file_rollover_size;
  
  /**
   * The page size of data read by the buffer manager when accessing
   * information in the database file(s). 
   */
  private int page_size;
  
  /**
   * The page size cache (the maximum number of pages that can be held by the
   * buffer manager).
   */
  private int max_page_count;
  
  /**
   * The size of a branch node (the maximum number of pointers on a branch
   * node).
   */
  private int branch_node_size;
  
  /**
   * The size of a leaf node (the maximum number of bytes of data that can be
   * stored on a leaf node).
   */
  private int leaf_node_size;

  /**
   * The maximum size of nodes stored on the heap before the data is
   * flushed to the backing store.
   */
  private long heap_node_cache_size;

  /**
   * The maximum heap size of branch nodes stored in the cache.
   */
  private long branch_node_cache_size;

  /**
   * Synchronization lock object.
   */
  private final Object lock_object = new Object();

  /**
   * Synchronization for enforcing serial commits.
   */
  private final Object commit_lock = new Object();
  
  
  
  /**
   * Constructs the file system database with the given local path to the
   * repository in the local file system where the data is to be stored.
   */
  public LocalFileSystemDatabase(File the_path) {
    this.path = the_path;
    this.debug = new DefaultDebugLogger();
    debug.setDebugLevel(1000000);

    setDefaultValues();

  }

  /**
   * Sets the configuration to default values.
   */
  private void setDefaultValues() {
    synchronized (lock_object) {
      file_rollover_size = 512 * 1024 * 1024;
      page_size = 8 * 1024;
      max_page_count = 1024;
      branch_node_size = 16;
      leaf_node_size = 4010;
      heap_node_cache_size = 14 * 1024 * 1024;
      branch_node_cache_size = 2 * 1024 * 1024;
    }
  }
  
  // ---------- Configuration ----------
  
  /**
   * Set the page size.
   */
  public void setPageSize(int page_size) {
    synchronized (lock_object) {
      this.page_size = page_size;
    }
  }
  
  /**
   * The page size of data read by the buffer manager when
   * accessing information in the database file(s).
   * <p>
   * Default is 8KB = (8 * 1024).
   */
  public int getPageSize() {
    synchronized (lock_object) {
      return this.page_size;
    }
  }
  
  /**
   * Set the maximum number of pages stored in the cache.
   */
  public void setMaxPageCount(int page_count) {
    synchronized (lock_object) {
      this.max_page_count = page_count;
    }
  }
  
  /**
   * The page size cache (the maximum number of pages that can be held by the
   * buffer manager).
   * <p>
   * Default is 1024.
   */
  public int getMaxPageCount() {
    synchronized (lock_object) {
      return this.max_page_count;
    }
  }

  /**
   * Sets the file rollover size in bytes.
   */
  public void setFileRolloverSize(long size) {
    synchronized (lock_object) {
      this.file_rollover_size = size;
    }
  }

  /**
   * The file size rollover value (the number of bytes stored in a data file
   * before expansion of the file is rolled over into a new file).
   * <p>
   * Default is 512MB = (512 * 1024 * 1024)
   */
  public long getFileRolloverSize() {
    synchronized (lock_object) {
      return this.file_rollover_size;
    }
  }
  
  /**
   * Sets the maximum number of pointers in a branch element (must be an even
   * value > 6).
   */
  public void setBranchNodeSize(int branch_size) {
    synchronized (lock_object) {
      this.branch_node_size = branch_size;
    }
  }
  
  /**
   * The size of a branch node (the maximum number of pointers on a branch
   * node).
   * <p>
   * Default is 16.
   */
  public int getBranchNodeSize() {
    synchronized (lock_object) {
      return this.branch_node_size;
    }
  }
  
  /**
   * Sets the maximum number of bytes that can be stored in a leaf element in
   * the tree.
   */
  public void setLeafNodeSize(int leaf_size) {
    synchronized (lock_object) {
      this.leaf_node_size = leaf_size;
    }
  }
  
  /**
   * The size of a leaf node (the maximum number of bytes of data that can be
   * stored on a leaf node).
   * <p>
   * Default is 4010.
   */
  public int getLeafNodeSize() {
    synchronized (lock_object) {
      return this.leaf_node_size;
    }
  }

  /**
   * Sets the size, in bytes, of all nodes stored on the heap during write
   * operations before nodes are flushed to the backing store.
   */
  public void setHeapNodeCacheSize(int heap_node_cache_size) {
    synchronized (lock_object) {
      this.heap_node_cache_size = heap_node_cache_size;
    }
  }
  
  /**
   * The size, in bytes, of all nodes stored on the heap during write
   * operations before nodes are flushed to the backing store.
   * <p>
   * Default is 14MB = (14 * 1024 * 1024).
   */
  public long getHeapNodeCacheSize() {
    synchronized (lock_object) {
      return this.heap_node_cache_size;
    }
  }

  /**
   * Sets the size of the cache for storing branch nodes.
   */
  public void setBranchNodeCacheSize(int branch_node_cache_size) {
    synchronized (lock_object) {
      this.branch_node_cache_size = branch_node_cache_size;
    }
  }
  
  /**
   * The maximum heap size of branch nodes stored in the cache.
   * <p>
   * Default is 2MB = (2 * 1024 * 1024).
   */
  public long getBranchNodeCacheSize() {
    synchronized (lock_object) {
      return this.branch_node_cache_size;
    }
  }

  // ---------- Operation ----------
  
//  /**
//   * Convenience for copying the entire set of keys in the given transaction
//   * to the destination transaction.
//   * <p>
//   * This can be used to implement a full database replication/backup
//   * procedure.
//   */
//  public static void copyAllKeyData(KeyObjectTransaction source_t,
//                                    KeyObjectTransaction destination_t)
//                                                          throws IOException {
//
//    // The transaction in this object,
//    TreeSystemTransaction sourcet = (TreeSystemTransaction) source_t;
//    Iterator<Key> all_keys = sourcet.allKeys();
//    // For each key
//    while (all_keys.hasNext()) {
//      Key key = all_keys.next();
//      // Get the source and destination files
//      DataFile source_file = sourcet.getDataFile(key, 'w');
//      DataFile dest_file = destination_t.getDataFile(key, 'w');
//      // Copy the data
////      source_file.copyTo(dest_file, source_file.size());
//      source_file.replicateTo(dest_file);
//    }
//
//  }

  /**
   * Initializes the storage objects and returns from this method when the
   * data system is in an initialized state and ready to accept transaction
   * requests.  Throws IOException if initialization failed due to a data
   * integrity error.
   * <p>
   * Returns true if the start operation succeeded, false if the database
   * already started and initialized.
   */
  public boolean start() throws IOException {

    synchronized (lock_object) {
      // We can't start a database that is already started,
      if (database_started || database_ob != null) {
        return false;
      }

      // Make a data.koi file with a single TreeSystem structure mapped into it
      String file_ext = "koi";
      String db_file_name = "data";

      debug = new DefaultDebugLogger();
      debug.setDebugLevel(1000000);
      buffer_manager = new LoggingBufferManager(
            path, path, false, max_page_count, page_size, file_ext,
            file_rollover_size, debug, true);
      buffer_manager.start();

      // The backing store
      file_store =
                 new JournalledFileStore(db_file_name, buffer_manager, false);
      file_store.open();
      
      // The actual database
      StoreBackedTreeSystem tree_store;
      
      // Get the header area
      Area header_area = file_store.getArea(-1);
      int magic_value = header_area.getInt();
      // If header area magic value is zero, then we assume this is a brand
      // new database and initialize it with the configuration information
      // given.
      if (magic_value == 0) {
        // Create a tree store inside the file store,
        tree_store = new StoreBackedTreeSystem(
                               file_store, branch_node_size, leaf_node_size,
                               heap_node_cache_size, branch_node_cache_size);
        // Create the tree and returns a pointer to the tree,
        long tree_pointer = tree_store.create();

        // Create an area object with state information about the tree
        AreaWriter awriter = file_store.createArea(128);
        awriter.putInt(0x0101);    // The version value
        awriter.putLong(tree_pointer);
        awriter.putInt(branch_node_size);
        awriter.putInt(leaf_node_size);
        awriter.finish();
        awriter.getID();
        MutableArea harea = file_store.getMutableArea(-1);
        harea.putInt(0x092BA001);  // The magic value
        harea.putLong(awriter.getID());
        harea.checkOut();
      }
      else if (magic_value == 0x092BA001) {
        long apointer = header_area.getLong();
        // The area that contains configuration details,
        Area init_area = file_store.getArea(apointer);
        int version = init_area.getInt();
        if (version != 0x0101) {
          throw new IOException("Unknown version in tree initialization area");
        }
        // Read the pointer to the tree store
        long tree_pointer = init_area.getLong();
        // Read the branch and leaf node sizes as set when the database was
        // created.
        int IBRANCH_NODE_SIZE = init_area.getInt();
        int ILEAF_NODE_SIZE = init_area.getInt();

        // Create the tree store
        tree_store =
            new StoreBackedTreeSystem(
                           file_store, IBRANCH_NODE_SIZE, ILEAF_NODE_SIZE,
                           heap_node_cache_size, branch_node_cache_size);
        // Initialize the tree
        tree_store.init(tree_pointer);

      }
      else {
        throw new IOException("Data is corrupt, invalid magic value in store");
      }

      // Set the point of the tree store
      tree_store.checkPoint();

      // Set up final internal state and return true
      database_ob = tree_store;
      database_started = true;
      return true;
    }
  }

  /**
   * Stops the storage objects and returns when the data storage system has
   * cleanly shut down.
   */
  public void stop() throws IOException {
    synchronized (lock_object) {
      // We can't stop a database that hasn't started
      if (!database_started || database_ob == null) {
        return;
      }

      // Check point before we stop
      checkPoint();

      // Close the store
      file_store.close();
      // Stop the buffer manager
      buffer_manager.stop();
      // Offer up all the internal objects to the GC
      buffer_manager = null;
      file_store = null;
      
      // Clear the internal state
      database_ob = null;
      database_started = false;

    }
  }

  // ---------- Public accessors ----------

  /**
   * Returns the underlying StoreBackedTreeSystem object.
   */
  public StoreBackedTreeSystem getTreeSystem() {
    return database_ob;
  }

  /**
   * Generates a diagnostic graph object of the entire database state.
   */
  public TreeReportNode createDiagnosticGraph() throws IOException {
    return database_ob.createDiagnosticGraph();
  }

  // ---------- Implementation from KeyObjectDatabase -----------

  @Override
  public KeyObjectTransaction createTransaction() {
    return database_ob.createTransaction();
  }

  @Override
  public void publish(KeyObjectTransaction transaction) {
    // Enforce the requirement that publish operations are serial
    synchronized (commit_lock) {
      database_ob.commit(transaction);
    }
  }

  @Override
  public void dispose(KeyObjectTransaction transaction) {
    database_ob.dispose(transaction);
  }

  @Override
  public void checkPoint() {
    database_ob.checkPoint();
  }

}
