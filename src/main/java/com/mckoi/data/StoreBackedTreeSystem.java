/**
 * com.mckoi.data.StoreBackedTreeSystem  Nov 26, 2008
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

package com.mckoi.data;

import com.mckoi.store.*;
import com.mckoi.util.Cache;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * An implementation of TreeSystem that is backed by a fully mutable Store
 * implementation (typically a heap or local file system).
 *
 * @author Tobias Downer
 */

public class StoreBackedTreeSystem implements TreeSystem {

  // ---------- Statics ----------

  /**
   * The type identifiers for branch and leaf nodes in the tree.
   */
  private static final short STORE_LEAF_TYPE   = 0x019EC;
  private static final short STORE_BRANCH_TYPE = 0x022EB;

//  /**
//   * The system key for the file that contains all the atomic state.
//   */
//  private static final Key ATOMIC_FILE_KEY = new Key((short) 0x07F81, 0, 1);

  // ---------- For debugging ----------

  /**
   * Pragmatic checks.
   */
  static final boolean PRAGMATIC_CHECKS = false; //true;

  // ---------- Stop condition handling ----------
  
  /**
   * This is set to the error in the case of a VM error or IOException that is
   * a critical stopping condition.
   */
  private volatile CriticalStopError critical_stop_error = null;

  // ---------- Members ----------

  /**
   * Details of each version of this BTree as an array list (VersionInfo).
   */
  private final ArrayList versions;

  /**
   * The maximum size of the per transaction node heap in bytes.
   */
  private final long node_heap_max_size;
  
  /**
   * The store that maps to all nodes stored in the immutible maps.
   */
  private Store node_store;

  /**
   * The maximum number of children allowed in a branch.
   */
  private int max_branch_size;

  /**
   * The maximum size of the leaf nodes in bytes.
   */
  private int max_leaf_byte_size;

  /**
   * A cache for branches.
   */
  private final Cache branch_cache;

//  /**
//   * The cache of atomic data elements.
//   */
//  private HashMap<AtomicKey, AtomicData> atomic_map;

//  /**
//   * The list of all AtomicData objects that updated since the last commit.
//   * This also is the lock under which all atomic operations must occur.
//   */
//  private ArrayList<AtomicData> atomic_data_updates_list;

  // ----- Post initialization/create vars -----

  /**
   * True if the tree is initialized.
   */
  private boolean initialized;

  /**
   * The header id.
   */
  private long header_id;

  // ----- Locks -----

  /**
   * A lock used when a reference count is accessed or updated in a leaf.
   */
  private final Object REFERENCE_COUNT_LOCK = new Object();
  


  /**
   * Constructs the tree store over the given Store object.  Assumes that the
   * store is initialized and open.
   * <p>
   * @paran node_store the backing Store
   * @param max_branch_children the maximum number of children per branch
   *   (must be multiple of 2)
   * @param max_leaf_size the maximum number of bytes stores in the leaf nodes
   * @param node_max_cache_memory the maximum amount of heap space that can
   *   be allocated for storing temporary nodes per transaction.
   * @param branch_cache_memory the maximum amount of heap space that can
   *   be allocated for storing branch nodes.
   */
  public StoreBackedTreeSystem(Store node_store,
                   int max_branch_children, int max_leaf_size,
                   long node_max_cache_memory,
                   long branch_cache_memory) {

    this.max_branch_size = max_branch_children;
    this.max_leaf_byte_size = max_leaf_size;
    this.node_store = node_store;
    this.node_heap_max_size = node_max_cache_memory;
    this.versions = new ArrayList();
    
    // Allocate some values for the branch cache,
    long branch_size_estimate = (max_branch_children * 24) + 64;
    // The number of elements in the branch cache
    int branch_cache_elements =
                           (int) (branch_cache_memory / branch_size_estimate);
    // Find a close prime to this
    int branch_prime = Cache.closestPrime(branch_cache_elements + 20);
    // Allocate the cache
    this.branch_cache = new Cache(branch_prime, branch_cache_elements, 20);

//    this.atomic_map = new HashMap(123);
//    this.atomic_data_updates_list = new ArrayList();

    initialized = false;
    if (PRAGMATIC_CHECKS) {
      System.out.println("@@ TreeSystem.PRAGMATIC_CHECKS = true");
    }
  }

  /**
   * Creates a tree system and returns a reference to the header node of the
   * structure, which does not change.
   */
  public long create() throws IOException {
    if (initialized) {
      throw new RuntimeException("This tree store is already initialized.");
    }

    // Temporary node heap for creating a starting database
    TreeNodeHeap node_heap = new TreeNodeHeap(17, 4 * 1024 * 1024);
    
    // Write a root node to the store,
    // Create an empty head node
    TreeLeaf head_leaf = node_heap.createEmptyLeaf(null, Key.HEAD_KEY, 256);
    // Insert a tree identification pattern
    head_leaf.put(0, new byte[] { 1, 1, 1, 1 }, 0, 4);
    // Create an empty tail node
    TreeLeaf tail_leaf = node_heap.createEmptyLeaf(null, Key.TAIL_KEY, 256);
    // Insert a tree identification pattern
    tail_leaf.put(0, new byte[] { 1, 1, 1, 1 }, 0, 4);

    // The write sequence,
    TreeWriteSequence seq = new TreeWriteSequence();
    seq.sequenceNodeWrite(head_leaf);
    seq.sequenceNodeWrite(tail_leaf);
    NodeReference[] refs = performTreeWrite(seq);

    // Create a branch,
    TreeBranch root_branch =
                       node_heap.createEmptyBranch(null, getMaxBranchSize());
    root_branch.set(refs[0], 4, Key.TAIL_KEY, refs[1], 4);

    seq = new TreeWriteSequence();
    seq.sequenceNodeWrite(root_branch);
    refs = performTreeWrite(seq);

    // The written root node reference,
    final NodeReference root_id = refs[0];

//    // Write the nodes,
//    final long head_id = writeNode(head_leaf);
//    final long tail_id = writeNode(tail_leaf);
//
//    // Create a branch,
//    TreeBranch root_branch =
//                       node_heap.createEmptyBranch(null, getMaxBranchSize());
//    root_branch.set(head_id, 4,
//                    Key.TAIL_KEY.encodedValue(1), Key.TAIL_KEY.encodedValue(2),
//                    tail_id, 4);
//
//    // Write the branch,
//    final long root_id = writeNode(root_branch);

    // Delete the head and tail leaf, and the root branch
    node_heap.delete(head_leaf.getReference());
    node_heap.delete(tail_leaf.getReference());
    node_heap.delete(root_branch.getReference());

    // Write this version info to the store,
    final long version_id = writeSingleVersionInfo(1, root_id, new ArrayList(0));

    // Make a first version
    VersionInfo version_info = new VersionInfo(1, root_id, version_id);
    versions.add(version_info);

    // Flush this to the version list
    AreaWriter version_list = node_store.createArea(64);
    version_list.putInt(0x01433);
    version_list.putInt(1);
    version_list.putLong(version_id);
    version_list.finish();
    // Get the versions id
    final long version_list_id = version_list.getID();

    // The final header
    AreaWriter header = node_store.createArea(64);
    header.putInt(0x09391);   // The magic value,
    header.putInt(1);         // The version
    header.putLong(version_list_id);
    header.finish();

    // Set up the internal variables,
    header_id = header.getID();

    initialized = true;
    // And return the header reference
    return header_id;
  }

  /**
   * Initializes the tree store.
   */
  public void init(long header_id) throws IOException {
    if (initialized) {
      throw new RuntimeException("This tree store is already initialized.");
    }

    // Set the header id
    this.header_id = header_id;

    // Get the header area
    Area header_area = node_store.getArea(header_id);
    header_area.position(8);
    // Read the versions list,
    long version_list_id = header_area.getLong();

    // Read the versions list area
    // magic(int), versions count(int), list of version id objects.
    Area versions_area = node_store.getArea(version_list_id);
    if (versions_area.getInt() != 0x01433) {
      throw new IOException("Incorrect magic value 0x01433");
    }
    int vers_count = versions_area.getInt();
    // For each id from the versions area, read in the associated VersionInfo
    // object into the 'vers' array.
    VersionInfo[] vers = new VersionInfo[vers_count];
    for (int i = 0; i < vers_count; ++i) {
      vers[i] = readSingleVersionInfo(versions_area.getLong());
    }

    // Set up the versions object
    for (int i = 0; i < vers_count; ++i) {
      versions.add(vers[i]);
    }
    // If more than two uncomitted versions, dispose them
    if (versions.size() > 2) {
      disposeOldVersions();
    }

    initialized = true;
    
//    // Create a transaction and read the atomic information from it.
//    TreeSystemTransaction transaction = createTransaction();
//    try {
//      // Read the atomic data objects map
//      DataFile atomic_file =
//                      transaction.unsafeGetDataFile(ATOMIC_FILE_KEY, 'r');
//      // The atomic item set
//      AtomicDataSerialSet atomic_set = new AtomicDataSerialSet(atomic_file);
//      int sz = (int) atomic_set.size();
//      for (int i = 0; i < sz; ++i) {
//        AtomicData adata = atomic_set.createAtomicData(i);
//        atomic_map.put(adata.getKey(), adata);
//      }
//    }
//    finally {
//      // Make sure we dispose the transaction,
//      dispose(transaction);
//    }

  }

  /**
   * Converts a 64 bit value into a NodeReference object for this type of
   * store. Note that this means store backed tree systems are limited to
   * 64-bit address space (a limitation inherited from the store package).
   */
  private NodeReference from64bitStoreAddress(long ref64bit) {
    return new NodeReference(0, ref64bit);
  }

  /**
   * Converts an 128 bit NodeReference object into a 64-bit value used to
   * reference objects from the store implementation. Note that this means
   * store backed tree systems are limited to 64-bit address space (a
   * limitation inherited from the store package).
   */
  private long to64bitStoreAddress(NodeReference node_ref) {
    long ref64bit = node_ref.getLowLong();
    return ref64bit;
  }

  /**
   * Writes out a single version info.
   */
  private long writeSingleVersionInfo(long version_id,
                    NodeReference root_node_ref,
                    ArrayList<NodeReference> deleted_refs) throws IOException {

    int deleted_ref_count = deleted_refs.size();

    // Write the version info and the deleted refs to a new area,
    AreaWriter writer =
         node_store.createArea(4 + 4 + 8 + 8 + 8 + 4 + (deleted_ref_count * 16));
    writer.putInt(0x04EA23);
    writer.putInt(1);
    writer.putLong(version_id);
//    writer.putLong(root_node_ref);
    writer.putLong(root_node_ref.getHighLong());
    writer.putLong(root_node_ref.getLowLong());

    writer.putInt(deleted_ref_count);
    for (int i = 0; i < deleted_ref_count; ++i) {
      NodeReference deleted_node = deleted_refs.get(i);
//      writer.putLong(deleted_node);
      writer.putLong(deleted_node.getHighLong());
      writer.putLong(deleted_node.getLowLong());
    }
    writer.finish();

    return writer.getID();
  }

  /**
   * Reads in a single version info.
   */
  private VersionInfo readSingleVersionInfo(long ver_ref)
                                                        throws IOException {
    Area ver_area = node_store.getArea(ver_ref);
    int MAGIC = ver_area.getInt();
    int VERSION = ver_area.getInt();
    long version_id = ver_area.getLong();
//    long root_node_ref = ver_area.getLong();
    long rnr_high = ver_area.getLong();
    long rnr_low = ver_area.getLong();
    NodeReference root_node_ref = new NodeReference(rnr_high, rnr_low);

//    // We don't need to read this information,
//    int deleted_ref_count = ver_area.getInt();
//    LongList deleted_refs = new LongList(deleted_ref_count);
//    for (int i = 0; i < deleted_ref_count; ++i) {
//      deleted_refs.add(ver_area.getLong());
//    }

    if (MAGIC != 0x04EA23) {
      throw new IOException("Incorrect magic value 0x04EA23");
    }
    if (VERSION < 1) {
      throw new IOException("Version incorrect.");
    }

    return new VersionInfo(version_id, root_node_ref, ver_ref);
  }

  /**
   * Adds a new VersionInfo to the versions list.  Returns the reference to the
   * new id record just added.
   */
  private synchronized long writeVersionsList(long version_id,
                                TreeSystemTransaction tran) throws IOException {

    // Write the version info and the deleted refs to a new area,
    NodeReference root_node_ref = tran.getRootNodeRef();
    if (root_node_ref.isInMemory()) {
      throw new RuntimeException("Assertion failed, root_node is on heap.");
    }
//    if (root_node_ref < 0) {
//      throw new RuntimeException("Assertion failed, root_node is on heap.");
//    }

    // Get the list of all nodes deleted in the transaction
    ArrayList<NodeReference> deleted_refs = tran.getAllDeletedNodeRefs();
    if (PRAGMATIC_CHECKS) {
      // Sort it
      Collections.sort(deleted_refs);
      // Check for any duplicate entries (we shouldn't double delete stuff).
      for (int i = 1; i < deleted_refs.size(); ++i) {
        if (deleted_refs.get(i - 1).equals(deleted_refs.get(i))) {
          // Oops, duplicated delete
          throw new RuntimeException(
                 "PRAGMATIC_CHECK failed: duplicate records in delete list.");
        }
      }

    }

    long the_version_id = writeSingleVersionInfo(
                                     version_id, root_node_ref, deleted_refs);

    // Now update the version list by copying the list and adding the new ref
    // to the end.

    // Get the current version list
    MutableArea header_area = node_store.getMutableArea(header_id);
    header_area.position(8);
    final long version_list_id = header_area.getLong();

    // Read information from the old version info,
    Area version_list_area = node_store.getArea(version_list_id);
    version_list_area.getInt();  // The magic
    final int version_count = version_list_area.getInt();

    // Create a new list,
    AreaWriter new_version_list =
                          node_store.createArea(8 + (8 * (version_count + 1)));
    new_version_list.putInt(0x01433);
    new_version_list.putInt(version_count + 1);
    for (int i = 0; i < version_count; ++i) {
      new_version_list.putLong(version_list_area.getLong());
    }
    new_version_list.putLong(the_version_id);
    new_version_list.finish();

    // Write the new area to the header,
    header_area.position(8);
    header_area.putLong(new_version_list.getID());

    // Delete the old version list Area,
    node_store.deleteArea(version_list_id);

    // Done,
    return the_version_id;
  }

  /**
   * Returns the max branch size.
   */
  @Override
  public int getMaxBranchSize() {
    return max_branch_size;
  }

  /**
   * Returns the maximum number of bytes in a leaf.
   */
  @Override
  public int getMaxLeafByteSize() {
    return max_leaf_byte_size;
  }

  /**
   * Creates a new transaction snapshot of the tree for the given version.
   */
  private TreeSystemTransaction createSnapshot(VersionInfo vinfo) {
    return new TreeSystemTransaction(this, vinfo.getVersionID(),
                                    vinfo.getRootNodePointer(), false);
  }

  /**
   * Checks the 'versions' list to determine if we can free up old versions
   * that do not have any references to them.
   */
  private void disposeOldVersions() throws IOException {
    ArrayList dispose_list = new ArrayList();
    synchronized (versions) {
      // size - 1 because we don't want to delete the very last version,
      int sz = versions.size() - 1;
      boolean found_locked_entry = false;
      for (int i = 0; i < sz && found_locked_entry == false; ++i) {
        VersionInfo vinfo = (VersionInfo) versions.get(i);
        // If this version isn't locked,
        if (vinfo.notLocked()) {
          // Add to the dispose list
          dispose_list.add(vinfo);
          // And delete from the versions list,
          versions.remove(i);
          --sz;
          --i;
        }
        else {
          // If it is locked, we exit the loop
          found_locked_entry = true;
        }
      }
    }

    // If there are entries to dispose?
    if (dispose_list.size() > 0) {
      // We synchronize here to ensure the versions list can't be modified by
      // a commit operation while we are disposing this.
      synchronized (this) {
        // Run within a write lock on the store
        try {
          node_store.lockForWrite();

          // First we write out a modified version header minus the versions we
          // are to delete,

          // Get the current version list
          MutableArea header_area = node_store.getMutableArea(header_id);
          header_area.position(8);
          final long version_list_id = header_area.getLong();

          // Read information from the old version info,
          Area version_list_area = node_store.getArea(version_list_id);
          version_list_area.getInt();  // The magic
          final int version_count = version_list_area.getInt();

          final int new_version_count = version_count - dispose_list.size();
          // Create a new list,
          AreaWriter new_version_list =
                            node_store.createArea(8 + (8 * new_version_count));
          new_version_list.putInt(0x01433);
          new_version_list.putInt(new_version_count);
          // Skip the versions we are deleting,
          for (int i = 0; i < dispose_list.size(); ++i) {
            version_list_area.getLong();
          }
          // Now copy the list from the new point
          for (int i = 0; i < new_version_count; ++i) {
            new_version_list.putLong(version_list_area.getLong());
          }
          new_version_list.finish();

          // Write the new area to the header,
          header_area.position(8);
          header_area.putLong(new_version_list.getID());

          // Delete the old version list Area,
          node_store.deleteArea(version_list_id);

          // Dispose the version info,
          int sz = dispose_list.size();
          for (int i = 0; i < sz; ++i) {
            VersionInfo vinfo = (VersionInfo) dispose_list.get(i);
            long v_ref = vinfo.version_info_ref;
            Area version_area = node_store.getArea(v_ref);
            int magic = version_area.getInt();
            int rev = version_area.getInt();
            // Check the magic,
            if (magic != 0x04EA23) {
              throw new RuntimeException(
                                "Magic value for version area is incorrect.");
            }
            long ver_id = version_area.getLong();
//            long root_ref = version_area.getLong();
            long nrn_high = version_area.getLong();
            long nrn_low = version_area.getLong();

            int node_count = version_area.getInt();
            // For each node,
            for (int n = 0; n < node_count; ++n) {
              // Read the next area
//              long del_node_ref = version_area.getLong();
              long drn_high = version_area.getLong();
              long drn_low = version_area.getLong();
              NodeReference del_node_ref = new NodeReference(drn_high, drn_low);
              // Cleanly disposes the node
              doDisposeNode(del_node_ref);
//              // This node may be a branch, so remove from the branch cache,
//              synchronized (branch_cache) {
//                branch_cache.remove(new Long(del_node_ref));
//              }
//              // And delete it
//              node_store.deleteArea(del_node_ref);
            }

            // Delete the node header,
            node_store.deleteArea(v_ref);

          }
        }
        finally {
          node_store.unlockForWrite();
        }
      }
    }
  }

  /**
   * Unlocks the given transaction_id version.  Once a transaction has no
   * locks established on it then it is available for reclaimation.
   */
  private void unlockTransaction(long version_id) {
    boolean done = false;
    synchronized (versions) {
      int sz = versions.size();
      for (int i = sz - 1; i >= 0 && done == false; --i) {
        VersionInfo vinfo = (VersionInfo) versions.get(i);
        if (vinfo.getVersionID() == version_id) {
          // Unlock this version,
          vinfo.unlock();
          // And finish,
          done = true;
        }
      }
    }
    if (!done) {
      throw new RuntimeException(
                            "Unable to find version to unlock: " + version_id);
    }
  }

  // ----- Critical stop error handling -----

  /**
   * Checks if the database is in a stop state, if it is throws the stop state
   * exception.
   */
  @Override
  public final void checkCriticalStop() {
    if (critical_stop_error != null) {
      // We wrap the critical stop error a second time to ensure the stack
      // trace accurately reflects where the failure originated.
      throw new CriticalStopError(
              critical_stop_error.getMessage(), critical_stop_error);
    }
  }
 
  /**
   * Called by an exception handler when an IOException is generated, most
   * typically this is a stopping condition that stops all access to the
   * database immediately.
   */
  @Override
  public final CriticalStopError handleIOException(IOException e) {
    critical_stop_error = new CriticalStopError(e.getMessage(), e);
    throw critical_stop_error;
  }

  /**
   * Called by an exception handler when a VirtualMachineError is generated,
   * most typically an OutOfMemoryError.  A caught VirtualMachineError
   * causes the database to enter a critical stop state.
   */
  @Override
  public final CriticalStopError handleVMError(VirtualMachineError e) {
    critical_stop_error = new CriticalStopError(e.getMessage(), e);
    throw critical_stop_error;
  }

  // ----- Public methods -----
  
  /**
   * Returns an object that represents the most current view of the database
   * and allows read and write operations on the data including creating new
   * key/data entries and modifying and removing existing entries.  The
   * returned object is <b>NOT</b> thread safe and may have various restrictions
   * on its use depending on the implementation of children classes.
   * <p>
   * It is intended this is used during the commit process to which the current
   * snapshot is updated and committed.
   */
  public TreeSystemTransaction createTransaction() {
    checkCriticalStop();
    try {
      // Returns the latest snapshot (the snapshot at the end of the versions
      // list)
      VersionInfo info;
      synchronized (versions) {
        info = (VersionInfo) versions.get(versions.size() - 1);
        info.lock();
      }
      return createSnapshot(info);
    }
    catch (VirtualMachineError e) {
      // A virtual machine error most often means the VM ran out of memory,
      // which represents a critical state that causes immediate cleanup.
      throw handleVMError(e);
    }
  }

  /**
   * Commits the transaction by making the given transaction the most current
   * view of the database.  The given TreeSystemTransaction object must be
   * based on the most recent version committed otherwise this operation will
   * fail.  For example, it's not possible to keep hold of a very old version
   * and commit changes on the old version with this method.
   * <p>
   * If it is necessary to commit changes from an old version then the various
   * key merge facilities should be used.  Operations such as multi version
   * index merges can not be represented in a general way and therefore the
   * functionality of version merges must be implemented over the top of this
   * model.
   */
  public void commit(KeyObjectTransaction tran) {
    checkCriticalStop();
    try {
      TreeSystemTransaction transaction = (TreeSystemTransaction) tran;
      VersionInfo top_version;
      synchronized (versions) {
        top_version = (VersionInfo) versions.get(versions.size() - 1);
      }
      // Check the version is based on the must current transaction,
      if (transaction.getVersionID() != top_version.getVersionID()) {
        // ID not the same as the top version, so throw the exception
        throw new RuntimeException("Can't commit non-sequential version.");
      }

//      // Make sure any changes to the atomic data items are written out now.
//      synchronized (atomic_data_updates_list) {
//        int sz = atomic_data_updates_list.size();
//        if (sz > 0) {
//          AtomicDataSerialSet atomic_data_set = new AtomicDataSerialSet(
//                       transaction.unsafeGetDataFile(ATOMIC_FILE_KEY, 'w'));
//          // Update all the atomic items that changed
//          for (int i = 0; i < sz; ++i) {
//            AtomicData adata = atomic_data_updates_list.get(i);
//            atomic_data_set.setAtomicData(adata.getKey(), adata);
//          }
//          // Clear the lists
//          atomic_data_updates_list.clear();
//        }
//      }

      // Make sure the transaction is written to the store,
      // NOTE: This MUST happen outside a node store lock otherwise checking
      //   out on the cache manage function could lock up the thread
      transaction.checkOut();

      try {
        node_store.lockForWrite();

        // The new version number,
        long new_version_num = top_version.getVersionID() + 1;

        // Write out the versions list to the store,
        long version_record_id = writeVersionsList(new_version_num, transaction);
        // Create a new VersionInfo object with a new id,
        VersionInfo new_vinfo = new VersionInfo(new_version_num,
                                                transaction.getRootNodeRef(),
                                                version_record_id);
        synchronized (versions) {
          // Add this version to the end of the versions list,
          versions.add(new_vinfo);
        }

      }
      finally {
        node_store.unlockForWrite();
      }

      // Notify the transaction is committed,
      // This will stop the transaction from cleaning up newly added nodes.
      transaction.notifyCommitted();
    }
    catch (IOException e) {
      // An IOException during this block represents a critical stopping
      // condition.
      throw handleIOException(e);
    }
    catch (VirtualMachineError e) {
      // A virtual machine error most often means the VM ran out of memory,
      // which also represents a critical state that causes immediate cleanup.
      throw handleVMError(e);
    }
  }

  /**
   * Disposes a transaction.  If the transaction was not committed with the
   * 'commit' method above, then all new nodes created in the transaction will
   * be immediately deleted.  If the transaction was committed with the 'commit'
   * method above, the transaction is marked 'out of scope' and the tree store
   * may decide to delete all resources in the store associated with the
   * transaction.
   */
  public void dispose(KeyObjectTransaction tran) {
    checkCriticalStop();
    try {
      TreeSystemTransaction transaction = (TreeSystemTransaction) tran;
      // Get the version id of the transaction,
      long version_id = transaction.getVersionID();
      // Call the dispose method,
      transaction.dispose();
      // Reduce the lock count for this version id,
      unlockTransaction(version_id);
      // Check if we can clear up old versions,
      disposeOldVersions();
    }
    catch (IOException e) {
      // An IOException during this block represents a critical stopping
      // condition.
      throw handleIOException(e);
    }
    catch (VirtualMachineError e) {
      // A virtual machine error most often means the VM ran out of memory,
      // which also represents a critical state that causes immediate cleanup.
      throw handleVMError(e);
    }
  }

  /**
   * Check points all the updates to the tree up to this point provided the
   * underlying store implementation supports check points.  This method
   * provides some guarantee that information written to the tree is
   * consistant over multiple invocations.
   * <p>
   * It is intended for check points to happen immediately after a commit, but
   * it is not necessary and check points can be made at any time.
   */
  @Override
  public void checkPoint() {
    checkCriticalStop();
    try {
      try {
        node_store.checkPoint();
      }
      catch (InterruptedException e) {
        throw new Error("Interrupted", e);
      }
    }
    catch (IOException e) {
      // An IOException during this block represents a critical stop
      // condition.
      throw handleIOException(e);
    }
    catch (VirtualMachineError e) {
      // A virtual machine error most often means the VM ran out of memory,
      // which also represents a critical state that causes immediate cleanup.
      throw handleVMError(e);
    }
  }

  /**
   * Outputs debugging status information about the current state of this
   * object.  The information can be generated fast and is used to
   * inspect the state of this object, helping to find any hanging locks
   * from transactions that weren't disposed.
   */
  public void printSystemStatus(PrintWriter out) throws IOException {
    checkCriticalStop();
    synchronized (versions) {
      int sz = versions.size();
      out.println("Active version list and locks;");
      for (int i = 0; i < sz; ++i) {
        VersionInfo ver_info = (VersionInfo) versions.get(i);
        int lock_count = ver_info.lock_count;
        long ver_id = ver_info.version_id;
        out.print("lock(");
        out.print(ver_id);
        out.print(").lock_count = ");
        out.println(lock_count);
      }
    }
  }

  /**
   * Debugging/analysis method that walks through the entire tree and generates
   * a graph of every area reference in the store that is touched by this tree
   * store.  Each tree node contains properties about the area.  Each node
   * always contains a property 'ref' which is the Long reference to the
   * area.
   * <p>
   * This walk will only produce details of the information within the store
   * structure and will not report any temporary in-memory modifications.
   * Behaviour of this method is undefined if any operations occur on the tree
   * while the tree walk is in progress.  This function is not intended to be
   * used while the tree is 'live'.
   * <p>
   * The graph may contain entries that point to the same reference.  For
   * example, each version is represented by its own graph and each version
   * tree usually shares elements.  This produces a graph that usually appears
   * to be larger than the number of areas actually in the tree.
   * <p>
   * Typically, the graph will contain a number of version nodes, each version
   * node will contain a node with children of all nodes deleted from the
   * previous version.  Each version node also graphs to the root node for the
   * tree represented by that version.
   * <p>
   * NOTE: This method is <b>NOT</b> safe to use when other concurrent
   *   operations are accessing the database.
   */
  public TreeReportNode createDiagnosticGraph() throws IOException {
    checkCriticalStop();

    // Create the header node
    TreeReportNode header_node = new TreeReportNode("header", header_id);

    // Get the header area
    Area header_area = node_store.getArea(header_id);
    header_area.position(8);
    // Read the versions list,
    long version_list_ref = header_area.getLong();

    // Create the version node
    TreeReportNode versions_node =
                    new TreeReportNode("versions list", version_list_ref);
    // Set this as a child to the header
    header_node.addChild(versions_node);

    // Read the versions list area
    // magic(int), versions count(int), list of version id objects.
    Area versions_area = node_store.getArea(version_list_ref);
    if (versions_area.getInt() != 0x01433) {
      throw new IOException("Incorrect magic value 0x01433");
    }
    int vers_count = versions_area.getInt();
    // For each id from the versions area, read in the associated VersionInfo
    // object into the 'vers' array.
    for (int i = 0; i < vers_count; ++i) {
      long v_info_ref = versions_area.getLong();
      // Set up the information in our node
      TreeReportNode v_info_node = new TreeReportNode("version", v_info_ref);

      // Read in the version information node
      Area v_info_area = node_store.getArea(v_info_ref);
      int MAGIC = v_info_area.getInt();
      int VER = v_info_area.getInt();
      long version_id = v_info_area.getLong();
//      long root_node_ref = v_info_area.getLong();
      long rnr_high = v_info_area.getLong();
      long rnr_low = v_info_area.getLong();
      NodeReference root_node_ref = new NodeReference(rnr_high, rnr_low);

      v_info_node.setProperty("MAGIC", MAGIC);
      v_info_node.setProperty("VER", VER);
      v_info_node.setProperty("version_id", version_id);
      // Make the deleted area list into a property
      int deleted_area_count = v_info_area.getInt();
      if (deleted_area_count > 0) {
        for (int n = 0; n < deleted_area_count; ++n) {
//          long del_node_ref = v_info_area.getLong();
          long deln_high = v_info_area.getLong();
          long deln_low = v_info_area.getLong();
          NodeReference del_node_ref = new NodeReference(deln_high, deln_low);
          v_info_node.addChild(new TreeReportNode("delete", del_node_ref));
        }

//        StringBuilder strbuf = new StringBuilder();
//        strbuf.append(new Long(v_info_area.getLong()).toString());
//        for (int n = 1; n < deleted_area_count; ++n) {
//          strbuf.append(",");
//          strbuf.append(new Long(v_info_area.getLong()).toString());
//        }
//        v_info_node.setProperty("REFLIST.deleted_refs", strbuf.toString());
      }

      // Add the child node (the root node of the version graph).
      v_info_node.addChild(createDiagnosticRootGraph(Key.HEAD_KEY, root_node_ref));

      // Add this to the version list node
      versions_node.addChild(v_info_node);
    }

    // Return the header node
    return header_node;

  }

  /**
   * Walks the tree from the given node returning a graph the contains
   * basic property information about the nodes.
   */
  private TreeReportNode createDiagnosticRootGraph(
                          Key left_key, NodeReference ref) throws IOException {

    // The node being returned
    TreeReportNode node;

    // Open the area
    Area area = node_store.getArea(to64bitStoreAddress(ref));
    // What type of node is this?
    short node_type = area.getShort();
    // The version
    short VER = area.getShort();
    if (node_type == STORE_LEAF_TYPE) {
      // Read the reference count,
      long ref_count = area.getInt();
      // The number of bytes in the leaf
      int leaf_size = area.getInt();

      // Set up the leaf node object
      node = new TreeReportNode("leaf", ref);
      node.setProperty("VER", VER);
      node.setProperty("key", left_key.toString());
      node.setProperty("reference_count", ref_count);
      node.setProperty("leaf_size", leaf_size);

    }
    else if (node_type == STORE_BRANCH_TYPE) {
      // The data size area containing the children information
      int child_data_size = area.getInt();
      long[] data_arr = new long[child_data_size];
      for (int i = 0; i < child_data_size; ++i) {
        data_arr[i] = area.getLong();
      }
      // Create the TreeBranch object to query it
      TreeBranch branch = new TreeBranch(ref, data_arr, child_data_size);
      // Set up the branch node object
      node = new TreeReportNode("branch", ref);
      node.setProperty("VER", VER);
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
      throw new IOException("Unknown node type: " + node_type);
    }

    return node;
  }

  // ---------- Store methods ----------

  /**
   * Returns the maximum size of the local transaction node heaps.
   */
  @Override
  public long getNodeHeapMaxSize() {
    return node_heap_max_size;
  }

//  /**
//   * Returns the AtomicData object assigned with the given identifier.
//   */
//  public AtomicData getAtomicData(AtomicKey atomic_id) {
//    // If it's in the cache, return it,
//    synchronized (atomic_data_updates_list) {
//      AtomicData atomic_element = atomic_map.get(atomic_id);
//      if (atomic_element == null) {
//        // Create it now
//        atomic_element = new TSAtomicData(atomic_id);
//        // Put it into the map
//        atomic_map.put(atomic_id, atomic_element);
//      }
//      return atomic_element;
//    }
//  }
//
//  /**
//   * Removes the atomic data record assigned with the given identifier.
//   */
//  public void removeAtomicData(AtomicKey atomic_id) {
//    synchronized (atomic_data_updates_list) {
//      // Setting the atomic value to zero will reclaim the resources for the
//      // item at the next commit.
//      AtomicData data = getAtomicData(atomic_id);
//      data.setValue(BigInteger.ZERO);
//    }
//  }

  /**
   * Returns a special static node (sparse nodes, etc).
   */
  public static TreeNode specialStaticNode(NodeReference node_ref) {
    return node_ref.createSpecialTreeNode();
  }

  /**
   * Fetches an immutable node kept in the tree at the given node reference.
   */
  private TreeNode fetchNode(NodeReference node_ref) throws IOException {
    // Is it a special static node?
    if (node_ref.isSpecial()) {
      return specialStaticNode(node_ref);
    }

    // Is this a branch node in the cache?
//    Long cache_key = new Long(node_ref);
    final NodeReference cache_key = node_ref;
    TreeBranch branch;
    synchronized (branch_cache) {
      branch = (TreeBranch) branch_cache.get(cache_key);
      if (branch != null) {
        return branch;
      }
    }

    // Not found in the cache, so fetch the area from the backing store and
    // create the node type.

    // Get the area for the node
    Area node_area = node_store.getArea(to64bitStoreAddress(node_ref));
    // Wrap around a buffered DataInputStream for reading values from the
    // store.
    DataInputStream in =
                 new DataInputStream(new AreaInputStream(node_area, 256));

    short node_type = in.readShort();
    // Is the node type a leaf node?
    if (node_type == STORE_LEAF_TYPE) {
      // Read the key
      in.readShort();  // version
      in.readInt();   // reference count
//      int secondary_key = in.readInt();
//      short type = (short) in.readInt();
//      long primary_key = in.readLong();
//      Key key = new Key(type, secondary_key, primary_key);
      int leaf_size = in.readInt();

      // Return a leaf that's mapped to the data in the store
      node_area.position(0);
      return new AreaTreeLeaf(node_ref, leaf_size, node_area);
    }
    // Is the node type a branch node?
    else if (node_type == STORE_BRANCH_TYPE) {
      // Note that the entire branch is loaded into memory now,
      in.readShort();  // version
      int child_data_size = in.readInt();
      long[] data_arr = new long[child_data_size];
      for (int i = 0; i < child_data_size; ++i) {
        data_arr[i] = in.readLong();
      }
      branch = new TreeBranch(node_ref, data_arr, child_data_size);
      // Put this branch in the cache,
      synchronized (branch_cache) {
        branch_cache.put(cache_key, branch);
        // And return the branch
        return branch;
      }
    }
    else {
      throw new RuntimeException("Unknown node type: " + node_type);
    }

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TreeNode[] fetchNode(NodeReference[] node_refs) throws IOException {
    int sz = node_refs.length;
    TreeNode[] node_results = new TreeNode[sz];
    for (int i = 0; i < sz; ++i) {
      node_results[i] = fetchNode(node_refs[i]);
    }
    return node_results;
  }

  /**
   * {@inhericDoc}
   */
  @Override
  public boolean isNodeAvailableLocally(NodeReference node_ref) {
    // Special node ref,
    if (node_ref.isSpecial()) {
      return true;
    }
    // Otherwise return true (all data for store backed tree systems is local),
    return true;
  }




//  /**
//   * Writes a heap node out to the store and returns a reference to the node in
//   * the store.  It is the responsibility of callee to update any references
//   * to this node in the tree.
//   */
//  private long writeNode(TreeNode node) throws IOException {
//
//    try {
//      node_store.lockForWrite();
//
//      // Make sure the branch is on the heap,
//      long node_ref = node.getReference();
//      if (node_ref >= 0) {
//        throw new RuntimeException("Can't write a node that isn't on the heap.");
//      }
//
//      if (node instanceof TreeBranch) {
//        TreeBranch branch = (TreeBranch) node;
//        // The number of children
//        int sz = branch.size();
//        // Assert that we aren't writing a branch that has children still on the
//        // heap, or key references to records on the heap either.
//        for (int i = 0; i < sz; ++i) {
//          long child_ref = branch.getChild(i);
//          if (child_ref < 0) {
//            throw new RuntimeException(
//                                 "Branch contains a child that is on the heap.");
//          }
//        }
//
//        // Write out the branch to the store
//        long[] node_data = branch.getNodeData();
//        int ndsz = branch.getNodeDataSize();
//        AreaWriter writer = node_store.createArea(4 + 4 + (ndsz * 8));
//        writer.putShort(STORE_BRANCH_TYPE);
//        writer.putShort((short) 1);  // version
//        writer.putInt(ndsz);
//        for (int i = 0; i < ndsz; ++i) {
//          writer.putLong(node_data[i]);
//        }
//        writer.finish();
//        // Get the id,
//        long node_id = writer.getID();
//
//        // Make this into a branch node and add to the cache,
//        branch = new TreeBranch(node_id, node_data, ndsz);
//        // Put this branch in the cache,
//        synchronized (branch_cache) {
//          branch_cache.put(new Long(node_id), branch);
//        }
//
//        // Return the id,
//        return node_id;
//
//      }
//      else {
//
//        // Otherwise it's a leaf node,
//        TreeLeaf leaf = (TreeLeaf) node;
//        int sz = leaf.getSize();
//
//        AreaWriter writer = node_store.createArea(12 + sz);
//        writer.putShort(STORE_LEAF_TYPE);
//        writer.putShort((short) 1);  // version
//        writer.putInt(1);            // reference count
//        writer.putInt(leaf.getSize());
//        leaf.writeDataTo(writer);
//        writer.finish();
//
//        // Get and return the id,
//        long node_id = writer.getID();
//        return node_id;
//
//      }
//
//    }
//    finally {
//      node_store.unlockForWrite();
//    }
//  }

  /**
   * Performs the sequence of node write operations described by the given
   * TreeWriteSequence object. This is used to flush a complete tree write
   * operation out to the backing store. Returns an array of node_ref 64-bit
   * values that represent the address of every node written to the backing
   * media on the completion of the process.
   */
  @Override
  public NodeReference[] performTreeWrite(
                               TreeWriteSequence sequence) throws IOException {
    try {
      node_store.lockForWrite();

      List<TreeNode> all_branches = sequence.getAllBranchNodes();
      List<TreeNode> all_leafs = sequence.getAllLeafNodes();
      ArrayList<TreeNode> nodes =
                       new ArrayList(all_branches.size() + all_leafs.size());
      nodes.addAll(all_branches);
      nodes.addAll(all_leafs);

//      List<TreeNode> nodes = sequence.getAllNodes();

      // The list of nodes to be allocated,
      int sz = nodes.size();
      // The list of allocated referenced for the nodes,
      NodeReference[] refs = new NodeReference[sz];
      // The list of area writers,
      AreaWriter[] writers = new AreaWriter[sz];

      // Allocate the space first,
      for (int i = 0; i < sz; ++i) {
        TreeNode node = nodes.get(i);
        // Is it a branch node?
        if (node instanceof TreeBranch) {
          TreeBranch branch = (TreeBranch) node;
          int ndsz = branch.getNodeDataSize();
          writers[i] = node_store.createArea(4 + 4 + (ndsz * 8));
        }
        // Otherwise, it must be a leaf node,
        else {
          TreeLeaf leaf = (TreeLeaf) node;
          int lfsz = leaf.getSize();
          writers[i] = node_store.createArea(12 + lfsz);
        }
        // Set the reference,
        refs[i] = from64bitStoreAddress(writers[i].getID());
      }

      // Now write out the data,
      for (int i = 0; i < sz; ++i) {
        TreeNode node = nodes.get(i);
        // Is it a branch node?
        if (node instanceof TreeBranch) {
          TreeBranch branch = (TreeBranch) node;

          // The number of children
          int chsz = branch.size();
          // For each child, if it's a heap node, look up the child id and
          // reference map in the sequence and set the reference accordingly,
          for (int o = 0; o < chsz; ++o) {
            NodeReference child_ref = branch.getChild(o);
            if (child_ref.isInMemory()) {
              // The ref is currently on the heap, so adjust accordingly
              int ref_id = sequence.lookupRef(i, o);
              branch.setChild(refs[ref_id], o);
            }
          }
          
          // Write out the branch to the store
          long[] node_data = branch.getNodeData();
          int ndsz = branch.getNodeDataSize();

          AreaWriter writer = writers[i];
          writer.putShort(STORE_BRANCH_TYPE);
          writer.putShort((short) 1);  // version
          writer.putInt(ndsz);
          for (int o = 0; o < ndsz; ++o) {
            writer.putLong(node_data[o]);
          }
          writer.finish();

          // Make this into a branch node and add to the cache,
          branch = new TreeBranch(refs[i], node_data, ndsz);
          // Put this branch in the cache,
          synchronized (branch_cache) {
            branch_cache.put(refs[i], branch);
          }

        }
        // Otherwise, it must be a leaf node,
        else {
          TreeLeaf leaf = (TreeLeaf) node;
          AreaWriter writer = writers[i];
          writer.putShort(STORE_LEAF_TYPE);
          writer.putShort((short) 1);  // version
          writer.putInt(1);            // reference count
          writer.putInt(leaf.getSize());
          leaf.writeDataTo(writer);
          writer.finish();
        }
      }

      return refs;

    }
    finally {
      node_store.unlockForWrite();
    }
  }

  /**
   * Links to a leaf incrementing its reference count.  Called when the
   * tree establishes a new reference to the leaf.
   * <p>
   * Returns true if the link was successful, false if the link was not
   * possible because the reference counter overflowed.
   */
  @Override
  public boolean linkLeaf(Key key, NodeReference ref) throws IOException {
    // If the node is a special node, then we don't need to reference count it.
    if (ref.isSpecial()) {
      return true;
    }
    try {
      node_store.lockForWrite();

      // Get the area as a MutableArea object
      MutableArea leaf_area =
                           node_store.getMutableArea(to64bitStoreAddress(ref));
      // We synchronize over a reference count lock
      // (Pending: should we lock by area instead?  I'm not sure it will be
      //  worth the complexity of a more fine grained locking mechanism for the
      //  performance improvements - maybe we should implement a locking
      //  mechanism inside MutableArea).
      synchronized (REFERENCE_COUNT_LOCK) {
        // Assert this is a leaf
        if (PRAGMATIC_CHECKS) {
          leaf_area.position(0);
          short node_type = leaf_area.getShort();
          if (node_type != STORE_LEAF_TYPE) {
            throw new IOException("Can only link to a leaf node.");
          }
        }
        leaf_area.position(4);
        int ref_count = leaf_area.getInt();
        // If reference counter is near overflowing, return false.
        if (ref_count > Integer.MAX_VALUE - 8) {
          return false;
        }
        leaf_area.position(4);
        leaf_area.putInt(ref_count + 1);

//        System.out.println(
//             "++ I changed reference count of " + ref + " to " + (ref_count + 1));
      }
      return true;
    }
    finally {
      node_store.unlockForWrite();
    }

  }
  
  /**
   * Performs the node dispose operation.  This determines if the node is a
   * branch or a leaf.  If the node is a branch it is reclaimed immediately.
   * If the node is a leaf, it is only reclaimed when its reference count is
   * 0.
   * <p>
   * Assumes we are under a lockForWrite.
   */
  private void doDisposeNode(NodeReference ref) throws IOException {
    // If the node is a special node, then we don't dispose it
    if (ref.isSpecial()) {
      return;
    }
    // Is 'ref' a leaf node?
    MutableArea node_area =
                          node_store.getMutableArea(to64bitStoreAddress(ref));
    // Are we a leaf?
    node_area.position(0);
    int node_type = node_area.getShort();
    if (node_type == STORE_LEAF_TYPE) {
      // Yes, get its reference_count,
      synchronized (REFERENCE_COUNT_LOCK) {
        node_area.position(4);
        int ref_count = node_area.getInt();
        // If the reference_count is >1 then decrement it and return
        if (ref_count > 1) {
          node_area.position(4);
          node_area.putInt(ref_count - 1);
//          System.out.println(
//                  "-- I changed reference count of " + ref + " to " + (ref_count - 1));
          return;
        }
      }
    }
    else if (node_type != STORE_BRANCH_TYPE) {
      // Has to be a branch type, otherwise failure
      throw new IOException("Unknown node type.");
    }
    // 'ref' is a none leaf branch or its reference count is 1, so delete the
    // area.
    
    // NOTE, we delete from the cache first before we delete the area
    //   because the deleted area may be reclaimed immediately and deleting
    //   from the cache after may be too late.

    // Delete from the cache because the given ref may be recycled for a new
    // node at some point.
    synchronized (branch_cache) {
      branch_cache.remove(ref);
    }
    // Delete the area
    node_store.deleteArea(to64bitStoreAddress(ref));
//    System.out.println("&& I deleted: " + ref);
    
  }

  /**
   * Immediately disposes a node in the store.  Called when we are disposing
   * a transaction that wasn't committed.
   */
  @Override
  public void disposeNode(NodeReference ref) throws IOException {
    try {
      node_store.lockForWrite();

      doDisposeNode(ref);

    }
    finally {
      node_store.unlockForWrite();
    }
  }

  /**
   * {@inhericDoc}
   */
  @Override
  public boolean featureAccountForAllNodes() {
    // All nodes must be accounted for in this implementation,
    return true;
  }

//  /**
//   * Writes a VersionInfo object out to the store.
//   */
//  private long writeVersionInfo(VersionInfo version_info) throws IOException {
//    long[] deleted_nodes = version_info.getDeletedNodesList();
//
//    // Write the version info,
//    AreaWriter writer =
//         node_store.createArea(4 + 4 + 8 + 8 + 4 + (deleted_nodes.length * 8));
//    writer.putInt(0x04EA23);
//    writer.putInt(1);
//    writer.putLong(version_info.version_id);
//    writer.putLong(version_info.root_node_pointer);
//    writer.putInt(deleted_nodes.length);
//    for (int i = 0; i < deleted_nodes.length; ++i) {
//      writer.putLong(deleted_nodes[i]);
//    }
//    writer.finish();
//
//    return writer.getID();
//  }

  // ---------- Inner classes ----------

//  /**
//   * An implementation of FixedSizeSerialSet that handles the storage of atomic
//   * data elements.
//   */
//  private class AtomicDataSerialSet extends FixedSizeSerialSet {
//
//    /**
//     * Constructor.
//     */
//    AtomicDataSerialSet(DataFile data) {
//      super(data, 16 + 16);
//    }
//
//    /**
//     * Returns the atomic data identifier for the record at index n.
//     */
//    AtomicKey getAtomicIdentifier(long n) {
//      positionOn(n);
//      long primary = getDataFile().getLong();
//      int secondary = getDataFile().getInt();
//      short type = getDataFile().getShort();
//      return new AtomicKey(type, secondary, primary);
//    }
//
//    /**
//     * Returns the atomic data element for the record at index n.
//     */
//    byte[] getAtomicData(int n) {
////      positionOn(n);
//      getDataFile().position((n * getRecordSize()) + 16);
//      byte[] buf = new byte[16];
//      getDataFile().get(buf, 0, 16);
//      return buf;
//    }
//
//    /**
//     * Create a new AtomicData object for the record at index n.
//     */
//    AtomicData createAtomicData(int n) {
//      positionOn(n);
//      long primary = getDataFile().getLong();
//      int secondary = getDataFile().getInt();
//      short type = getDataFile().getShort();
//      AtomicKey id = new AtomicKey(type, secondary, primary);
//      byte[] buf = new byte[16];
//      getDataFile().get(buf, 0, 16);
//      return new TSAtomicData(id, buf);
//    }
//
//    /**
//     * Updates the serial set setting the atomic data element with the given
//     * identifier.  If the data is zero, the record is removed.
//     */
//    void setAtomicData(AtomicKey atomic_id, AtomicData data) {
//      // Remove if the data is zero
//      if (data.toBigInteger().equals(BigInteger.ZERO)) {
//        removeAtomicData(atomic_id);
//      }
//      // Otherwise search for the record and insert it or set it (whichever
//      // is appropriate).
//      else {
//        long record_elem = searchForRecord(atomic_id);
//        // If the record doesn't exist,
//        if (record_elem < 0) {
//          // Make room at the given position
//          record_elem = -(record_elem + 1);
//          getDataFile().position(record_elem * getRecordSize());
//          getDataFile().shift(getRecordSize());
//        }
//        getDataFile().position(record_elem * getRecordSize());
//        getDataFile().putLong(atomic_id.getPrimary());
//        getDataFile().putInt(atomic_id.getSecondary());
//        getDataFile().putShort(atomic_id.getType());
//        ((TSAtomicData) data).writeTo(getDataFile());
//      }
//    }
//
//    /**
//     * Removes the atomic data element with the given identifier.
//     */
//    void removeAtomicData(AtomicKey atomic_id) {
//      remove(atomic_id);
//    }
//
//    // ----- Implemented from FixedSizeSerialSet -----
//
//    protected Object getRecordKey(long record_pos) {
//      return getAtomicIdentifier(record_pos);
//    }
//
//    protected int compareRecordTo(long record_pos, Object record_key) {
//      return getAtomicIdentifier(record_pos).compareTo((AtomicKey) record_key);
//    }
//
//  }
  
//  /**
//   * The atomic data implementation.
//   */
//  class TSAtomicData implements AtomicData {
//
//    /**
//     * The unique identifier assigned this atomic data element in the database.
//     */
//    private final AtomicKey id;
//
//    /**
//     * The data element.
//     */
//    private final byte[] element;
//
//    /**
//     * Constructs the data element.
//     */
//    TSAtomicData(AtomicKey id) {
//      this.id = id;
//      element = new byte[16];
//    }
//
//    TSAtomicData(AtomicKey id, byte[] buf) {
//      this(id);
//      // Copy this value
//      assert(buf.length == element.length);
//      System.arraycopy(buf, 0, element, 0, buf.length);
//    }
//
//    /**
//     * Returns the identifier for this data element.
//     */
//    public AtomicKey getKey() {
//      return id;
//    }
//
//    /**
//     * Notifies the parent class that we updated.
//     */
//    private void notifyUpdate() {
//      if (!atomic_data_updates_list.contains(this)) {
//        atomic_data_updates_list.add(this);
//      }
//    }
//
//    /**
//     * Sets this element to the given value.
//     */
//    public void setValue(byte[] buf) {
//      assert(buf.length == element.length);
//      synchronized (atomic_data_updates_list) {
//        notifyUpdate();
//        System.arraycopy(buf, 0, element, 0, buf.length);
//      }
//    }
//
//    /**
//     * Gets this element (copies it to the given byte[] array).
//     */
//    public void getValue(byte[] buf) {
//      assert(buf.length == element.length);
//      synchronized (atomic_data_updates_list) {
//        System.arraycopy(element, 0, buf, 0, buf.length);
//      }
//    }
//
//    /**
//     * Returns this data element as a BigInteger value.
//     */
//    public BigInteger toBigInteger() {
//      synchronized (atomic_data_updates_list) {
//        return new BigInteger(element);
//      }
//    }
//
//    /**
//     * Sets this data element as a BigInteger value.
//     */
//    public void setValue(BigInteger bi) {
//      synchronized (atomic_data_updates_list) {
//        notifyUpdate();
//        byte[] in = bi.toByteArray();
//        if (in.length > element.length) {
//          throw new RuntimeException("Overflow");
//        }
//        int sz = element.length;
//        int p = in.length - 1;
//        for (int i = sz - 1; i >= 0; --i) {
//          if (p >= 0) {
//            element[i] = in[p];
//            --p;
//          }
//          else {
//            if (in[0] < 0) {
//              element[i] = -1;
//            }
//            else {
//              element[i] = 0;
//            }
//          }
//        }
//      }
//    }
//
//    /**
//     * Atomic mutation, add then fetch the value after the addition.
//     */
//    public BigInteger addThenFetch(long add_amount) {
//      synchronized (atomic_data_updates_list) {
//        BigInteger bi = toBigInteger();
//        bi = bi.add(BigInteger.valueOf(add_amount));
//        setValue(bi);
//        return bi;
//      }
//    }
//
//    /**
//     * Atomic mutation, add and return the value before the addition.
//     */
//    public BigInteger fetchThenAdd(long add_amount) {
//      synchronized (atomic_data_updates_list) {
//        BigInteger bi = toBigInteger();
//        BigInteger cur_bi = bi;
//        bi = bi.add(BigInteger.valueOf(add_amount));
//        setValue(bi);
//        return cur_bi;
//      }
//    }
//
//
//
//    /**
//     * Writes the data element of this out to the given DataFile object.
//     */
//    public void writeTo(DataFile data) {
//      synchronized (atomic_data_updates_list) {
//        data.put(element, 0, 16);
//      }
//    }
//
//  }

  
  
  
  
  /**
   * Details of a version of this BTree.
   */
  public static class VersionInfo {

    /**
     * The version_id of this version.
     */
    private long version_id;

    /**
     * A pointer to the root node for this version.
     */
    private NodeReference root_node_pointer;

    /**
     * A reference to the Area in the store with information about this
     * version including nodes deleted from previous version.
     */
    private long version_info_ref;

    /**
     * The number of locks currently established on this version of the
     * database (number of currently active TreeSystemTransaction objects the
     * reference the data here).
     */
    private int lock_count;


    /**
     * Constructor.
     */
    public VersionInfo(long version_id,
                       NodeReference root_node_pointer,
                       long version_info_ref) {
      this.version_id = version_id;
      this.root_node_pointer = root_node_pointer;
      this.version_info_ref = version_info_ref;
    }

    public long getVersionID() {
      return version_id;
    }

    public NodeReference getRootNodePointer() {
      return root_node_pointer;
    }

    /**
     * Returns true if there are no locks on this version.
     */
    public boolean notLocked() {
      return lock_count == 0;
    }

    public void lock() {
      ++lock_count;
    }

    public void unlock() {
      --lock_count;
      if (lock_count < 0) {
        throw new RuntimeException("Lock error.");
      }
    }

    @Override
    public boolean equals(Object ob) {
      VersionInfo dest_v = (VersionInfo) ob;
      return (dest_v.version_id == version_id &&
              dest_v.root_node_pointer.equals(root_node_pointer));
    }

    @Override
    public int hashCode() {
      int hash = 7;
      hash = 61 * hash + (int) (this.version_id ^ (this.version_id >>> 32));
      hash = 61 * hash + (this.root_node_pointer != null ? this.root_node_pointer.hashCode() : 0);
      return hash;
    }

    @Override
    public String toString() {
      return "VersionInfo " + version_id + " rnp = " + root_node_pointer;
    }

  }

  /**
   * A tree leaf whose data is backed by an Area object.
   */
  private static class AreaTreeLeaf extends TreeLeaf {

    /**
     * The Area object.
     */
    private final Area area;

    /**
     * The size of the leaf,
     */
    private final int leaf_size;

    /**
     * The node ref of this leaf.
     */
    private final NodeReference node_ref;

    /**
     * Constructor.
     */
    public AreaTreeLeaf(NodeReference node_ref, int leaf_size, Area area) {
      super();
      this.node_ref = node_ref;
      this.leaf_size = leaf_size;
      this.area = area;
    }

    // ---------- Implemented from TreeLeaf ----------

    @Override
    public NodeReference getReference() {
      return node_ref;
    }

    @Override
    public int getSize() {
      return leaf_size;
    }

    @Override
    public int getCapacity() {
      throw new RuntimeException(
                           "Area leaf does not have a meaningful capacity.");
    }

    @Override
    public byte get(int position) throws IOException {
      area.position(position + 12);  // Make sure we position past the headers
      return area.get();
    }

    @Override
    public void get(int position, byte[] buf, int off, int len)
                                                          throws IOException {
      area.position(position + 12);  // Make sure we position past the headers
      area.get(buf, off, len);
    }

    @Override
    public void writeDataTo(AreaWriter writer) throws IOException {
      area.position(12);
      area.copyTo(writer, getSize());
    }

    @Override
    public void shift(int position, int offset) throws IOException {
      throw new IOException(
                      "Write methods not available for immutable store leaf.");
    }

    @Override
    public void put(int position, byte[] buf, int off, int len)
                                                          throws IOException {
      throw new IOException(
                      "Write methods not available for immutable store leaf.");
    }

    @Override
    public void setSize(int size) throws IOException {
      throw new IOException(
                      "Write methods not available for immutable store leaf.");
    }

    @Override
    public int getHeapSizeEstimate() {
      // Unsupported, we should never store this object on a heap since it's
      // not materialized.
      throw new UnsupportedOperationException();
    }

  }

}
