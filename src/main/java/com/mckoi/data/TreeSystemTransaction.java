/**
 * com.mckoi.treestore.TreeSystemTransaction  08 Oct 2004
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

import com.mckoi.util.ByteArrayUtil;
import java.io.IOException;
import java.security.SecurityPermission;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * A TreeSystemTransaction is a view of the TreeSystem that can be changed in
 * any way and the changes later committed.  Any changes made via this object
 * are immediately reflected in the view, but the changes are isolated from
 * any other transactions that may be running concurrently.
 * <p>
 * This class can simply be broken down into the following key methods;
 * <br>
 *   <b>boolean dataFileExists(Key)</b> - returns true if the DataFile exists
 *     (contains more than 0 information).<br>
 *   <b>DataFile getDataFile(Key, char)</b> - returns a DataFile object for the
 *     given key in the given mode ('r' for read only mode).
 * <p>
 * NOTE: This object is <b>NOT</b> thread safe, nor are the DataFile objects
 *   that are created either exclusive or inclusive of other DataFile objects
 *   created by this object.  For example, one DataFile object may not be
 *   changed while another is being written to at the same time by a different
 *   thread.
 *
 * @author Tobias Downer
 */

public class TreeSystemTransaction implements KeyObjectTransaction {

  // ----- Statics -----
  


  
  // ----- Members -----

  /**
   * A pointer to the root node.
   */
  private NodeReference root_node_ref;

  /**
   * The version_id of the version this transaction is rooted on.
   */
  private final long version_id;

  /**
   * The list of all nodes in the store (not heap nodes) that have been either
   * been added or deleted because of operations on this BTree.
   */
  private ArrayList<NodeReference> node_deletes;
  private ArrayList<NodeReference> node_inserts;

  /**
   * The local node heap containing the temporary in memory nodes created
   * during operations on this transaction.
   */
  private TreeNodeHeap local_node_heap = null;

//  /**
//   * A small cache for key properties used to help write efficiency.
//   */
//  private KeyValues[] key_value_cache = new KeyValues[16];
//  private int key_value_pos = 0;

  /**
   * The TreeSystem that this transaction is based on.
   */
  private final TreeSystem tree_store;

  /**
   * The current update version.  This is incremented every time an update
   * to the tree happens.
   */
  private long update_version;

  /**
   * The lowest key that has been updated in this transaction.
   */
  private Key lowest_size_changed_key = Key.TAIL_KEY;

  /**
   * The current calculated height of the tree, or -1 if unknown.
   */
  private int tree_height = -1;


  /**
   * Used to store hints on keys to fetch in subsequent queries.
   */
  private final HashMap<Key, String> prefetch_keymap = new HashMap();
//  ArrayList<Key> key_prefetch_list = new ArrayList();
//  private int prefetch_base_access = 0;


  /**
   * True if this is a read-only transaction and does not permit modifications
   * to be made.
   */
  private boolean read_only;

  /**
   * Set to true when this object is disposed.
   */
  private boolean disposed;

  /**
   * True if this object has been committed.
   */
  private boolean committed;

  /**
   * True if this transaction is non-committable (has state changed such that
   * it is no longer valid to commit it (typically the 'delete' list has been
   * broken).
   */
  private boolean non_committable;




  /**
   * Constructor.
   */
  protected TreeSystemTransaction(TreeSystem tree_store, long version_id,
                             NodeReference root_node_ref, boolean read_only) {

    this.tree_store = tree_store;
    this.root_node_ref = root_node_ref;
    this.version_id = version_id;
    this.update_version = 0;
    this.node_deletes = null;
    this.node_inserts = null;
    this.read_only = read_only;
    this.disposed = false;
    
  }

  /**
   * Returns the version id of this transaction's view of the database.
   */
  protected long getVersionID() {
    return version_id;
  }

  /**
   * Returns the current root node reference of this transaction.
   */
  protected NodeReference getRootNodeRef() {
    return root_node_ref;
  }

  void setRootNodeRef(NodeReference node_reference) {
    root_node_ref = node_reference;
  }

  TreeSystem getTreeSystem() {
    return tree_store;
  }

  int getTreeHeight() {
    return tree_height;
  }

  void setTreeHeight(int tree_height) {
    this.tree_height = tree_height;
  }

  /**
   * Returns the node inserts/deletes logs.
   */
  private ArrayList<NodeReference> getNodeDeletes() {
    if (node_deletes == null) {
      node_deletes = new ArrayList(64);
    }
    return node_deletes;
  }

  private ArrayList<NodeReference> getNodeInserts() {
    if (node_inserts == null) {
      node_inserts = new ArrayList(64);
    }
    return node_inserts;
  }

  /**
   * Returns all the nodes deleted by operations on this transaction.
   */
  protected ArrayList<NodeReference> getAllDeletedNodeRefs() {
    return getNodeDeletes();
  }

  /**
   * Returns the local node heap.
   */
  private TreeNodeHeap getNodeHeap() {
    // Note that we create the node heap on demand.  Transactions that only
    // read data will not incur this overhead.
    if (local_node_heap == null) {
      local_node_heap =
               new TreeNodeHeap(13999, tree_store.getNodeHeapMaxSize());
    }
    return local_node_heap;
  }
  
  /**
   * Fetches a node from the tree.  This may return either a branch or a leaf
   * from the store or from the mutable heap.
   */
  TreeNode fetchNode(NodeReference node_ref) throws IOException {

//    SecurityManager sm = System.getSecurityManager();
//    if (sm != null) {
//      sm.checkPermission(new SecurityPermission("mckoiddb.fetchNode"));
//    }

    // Is it a node we can fetch from the local node heap?
    if (isHeapNode(node_ref)) {
      TreeNode n = getNodeHeap().fetchNode(node_ref);
      if (n == null) throw new NullPointerException(node_ref.toString());
      return n;
    }

    // If there's nothing in the prefetch keymap,
    if (prefetch_keymap.isEmpty()) {
//      // TODO: REMOVE THIS
//      if (!tree_store.isNodeAvailableLocally(node_ref)) {
//        System.out.println("FETCHING SINGLE.");
//      }
      TreeNode n = tree_store.fetchNode(new NodeReference[] { node_ref })[0];
      if (n == null) throw new NullPointerException(node_ref.toString());
      return n;
    }

    ArrayList<NodeReference> prefetch_nodeset = new ArrayList();
    prefetch_nodeset.add(node_ref);
    discoverPrefetchNodeSet(prefetch_nodeset);

    int len = prefetch_nodeset.size();
    NodeReference[] node_refs = new NodeReference[len];
    for (int i = 0; i < len; ++i) {
      node_refs[i] = prefetch_nodeset.get(i);
    }

//    // TODO: REMOVE THIS
//    int fetch_count = 0;
//    for (long node : node_refs) {
//      if (!tree_store.isNodeAvailableLocally(node)) {
//        ++fetch_count;
//      }
//    }
//    if (fetch_count > 0) {
//      System.out.println("FETCHING: " + fetch_count + " nodes.");
//    }

    // Otherwise fetch the node from the tree store
    TreeNode n = tree_store.fetchNode(node_refs)[0];
    if (n == null) throw new NullPointerException(node_ref.toString());
    return n;
  }

  /**
   * Records all store node creation and deletion by this transaction.  This
   * information is logged so that if this transaction is committed and the
   * transaction goes out of scope that we may delete the deleted nodes, or
   * if this transaction is rolled back that we may immediately delete all
   * newly inserted nodes.
   */
  private void logStoreChange(byte type, NodeReference pointer) {
    if (!tree_store.featureAccountForAllNodes()) {
      return;
    }

    // Special node type changes are not logged
    if (pointer.isSpecial()) {
      return;
    }

//    if ((pointer & 0x02000000000000000L) != 0) {
//      // This could happen if there's a pointer overflow
//      throw new RuntimeException("Pointer error.");
//    }
//    // Special node type changes are not logged
//    if ((pointer & 0x01000000000000000L) != 0) {
//      return;
//    }

    if (type == 0) {
      // This type is for deleted nodes,
      getNodeDeletes().add(pointer);
    }
    else if (type == 1) {
      // This type is for inserts,
      getNodeInserts().add(pointer);
    }
    else {
      throw new RuntimeException("Incorrect type");
    }
  }

//  /**
//   * Removes the given elements from the log assuming the list of elements is
//   * sorted.
//   */
//  private final void removeFromLog(byte type, LongList refs) {
//    if (type == 0) {
//      // Removes from the delete list,
//      node_deletes = node_deletes.notIn(refs);
//    }
//    else if (type == 1) {
//      // Removes from the insert list,
//      node_inserts = node_inserts.notIn(refs);
//    }
//    else {
//      throw new RuntimeException("Incorrect type");
//    }
//  }
//
//  /**
//   * Returns true if either the insert or delete log entirely contains the
//   * elements given.
//   */
//  private final boolean logContainsAllItems(byte type, LongList refs) {
//    LongList in_list;
//    if (type == 0) {
//      in_list = node_deletes;
//    }
//    else if (type == 1) {
//      in_list = node_inserts;
//    }
//    else {
//      throw new RuntimeException("Incorrect type");
//    }
//
//    in_list.sort();
//
//    // For each element of the list,
//    int sz = refs.size();
//    for (int i = 0; i < sz; ++i) {
//      // Get the value
//      long v = refs.get(i);
//      // Is the value in the refs list?
//      if (in_list.binarySearch(v) < 0) {
//        // Not found so return false,
//        return false;
//      }
//    }
//    // The whole list searched and matched found for all values so return
//    // true.
//    return true;
//  }

  /**
   * Makes a node as deleted in the tree.  This will not actually delete the
   * node, but add the node to the list of node to be deleted.
   */
  final void deleteNode(NodeReference pointer) {
    // If we are deleting a node that's on the temporary node heap, we delete
    // it immediately.  We know such nodes are only accessed within the scope of
    // this transaction so we can free up the resources immediately.

    // Is this a heap node?
    if (isHeapNode(pointer)) {
      // Delete it now
      getNodeHeap().delete(pointer);
    }
    else {
      // Not a heap node, so we log that this node needs to be deleted when
      // we are certain it has gone out of scope of any concurrent transaction
      // that may need access to this data.
      // Logs a delete operation,
      logStoreChange((byte) 0, pointer);
    }
  }

  /**
   * Notifies that the given node has been written to the backing store,
   * updating any internal structures as necessary.
   */
  private void writtenNode(TreeNode node, NodeReference ref)
                                                           throws IOException {
    // Delete the reference to the old node,
    deleteNode(node.getReference());
    // Log the insert operation.
    logStoreChange((byte) 1, ref);
  }

//  /**
//   * Writes the heap node to the store, deletes the heap node, and returns a
//   * reference to the node in the store.
//   */
//  private final long actualWriteNode(TreeNode node) throws IOException {
//    // Write the node,
//    long ref = tree_store.writeNode(node);
//    // Delete the reference to the old node,
//    deleteNode(node.getReference());
//    // Log the insert operation.
//    logStoreChange((byte) 1, ref);
//    // And return,
//    return ref;
//  }

  /**
   * Disposes the store node immediately and permanently.
   */
  private void actualDisposeNode(NodeReference node_id) throws IOException {
    // Dispose of the node,
    tree_store.disposeNode(node_id);
    // And return
  }

  /**
   * Returns true if the node is frozen.
   */
  boolean isFrozen(NodeReference node_ref) {
    return !node_ref.isInMemory();
//    // A node is frozen if either it is in the store (node_ref >= 0) or it has
//    // the lock bit set to 0
//    return node_ref >= 0 ||
//           (node_ref & 0x02000000000000000L) == 0;
  }

  /**
   * Returns true if this node is on the heap.
   */
  boolean isHeapNode(NodeReference node_ref) {
    return node_ref.isInMemory();
  }

//  /**
//   * Compares the given Key object with the key of the record pointed to by the
//   * key.
//   */
//  int compareKeys(long key_ref, Key key) throws IOException {
//    // Fetch the leaf,
//    TreeLeaf leaf = (TreeLeaf) fetchNode(key_ref);
//    // And compare the keys
//    return leaf.getKey().compareTo(key);
//  }

  /**
   * Called after an operation to handle cache flushes.
   */
  private void cacheManage() throws IOException {
    // When this is called, there should be no locks on anything related to
    // this object.
    
//    // Assert there's no locks (manageNodeCache can perform a check point which
//    // requires there's no write locks).
//    tree_store.assertNoLocks();

    // Manages the node cache
    getNodeHeap().manageNodeCache();

  }
  
  /**
   * Returns the maximum leaf byte size.
   */
  private int maxLeafByteSize() {
    return tree_store.getMaxLeafByteSize();
  }

  /**
   * Returns the maximum branch size.
   */
  private int maxBranchSize() {
    return tree_store.getMaxBranchSize();
  }


  /**
   * Returns the previous key in the ordered set of keys.
   */
  private Key previousKeyOrder(Key key) {
    short type = key.getType();
    int secondary = key.getSecondary();
    long primary = key.getPrimary();
    if (primary == Long.MIN_VALUE) {
      // Should not use negative primary keys.
      throw new java.lang.IllegalStateException();
    }
    return new Key(type, secondary, primary - 1);
  }


  private TreeNode fetchNodeIfLocallyAvailable(NodeReference node_ref)
                                                           throws IOException {
    // If it's a heap node,
    if (isHeapNode(node_ref)) {
      return fetchNode(node_ref);
    }

    // If the node is locally available, return it,
    if (tree_store.isNodeAvailableLocally(node_ref)) {
      return tree_store.fetchNode(new NodeReference[] { node_ref })[0];
    }
    // Otherwise return null
    return null;
  }

  /**
   * Performs a tree walk for the first node of the given key and returns the
   * first node that isn't stored/cached locally, or null if the tree walk for
   * the key is currently stored locally.
   */
  private NodeReference lastUncachedNode(Key key) throws IOException {
    int cur_height = 1;
    NodeReference child_node_ref = getRootNodeRef();
    TreeBranch last_branch = null;
    int child_i = -1;

    // How this works;
    // * Descend through the tree and try to find the last node of the
    //   previous key.
    // * If a node is encoutered that is not cached locally, return it.
    // * If a leaf is reached, return the next leaf entry from the previous
    //   branch (this should be the first node of key).

    // This does not perform completely accurately for tree edges but this
    // should not present too much of a problem.

    key = previousKeyOrder(key);

    // Try and fetch the node, if it's not available locally then return the
    // child node ref
    TreeNode node = fetchNodeIfLocallyAvailable(child_node_ref);
    if (node == null) {
      return child_node_ref;
    }

    while (true) {
      // Is the node a leaf?
      if (node instanceof TreeLeaf) {
        tree_height = cur_height;
        break;
      }
      // Must be a branch,
      else {
        final TreeBranch branch = (TreeBranch) node;
        last_branch = branch;
        // We ask the node for the child sub-tree that will contain this node
        child_i = branch.searchLast(key);
        // Child will be in this subtree
        child_node_ref = branch.getChild(child_i);

        // Ok, if we know child_node_ref is a leaf,
        if (cur_height + 1 == tree_height) {
          break;
        }

        // Try and fetch the node, if it's not available locally then return
        // the child node ref
        node = fetchNodeIfLocallyAvailable(child_node_ref);
        if (node == null) {
          return child_node_ref;
        }
        // Otherwise, descend to the child and repeat
        ++cur_height;
      }
    }

    // Ok, we've reached the end of the tree,

    // Fetch the next child_i if we are not at the end already,
    if (child_i + 1 < last_branch.size()) {
      child_node_ref = last_branch.getChild(child_i);
    }

    // If the child node is not a heap node, and is not available locally then
    // return it.
    if (!isHeapNode(child_node_ref) &&
        !tree_store.isNodeAvailableLocally(child_node_ref)) {
      return child_node_ref;
    }
    // The key is available locally,
    return null;
  }

  /**
   * Inspects the prefetch map and discovers nodes that can be fetched to
   * resolve data that is likely to be fetched in the future. The node_set is
   * populated
   */
  private void discoverPrefetchNodeSet(ArrayList<NodeReference> node_set)
                                                           throws IOException {

    // If the map is empty, return
    if (prefetch_keymap.isEmpty()) {
      return;
    }

    Iterator<Key> i = prefetch_keymap.keySet().iterator();
    while (i.hasNext()) {
      Key k = i.next();

      NodeReference node_ref = lastUncachedNode(k);

      if (node_ref != null) {
        if (!node_set.contains(node_ref)) {
          node_set.add(node_ref);
        }
      }
      else {
        // Remove the key from the prefetch map
        i.remove();
      }
    }

  }



  /**
   * Returns a sparse leaf node that will best fit the given max size and is
   * less than the maxLeafByteSize().
   */
  TreeLeaf createSparseLeaf(Key key, byte b, long max_size)
                                                         throws IOException {
    // Make sure the sparse leaf doesn't exceed the maximum leaf size
    int sparse_size = (int) Math.min(max_size, (long) maxLeafByteSize());
    // Make sure the sparse leaf doesn't exceed the maximum size of the
    // sparse leaf object.
    sparse_size = Math.min(65535, sparse_size);

    // Create node reference for a special sparse node,
    NodeReference node_ref =
                        NodeReference.createSpecialSparseNode(b, sparse_size);

//    // The byte encoding
//    int byte_code = (((int) b) & 0x0FF) << 16;
//    // Merge all the info into the sparse node ref
//    int sparse_code = sparse_size | byte_code | 0x01000000;
//    long node_ref = 0x01000000000000000L + sparse_code;

    return (TreeLeaf) fetchNode(node_ref);
  }
  
  /**
   * Creates a new empty leaf node in memory.
   */
  TreeLeaf createEmptyLeaf(Key key) {
    TreeLeaf leaf =
        getNodeHeap().createEmptyLeaf(this, key, maxLeafByteSize());
    return leaf;
  }

  /**
   * Creates a new branch node in memory.
   */
  TreeBranch createEmptyBranch() {
    TreeBranch branch =
        getNodeHeap().createEmptyBranch(this, maxBranchSize());
    return branch;
  }

  /**
   * Unfreezes a node (copies it from disk to memory so it may be mutated).
   * The old node is passed to the 'deleteNode' method.
   */
  TreeNode unfreezeNode(TreeNode node) throws IOException {
    NodeReference node_ref = node.getReference();
    if (isFrozen(node_ref)) {
      // Return a copy of the node
      TreeNode new_copy = getNodeHeap().copy(node,
               tree_store.getMaxBranchSize(), tree_store.getMaxLeafByteSize(),
               this);
      // Delete the old node,
      deleteNode(node_ref);
      return new_copy;
    }
    return node;
  }

  /**
   * Merges two children in a branch and sets the 'merge_buffer' with
   * information about the merge. If returns 1 then the left reference will
   * contain all the data and the right node will be deleted.
   */
  private int mergeNodes(final Key middle_key_value,
                         final NodeReference left_ref,
                         final NodeReference right_ref,
                         final Key left_left_key, final Key right_left_key,
                         final Object[] merge_buffer) throws IOException {
    // Fetch the nodes,
    TreeNode left_node = fetchNode(left_ref);
    TreeNode right_node = fetchNode(right_ref);
    // Are we merging branches or leafs?
    if (left_node instanceof TreeLeaf) {
      TreeLeaf lleaf = (TreeLeaf) left_node;
      TreeLeaf rleaf = (TreeLeaf) right_node;
      // Check the keys are identical,
      if (left_left_key.equals(right_left_key)) {
        // 80% capacity on a leaf
        final int capacity80 = (int) (0.80 * maxLeafByteSize());
        // True if it's possible to full merge left and right into a single
        final boolean fully_merge =
                        lleaf.getSize() + rleaf.getSize() <= maxLeafByteSize();
        // Only proceed if the leafs can be fully merged or the left is less
        // than 80% full,
        if (fully_merge || lleaf.getSize() < capacity80) {
          // Move elements from the right leaf to the left leaf so that either
          // the right node becomes completely empty or if that's not possible
          // the left node is 80% full.
          if (fully_merge) {
            // We can fit both nodes into a single node so merge into a single
            // node,
            TreeLeaf nleaf = (TreeLeaf) unfreezeNode(lleaf);
            byte[] copy_buf = new byte[rleaf.getSize()];
            rleaf.get(0, copy_buf, 0, copy_buf.length);
            nleaf.put(nleaf.getSize(), copy_buf, 0, copy_buf.length);

            // Delete the right node,
            deleteNode(rleaf.getReference());

            // Setup the merge state
            merge_buffer[0] = nleaf.getReference();
            merge_buffer[1] = (long) nleaf.getSize();
            return 1;
          }
          else {
            // Otherwise, we move bytes from the right leaf into the left
            // leaf until it is 80% full,
            int to_copy = capacity80 - lleaf.getSize();
            // Make sure we are copying at least 4 bytes and there are enough
            // bytes available in the right leaf to make the copy,
            if (to_copy > 4 && rleaf.getSize() > to_copy) {
              // Unfreeze both the nodes,
              TreeLeaf mlleaf = (TreeLeaf) unfreezeNode(lleaf);
              TreeLeaf mrleaf = (TreeLeaf) unfreezeNode(rleaf);
              // Copy,
              byte[] copy_buf = new byte[to_copy];
              mrleaf.get(0, copy_buf, 0, to_copy);
              mlleaf.put(mlleaf.getSize(), copy_buf, 0, to_copy);
              // Shift the data in the right leaf,
              mrleaf.shift(to_copy, -to_copy);

              // Return the merge state
              merge_buffer[0] = mlleaf.getReference();
              merge_buffer[1] = (long) mlleaf.getSize();
              merge_buffer[2] = right_left_key;
              merge_buffer[3] = mrleaf.getReference();
              merge_buffer[4] = (long) mrleaf.getSize();
              return 2;
            }
          }
        }
      } // leaf keys unequal
    }
    else if (left_node instanceof TreeBranch) {
      // Merge branches,
      TreeBranch lbranch = (TreeBranch) left_node;
      TreeBranch rbranch = (TreeBranch) right_node;

      final int capacity75 = (int) (0.75 * maxBranchSize());
      // True if it's possible to full merge left and right into a single
      final boolean fully_merge =
                           lbranch.size() + rbranch.size() <= maxBranchSize();

      // Only proceed if left is less than 75% full,
      if (fully_merge || lbranch.size() < capacity75) {
        // Move elements from the right branch to the left leaf only if the
        // branches can be completely merged into a node
        if (fully_merge) {
          // We can fit both nodes into a single node so merge into a single
          // node,
          TreeBranch nbranch = (TreeBranch) unfreezeNode(lbranch);
          // Merge,
          nbranch.mergeLeft(rbranch, middle_key_value, rbranch.size());

          // Delete the right branch,
          deleteNode(rbranch.getReference());

          // Setup the merge state
          merge_buffer[0] = nbranch.getReference();
          merge_buffer[1] = nbranch.getLeafElementCount();
          return 1;
        }
        else {
          // Otherwise, we move children from the right branch into the left
          // branch until it is 75% full,
          int to_copy = capacity75 - lbranch.size();
          // Make sure we are copying at least 4 bytes and there are enough
          // bytes available in the right leaf to make the copy,
          if (to_copy > 2 && rbranch.size() > to_copy + 3) {
            // Unfreeze the nodes,
            TreeBranch mlbranch = (TreeBranch) unfreezeNode(lbranch);
            TreeBranch mrbranch = (TreeBranch) unfreezeNode(rbranch);
            // And merge
            Key new_middle_value =
                       mlbranch.mergeLeft(mrbranch, middle_key_value, to_copy);

            // Setup and return the merge state
            merge_buffer[0] = mlbranch.getReference();
            merge_buffer[1] = mlbranch.getLeafElementCount();
            merge_buffer[2] = new_middle_value;
            merge_buffer[3] = mrbranch.getReference();
            merge_buffer[4] = mrbranch.getLeafElementCount();
            return 2;
          }
        }
      }
    }
    else {
      throw new RuntimeException("Unknown node type.");
    }
    // Signifies no change to the branch,
    return 3;
  }

  /**
   * Compacts the node by compacting the children that are on the heap.  This
   * is a recursive algorithm.  This will not reduce the height of the tree,
   * but it does 'bubble' the empty room up to the top of the tree where it is
   * less wasteful of physical space.  The 'min_bound' and 'max_bound' values
   * represent the minimum and maximum bounds of the keys to compact.
   */
  private void compactNode(final Key far_left,
                final NodeReference ref,
                final Object[] merge_buffer,
                final Key min_bound, final Key max_bound) throws IOException {

    // If the ref is not on the heap, return the ref,
    if (!isHeapNode(ref)) {
      return;
    }
    // Fetch the node,
    TreeNode node = fetchNode(ref);
    // If the node is a leaf, return the ref,
    if (node instanceof TreeLeaf) {
      return;
    }
    // If the node is a branch,
    else if (node instanceof TreeBranch) {
      // Cast to a branch
      TreeBranch branch = (TreeBranch) node;

      // We ask the node for the child sub-tree that will contain the range
      // of this key
      int first_child_i = branch.searchFirst(min_bound);
      int last_child_i = branch.searchLast(max_bound);
//      System.out.println("min_bound=" + min_bound);
//      System.out.println("max_bound=" + max_bound);
//      System.out.println("first_child_i=" + first_child_i);
//      System.out.println("last_child_i=" + last_child_i);
      // first_child_i may be negative which means a key reference is equal
      // to the key being searched, in which case we follow the left branch.
      if (first_child_i < 0) {
        first_child_i = -(first_child_i + 1);
      }

      // Compact the children,
      for (int i = first_child_i; i <= last_child_i; ++i) {
        // Change far left to represent the new far left node
        Key new_far_left = (i > 0) ? branch.getKeyValue(i) : far_left;
//        // Change min_bound to represent the new minimum key extent,
//        Key new_min_bound = (first_child_i > 0) ?
//                               branch.getKeyValue(first_child_i) : min_bound;
        // We don't change max_bound because it's not necessary.
        compactNode(new_far_left, branch.getChild(i), merge_buffer,
                     min_bound, max_bound);
      }

      // The number of children in this branch,
      int sz = branch.size();
//      // Compact the children,
//      for (int i = 0; i < sz; ++i) {
//        compactNode(branch.getChild(i), merge_buffer);
//      }
      // Now try and merge the compacted children,
      int i = first_child_i;
      // We must not let there be less than 3 children
      while (sz > 3 && i <= last_child_i - 1) {
        // The left and right children nodes,
        NodeReference left_child_ref = branch.getChild(i);
        NodeReference right_child_ref = branch.getChild(i + 1);
        // If at least one of them is a heap node we attempt to merge the
        // nodes,
        if (isHeapNode(left_child_ref) || isHeapNode(right_child_ref)) {
          // Set the left left key and right left key of the references,
          Key left_left_key = (i > 0) ? branch.getKeyValue(i) : far_left;
          Key right_left_key = branch.getKeyValue(i + 1);
          // Attempt to merge the nodes,
          int node_result = mergeNodes(branch.getKeyValue(i + 1),
                    left_child_ref, right_child_ref,
                    left_left_key, right_left_key,
                    merge_buffer);
          // If we merged into a single node then we update the left and
          // delete the right
          if (node_result == 1) {
            branch.setChild(
                    (NodeReference) merge_buffer[0], i);
            branch.setChildLeafElementCount(
                    (Long) merge_buffer[1], i);
            branch.removeChild(i + 1);
            // Reduce the size but don't increase i, because we may want to
            // merge again.
            --sz;
            --last_child_i;
          }
          else if (node_result == 2) {
            // Two result but there was a change (the left was increased in
            // size)
            branch.setChild(
                    (NodeReference) merge_buffer[0], i);
            branch.setChildLeafElementCount(
                    (Long) merge_buffer[1], i);
            branch.setKeyValueToLeft(
                    (Key) merge_buffer[2], i + 1);
            branch.setChild(
                    (NodeReference) merge_buffer[3], i + 1);
            branch.setChildLeafElementCount(
                    (Long) merge_buffer[4], i + 1);
            ++i;
          }
          else {
            // Otherwise, no change so skip to the next child,
            ++i;
          }
        }
        // left or right are not nodes on the heap so go to next,
        else {
          ++i;
        }
      }
    }
  }

  /**
   * Compacts the tree by traversing down the tree to the leaf of the given
   * absolute position merging branches to the left as it goes. This should
   * be used after a large branch delete operation to rebalance the tree
   * around the area that was pruned. Note that this only merges nodes that
   * are currently on the heap so should be called immediately after the tree
   * mutation.
   * <p>
   * This operation will not change the height of the tree. It may leave
   * single branch nodes at the top however.
   */
  private TreeBranch recurseRebalanceTree(
                            final long left_offset,
                            final int height,
                            final NodeReference node_ref,
                            final long absolute_position,
                            final Key in_left_key) throws IOException {

    // Put the node in memory,
    TreeBranch branch = (TreeBranch) fetchNode(node_ref);

    int sz = branch.size();
    int i;
    long pos = left_offset;
    // Find the first child i that contains the position.
    for (i = 0; i < sz; ++i) {
      long child_elem_count = branch.getChildLeafElementCount(i);
      // abs position falls within bounds,
      if (absolute_position >= pos &&
          absolute_position < pos + child_elem_count ) {
        break;
      }
      pos += child_elem_count;
    }

    if (i > 0) {

      NodeReference left_ref = branch.getChild(i - 1);
      NodeReference right_ref = branch.getChild(i);

      // Only continue if both left and right are on the heap
      if (isHeapNode(left_ref) &&
          isHeapNode(right_ref) &&
          isHeapNode(node_ref)) {

        final Key left_key = (i - 1 == 0) ? in_left_key
                                          : branch.getKeyValue(i - 1);
        final Key right_key = branch.getKeyValue(i);

        // Perform the merge operation,
        final Key mid_key_value = right_key;
        Object[] merge_buffer = new Object[5];
        int merge_result = mergeNodes(mid_key_value, left_ref, right_ref,
                                      left_key, right_key, merge_buffer);
        if (merge_result == 1) {
          branch.setChild((NodeReference) merge_buffer[0], i - 1);
          branch.setChildLeafElementCount((Long) merge_buffer[1], i - 1);
          branch.removeChild(i);
        }
        //
        else if (merge_result == 2) {
          branch.setChild((NodeReference) merge_buffer[0], i - 1);
          branch.setChildLeafElementCount((Long) merge_buffer[1], i - 1);
          branch.setKeyValueToLeft((Key) merge_buffer[2], i);
          branch.setChild((NodeReference) merge_buffer[3], i);
          branch.setChildLeafElementCount((Long) merge_buffer[4], i);
        }

      }

    }

    // After merge, we don't know how the children will be placed, so we
    // do another search on the child to descend to,

    sz = branch.size();
    pos = left_offset;
    // Find the first child i that contains the position.
    for (i = 0; i < sz; ++i) {
      long child_elem_count = branch.getChildLeafElementCount(i);
      // abs position falls within bounds,
      if (absolute_position >= pos &&
          absolute_position < pos + child_elem_count ) {
        break;
      }
      pos += child_elem_count;
    }

    // Descend on 'i'
    TreeNode descend_child = fetchNode(branch.getChild(i));

    // Finish if we hit a leaf
    if (descend_child instanceof TreeLeaf) {
      // End if we hit the leaf,
      return branch;
    }

    final Key new_left_key = (i == 0) ? in_left_key
                                      : branch.getKeyValue(i);

    // Otherwise recurse on the child,
    TreeBranch child_branch =
        recurseRebalanceTree(pos, height + 1,
                             descend_child.getReference(), absolute_position,
                             new_left_key);

    // Make sure we unfreeze the branch
    branch = (TreeBranch) unfreezeNode(branch);

    // Update the child,
    branch.setChild(child_branch.getReference(), i);
    branch.setChildLeafElementCount(child_branch.getLeafElementCount(), i);

    // And return this branch,
    return branch;
  }

  /**
   * Generates a 'TreeWriteSequence' object that represents the series of
   * commands needed to flush a tree update to a backing store. The tree
   * write operation is broken into 2 phases, the allocation phase and the
   * write phase.
   */
  private int populateSequence(final NodeReference ref,
                              TreeWriteSequence sequence) throws IOException {
    // If it's not a heap node, return
    if (!isHeapNode(ref)) {
      return -1;
    }

    // It is a heap node, so fetch
    TreeNode node = fetchNode(ref);
    // Is it a leaf or a branch?
    if (node instanceof TreeLeaf) {
      // If it's a leaf, simply write it out
      return sequence.sequenceNodeWrite(node);
    }
    else if (node instanceof TreeBranch) {
      // This is a branch,
      // Sequence this branch to be written out,
      int branch_id = sequence.sequenceNodeWrite(node);
      // For each child in the branch,
      TreeBranch branch = (TreeBranch) node;
      int sz = branch.size();
      for (int i = 0; i < sz; ++i) {
        NodeReference child_ref = branch.getChild(i);
        // Sequence the child
        int child_id = populateSequence(child_ref, sequence);
        // If something could be sequenced in the child,
        if (child_id != -1) {
          // Make the branch command,
          sequence.sequenceBranchLink(branch_id, i, child_id);
        }
      }
      // Return the id of the branch in the sequence,
      return branch_id;
    }
    else {
      throw new RuntimeException("Unknown node type.");
    }
  }
  
  /**
   * Writes the node out to the backing store and deletes the memory nodes as
   * are necessary.
   */
  NodeReference writeNode(final NodeReference ref) throws IOException {
    // Create the sequence,
    TreeWriteSequence sequence = new TreeWriteSequence();
    // Create the command sequence to write this tree out,
    int root_id = populateSequence(ref, sequence);

    if (root_id != -1) {
      // Write out this sequence,
      NodeReference[] refs = tree_store.performTreeWrite(sequence);

      // Update internal structure for each node written,
      List<TreeNode> nodes = sequence.getAllBranchNodes();
      int sz = nodes.size();
      for (int i = 0; i < sz; ++i) {
        writtenNode(nodes.get(i), refs[i]);
      }
      int bnodes_sz = sz;
      nodes = sequence.getAllLeafNodes();
      sz = nodes.size();
      for (int i = 0; i < sz; ++i) {
        writtenNode(nodes.get(i), refs[i + bnodes_sz]);
      }

      // Normalize the pointer,
      if (root_id >= TreeWriteSequence.BPOINT) {
        root_id = root_id - TreeWriteSequence.BPOINT;
      }
      else {
        root_id = root_id + bnodes_sz;
      }

      // Return a reference to the node written,
      return refs[root_id];
    }
    else {
      return ref;
    }

//    // If it's not a heap node, return
//    if (!isHeapNode(ref)) {
//      return ref;
//    }
//
//    // It is a heap node, so fetch
//    TreeNode node = fetchNode(ref);
//    // Is it a leaf or a branch?
//    if (node instanceof TreeLeaf) {
//      TreeLeaf leaf = (TreeLeaf) node;
//      // If it's a leaf, simply write it out
//      long new_ref = actualWriteNode(leaf);
//      // And return it.
//      return new_ref;
//    }
//    else if (node instanceof TreeBranch) {
//      // This is a branch, so we need to write out any children that are on
//      // the heap before we write out the branch itself,
//      TreeBranch branch = (TreeBranch) node;
//
//      int sz = branch.size();
//
//      for (int i = 0; i < sz; ++i) {
//        long old_ref = branch.getChild(i);
//        long new_ref = writeNode(old_ref);
//        branch.setChild(new_ref, i);
//      }
//      // Then write out the branch node,
//      long new_ref = actualWriteNode(branch);
//      // And return the new reference
//      return new_ref;
//    }
//    else {
//      throw new RuntimeException("Unknown node type.");
//    }
  }

  /**
   * Walks the children of the branches and disposes all heap nodes that
   * are found.
   */
  private void disposeHeapNodes(final NodeReference ref) throws IOException {
    // If it's not a heap node, return
    if (!isHeapNode(ref)) {
      return;
    }
    // It is a heap node, so fetch
    TreeNode node = fetchNode(ref);
    // Is it a leaf or a branch?
    if (node instanceof TreeLeaf) {
      // If it's a leaf, dispose it
      deleteNode(ref);
      // And return,
      return;
    }
    else if (node instanceof TreeBranch) {
      // This is a branch, so we need to dipose the children if they are heap
      TreeBranch branch = (TreeBranch) node;

      int sz = branch.size();
      for (int i = 0; i < sz; ++i) {
        // Recurse for each child,
        disposeHeapNodes(branch.getChild(i));
      }
      // Then dispose this,
      deleteNode(ref);
      // And return,
      return;
    }
    else {
      throw new RuntimeException("Unknown node type.");
    }
  }

  /**
   * Walks the children of the branches and disposes all nodes that
   * are found.
   */
  private void disposeTree(final NodeReference ref) throws IOException {
    // It is a heap node, so fetch
    TreeNode node = fetchNode(ref);
    // Is it a leaf or a branch?
    if (node instanceof TreeLeaf) {
      // If it's a leaf, dispose it
      deleteNode(ref);
      // And return,
      return;
    }
    else if (node instanceof TreeBranch) {
      // This is a branch, so we need to dipose the children if they are heap
      TreeBranch branch = (TreeBranch) node;

      int sz = branch.size();
      for (int i = 0; i < sz; ++i) {
        // Recurse for each child,
        disposeTree(branch.getChild(i));
      }
      // Then dispose this,
      deleteNode(ref);
      // And return,
      return;
    }
    else {
      throw new RuntimeException("Unknown node type.");
    }
  }

  /**
   * Flushes the nodes that are found in the given sorted refs list
   * 'include_refs'.
   */
  private NodeReference flushNodes(final NodeReference ref,
              final NodeReference[] include_refs) throws IOException {
    if (!isHeapNode(ref)) {
      return ref;
    }
    // Is this reference in the list?
    int c = Arrays.binarySearch(include_refs, ref);
    if (c < 0) {
      // It was not found, so go to the children,
      // Note that this node will change if it's a branch node, but the
      // reference to it will not change.

      // It is a heap node, so fetch
      TreeNode node = fetchNode(ref);
      // Is it a leaf or a branch?
      if (node instanceof TreeLeaf) {
        return ref;
      }
      else if (node instanceof TreeBranch) {
        // This is a branch, so we need to write out any children that are on
        // the heap before we write out the branch itself,
        TreeBranch branch = (TreeBranch) node;

        int sz = branch.size();

        for (int i = 0; i < sz; ++i) {
          NodeReference old_ref = branch.getChild(i);
          // Recurse
          NodeReference new_ref = flushNodes(old_ref, include_refs);
          branch.setChild(new_ref, i);
        }
        // And return the reference
        return ref;
      }
      else {
        throw new RuntimeException("Unknown node type.");
      }

    }
    else {
      // This node was in the 'include_refs' list so write it out now,
      return writeNode(ref);
    }
    
  }

  /**
   * Removes the child tree and any leaf nodes.
   */
  private void deleteChildTree(int height, NodeReference node)
                                                         throws IOException {

    if (height == tree_height) {
      // This is a known leaf node,
      deleteNode(node);
      return;
    }
    // Fetch the node,
    TreeNode tree_node = fetchNode(node);
    if (tree_node instanceof TreeLeaf) {
      // Leaf reached, so set the tree height, delete and return
      tree_height = height;
      deleteNode(node);
      return;
    }

//    // Otherwise it's a branch,
//    TreeBranch tree_branch = (TreeBranch) tree_node;

    // The behaviour here changes depending on the system implementation.
    // Either we can simply unlink from the entire tree or we need to
    // recursely free all the leaf nodes.
    if (tree_store.featureAccountForAllNodes()) {
      // Need to account for all nodes so delete the node and all in the
      // sub-tree.
      disposeTree(node);
    }
    else {
      // Otherwise we can simply unlink the branches on the heap and be
      // done with it.
      disposeHeapNodes(node);
    }

  }


  /**
   * Deletes the given absolute bounds from the leaf.
   */
  private Object[] deleteFromLeaf(final long left_offset,
          final NodeReference leaf,
          final long start_pos, final long end_pos,
          final Key in_left_key) throws IOException {

    assert(start_pos < end_pos);

    TreeLeaf tree_leaf = (TreeLeaf) unfreezeNode(fetchNode(leaf));
    int leaf_start = 0;
    int leaf_end = tree_leaf.getSize();
    int del_start = (int) Math.max(start_pos - left_offset, (long) leaf_start);
    int del_end =   (int) Math.min(end_pos - left_offset,   (long) leaf_end);

    int remove_amount = del_end - del_start;

    // Remove from the end point,
    tree_leaf.shift(del_end, -remove_amount);

//    System.out.println("leaf_start = " + leaf_start + " leaf_end = " + leaf_end);
//    System.out.println("del_start = " + del_start + " del_end = " + del_end);
//    System.out.println("removed = " + remove_amount);

    return new Object[] { tree_leaf.getReference(), (long) remove_amount,
                          in_left_key, false };

  }

  /**
   * Recursive method that removes all the leaf and branch nodes between
   * the absolute start_pos and end_pos positions.
   * <p>
   * This can remove nodes over key boundaries so it can be used to remove
   * a range of consecutive keys.
   * <p>
   * This operation does not change the height of the tree, therefore it's
   * possible (likely even) to leave 'strangling' single reference branch
   * chains. Another process should therefore occur after this operation to
   * re-balance over the data that was removed.
   */
  private Object[] recurseRemoveBranches(final long left_offset,
              final int height, final NodeReference node,
              final long start_pos, final long end_pos,
              final Key in_left_key) throws IOException {

//      System.out.println("+ left_offset = " + left_offset);
//      System.out.println("+ tree_height = " + tree_height + " height = " + height);

    // Do we know if this is a leaf node?
    if (tree_height == height) {
      return deleteFromLeaf(left_offset, node, start_pos, end_pos, in_left_key);
//        // We reached a leaf, so return,
//        return new Object[] { node, (long) 0, in_left_key, false };
    }

    // Fetch the node,
    TreeNode tree_node = fetchNode(node);
    if (tree_node instanceof TreeLeaf) {
      // Leaf reach, so set the tree height and return
      tree_height = height;
      return deleteFromLeaf(left_offset, node, start_pos, end_pos, in_left_key);
//        return new Object[] { node, (long) 0, in_left_key, false };
    }




    // The amount removed,
    long remove_count = 0;

    // This is a branch,
    TreeBranch tree_branch = (TreeBranch) tree_node;
    tree_branch = (TreeBranch) unfreezeNode(tree_branch);

//      // Set to true if this branch needs to be rebalanced,
//      boolean do_branch_rebalance = false;

    Key parent_left_key = in_left_key;

    // Find all the children branches between the bounds,
    int child_count = tree_branch.size();
    long pos = left_offset;
    for (int i = 0; i < child_count && pos < end_pos; ++i) {
      long child_node_size = tree_branch.getChildLeafElementCount(i);
      long next_pos = pos + child_node_size;

//        System.out.println("  * pos = (" + start_pos + ", " + end_pos + ")");
//        System.out.println("  * child = (" + pos + ", " + next_pos + ")");

      // Test if start_pos/end_pos bounds intersects with this child,
      if (start_pos < next_pos && end_pos > pos) {
        // Yes, we intersect,

        // Make sure the branch is on the heap,
        NodeReference child_node = tree_branch.getChild(i);

        // If we intersect entirely remove the child from the branch,
        if (pos >= start_pos && next_pos <= end_pos) {
//            System.out.println("^^^ DELETING CHILD(1)");
          // Delete the child tree,
          deleteChildTree(height + 1, child_node);
          remove_count += child_node_size;

          // If removing the first child, bubble up a new left_key
          if (i == 0) {
            parent_left_key = tree_branch.getKeyValue(1);
          }
          // Otherwise parent left key doesn't change
          else {
            parent_left_key = in_left_key;
          }

//            // Assert it's leaf,
//            // PENDING: REMOVE THIS
//            TreeLeaf child_leaf = (TreeLeaf) fetchNode(child_node);

          // Remove the child from the branch,
          tree_branch.removeChild(i);
          --i;
          --child_count;
        }
        else {
//            System.out.println("^^^ DELETING CHILD(2)");
          // We don't intersect entirely, so recurse on this,
          // The left key
          Key r_left_key = (i == 0) ? in_left_key :
                                      tree_branch.getKeyValue(i);

          Object[] rv =
               recurseRemoveBranches(pos, height + 1,
                                     child_node, start_pos, end_pos,
                                     r_left_key);
          NodeReference new_child_ref = (NodeReference) rv[0];
          long removed_in_child = (Long) rv[1];
          Key child_left_key = (Key) rv[2];
//            do_branch_rebalance = (Boolean) rv[3];

          remove_count += removed_in_child;

          // Update the child,
          tree_branch.setChild(new_child_ref, i);
          tree_branch.setChildLeafElementCount(
                                      child_node_size - removed_in_child, i);
          if (i == 0) {
            parent_left_key = child_left_key;
          }
          else {
            tree_branch.setKeyValueToLeft(child_left_key, i);
            parent_left_key = in_left_key;
          }

        }

      }

      // Next child in the branch,
      pos = next_pos;
    }

    // Return the reference and remove count,
//      System.out.println("<--- RETURN");
    boolean parent_rebalance = (tree_branch.size() <= 2);
    return new Object[] { tree_branch.getReference(), remove_count,
                          parent_left_key, parent_rebalance };

  }

  /**
   * Removes all data between the given absolute positions where the data is
   * covered by the given key (can not be used to remove data across data
   * covered by different keys). The bounds is inclusive of position_start
   * but exclusive for position_end.
   */
  final void removeAbsoluteBounds(
                 long position_start, long position_end) throws IOException {

//    System.out.println("BIG DELETE");
//    System.out.println("position_start = " + position_start);
//    System.out.println("position_end = " + position_end);
//    System.out.println("size = " + (position_end - position_start));
//    new Error().printStackTrace();

    // We scan from the root and remove branches that we determine are
    // fully represented by the key and bounds, being careful about edge
    // conditions.

    Object[] rv =
           recurseRemoveBranches(0, 1, getRootNodeRef(),
                                 position_start, position_end, Key.HEAD_KEY);
    setRootNodeRef((NodeReference) rv[0]);
    long remove_count = (Long) rv[1];

    // Assert we didn't remove more or less than requested,
    if (remove_count != (position_end - position_start)) {
      throw new RuntimeException("Assert failed " + remove_count +
                                 " to " + (position_end - position_start));
    }

    // Adjust position_end by the amount removed,
    position_end -= remove_count;

    // Rebalance the tree. This does not change the height of the tree but
    // it may leave single branch nodes at the top.
    setRootNodeRef(recurseRebalanceTree(0, 1,
                 getRootNodeRef(), position_end, Key.HEAD_KEY).getReference());

    // Shrink the tree if the top contains single child branches
    while (true) {
      TreeBranch branch = (TreeBranch) fetchNode(getRootNodeRef());
      if (branch.size() == 1) {
        // Delete the root node and go to the child,
        deleteNode(getRootNodeRef());
        setRootNodeRef(branch.getChild(0));
        if (getTreeHeight() != -1) {
          setTreeHeight(getTreeHeight() - 1);
        }
      }
      // Otherwise break,
      else {
        break;
      }
    }

//    this.printDebugOutput();

    // Done,

  }

  /**
   * Walks the tree and discovers the position of the end of the data for the
   * given key.  The 64-bit value is the absolute position of the key
   * within the sequence of all data stored in the tree. Returns -(pos + 1)
   * if the key isn't found, where pos is the position the key would be
   * placed.
   * <p>
   * Note that the returned value is likely to be invalidated when the tree is
   * modified.
   */
  private long keyEndPosition(Key key) throws IOException {
    Key left_key = Key.HEAD_KEY;
    int cur_height = 1;
    long left_offset = 0;
    long node_total_size = -1;
    TreeNode node = fetchNode(getRootNodeRef());

    while (true) {
      // Is the node a leaf?
      if (node instanceof TreeLeaf) {
        tree_height = cur_height;
        break;
      }
      // Must be a branch,
      else {
        final TreeBranch branch = (TreeBranch) node;
        // We ask the node for the child sub-tree that will contain this node
        int child_i = branch.searchLast(key);
        // Child will be in this subtree
        final long child_offset = branch.childOffset(child_i);
        final NodeReference child_node_ref = branch.getChild(child_i);
        node_total_size = branch.getChildLeafElementCount(child_i);
        // Get the left key of the branch if we can
        if (child_i > 0) {
          left_key = branch.getKeyValue(child_i);
        }
        // Update left_offset
        left_offset += child_offset;

        // Ok, if we know child_node_ref is a leaf,
        if (cur_height + 1 == tree_height) {
          break;
        }

        // Otherwise, descend to the child and repeat
        node = fetchNode(child_node_ref);
        ++cur_height;
      }
    }

    // Ok, we've reached the end of the tree,
    // 'left_key' will be the key of the node we are on,
    // 'node_total_size' will be the size of the node,

    // If the key matches,
    final int c = key.compareTo(left_key);
    if (c == 0) {
      return left_offset + node_total_size;
    }
    // If the searched for key is less than this
    else if (c < 0) {
      return -(left_offset + 1);
    }
    // If this key is greater, relative offset is at the end of this node.
    else { //if (c > 0) {
      return -((left_offset + node_total_size) + 1);
    }

  }

  /**
   * Returns an absolute position for the key or position where it should
   * be inserted. This does not return a negative location if the key is
   * not found, unlike 'keyEndPosition'.
   */
  private long absKeyEndPosition(Key key) throws IOException {
    long pos = keyEndPosition(key);
    return (pos < 0) ? -(pos + 1) : pos;
  }

  /**
   * Returns the start and end position of the given key or null if the key
   * doesn't exist.
   */
  private long[] getDataFileBounds(Key key) throws IOException {

    Key left_key = Key.HEAD_KEY;
    int cur_height = 1;
    long left_offset = 0;
    long node_total_size = -1;
    TreeNode node = fetchNode(getRootNodeRef());
    TreeBranch last_branch = (TreeBranch) node;
    int child_i = -1;

    while (true) {
      // Is the node a leaf?
      if (node instanceof TreeLeaf) {
        tree_height = cur_height;
        break;
      }
      // Must be a branch,
      else {
        final TreeBranch branch = (TreeBranch) node;
        // We ask the node for the child sub-tree that will contain this node
        child_i = branch.searchLast(key);
        // Child will be in this subtree
        final long child_offset = branch.childOffset(child_i);
        node_total_size = branch.getChildLeafElementCount(child_i);
        // Get the left key of the branch if we can
        if (child_i > 0) {
          left_key = branch.getKeyValue(child_i);
        }
        // Update left_offset
        left_offset += child_offset;
        last_branch = branch;

        // Ok, if we know child_node_ref is a leaf,
        if (cur_height + 1 == tree_height) {
          break;
        }

        // Otherwise, descend to the child and repeat
        final NodeReference child_node_ref = branch.getChild(child_i);
        node = fetchNode(child_node_ref);
        ++cur_height;
      }
    }

    // Ok, we've reached the leaf node on the search,
    // 'left_key' will be the key of the node we are on,
    // 'node_total_size' will be the size of the node,
    // 'last_branch' will be the branch immediately above the leaf
    // 'child_i' will be the offset into the last branch we searched

    long end_pos;

    // If the key matches,
    final int c = key.compareTo(left_key);
    if (c == 0) {
      end_pos = left_offset + node_total_size;
    }
    // If the searched for key is less than this
    else if (c < 0) {
      end_pos = -(left_offset + 1);
    }
    // If this key is greater, relative offset is at the end of this node.
    else { //if (c > 0) {
      end_pos = -((left_offset + node_total_size) + 1);
    }

    // If the key doesn't exist return the bounds as the position data is
    // entered.
    if (end_pos < 0) {
      long p = -(end_pos + 1);
      return new long[] { p, p };
    }

    // Now we have the end position of a key that definitely exists, we can
    // query the parent branch and see if we can easily find the record
    // start.

    // Search back through the keys until we find a key that is different,
    // which is the start bounds of the key,
    long predicted_start_pos = end_pos - node_total_size;
    for (int i = child_i - 1; i > 0; --i) {
      Key k = last_branch.getKeyValue(i);
      if (key.compareTo(k) == 0) {
        // Equal,
        predicted_start_pos = predicted_start_pos -
                                      last_branch.getChildLeafElementCount(i);
      }
      else {
        // Not equal
        if (predicted_start_pos > end_pos) {
          throw new RuntimeException(
                                 "Assertion failed: (1) start_pos > end_pos");
        }
        return new long[] { predicted_start_pos, end_pos };
      }
    }

    // Otherwise, find the end position of the previous key through a tree
    // search
    Key previous_key = previousKeyOrder(key);
    long start_pos = absKeyEndPosition(previous_key);

    if (start_pos > end_pos) {
      throw new RuntimeException("Assertion failed: (2) start_pos > end_pos");
    }
    return new long[] { start_pos, end_pos };

  }

//  /**
//   * Walks the tree and discovers the next Key value after the key value given,
//   * or TAIL_KEY if the last key is found.  'right_key' must be set to
//   * 'TAIL_KEY' on the initial call to this recursive method.
//   */
//  private Key nextKey(final Key right_key,
//                      final Key key,
//                      final NodeReference ref) throws IOException {
//    // Fetch the node
//    TreeNode node = fetchNode(ref);
//    // If the node is a branch node
//    if (node instanceof TreeBranch) {
//      final TreeBranch branch = (TreeBranch) node;
//      // We ask the node for the child sub-tree that will contain this node
//      int child_i = branch.searchLast(key);
//      // Child will be in this subtree
//      final NodeReference child_ref = branch.getChild(child_i);
//      // Get the right key of the branch if we can
//      Key new_right_key;
//      if (child_i + 1 < branch.size()) {
//        new_right_key = branch.getKeyValue(child_i + 1);
//      }
//      else {
//        new_right_key = right_key;
//      }
//      // Recurse,
//      return nextKey(new_right_key, key, child_ref);
//    }
//    else {
//      // We hit a leaf node, therefore return the 'right_key' value
//      return right_key;
//    }
//  }

  /**
   * Called by the cache cleanup to flush nodes.  'refs' will be a sorted
   * list of node references that are to be deleted from this tree.  Note that
   * operations may have been performed on this tree and the node list may be
   * slightly out of date so it should not be unexpected if some nodes are
   * not found in the tree.
   */
  void flushNodesToStore(final NodeReference[] refs) throws IOException {
    
    // If not disposed,
    if (!disposed) {

      // Compact the entire tree
      Object[] merge_buffer = new Object[5];
      compactNode(Key.HEAD_KEY, getRootNodeRef(), merge_buffer,
                  Key.HEAD_KEY, Key.TAIL_KEY);

      // Flush the reference node list,
      setRootNodeRef(flushNodes(getRootNodeRef(), refs));

      // Update the version so any data file objects will flush with the
      // changes.
      ++update_version;

      // Check out the changes
      tree_store.checkPoint();
    }
  }

  /**
   * Compacts the given node key in this transaction.
   */
  private void compactNodeKey(Key key) throws IOException {
    Object[] merge_buffer = new Object[5];
    // Compact the node,
    compactNode(Key.HEAD_KEY, getRootNodeRef(), merge_buffer, key, key);
  }

  /**
   * Compacts and then flushes the entire tree. This is used during the commit
   * process to ensure the transaction is fully materialized into the backed
   * data storage scheme.
   */
  protected void checkOut() throws IOException {

    // Compact the entire tree,
    Object[] merge_buffer = new Object[5];
    compactNode(Key.HEAD_KEY, getRootNodeRef(), merge_buffer,
                Key.HEAD_KEY, Key.TAIL_KEY);
    // Write out the changes
    setRootNodeRef(writeNode(getRootNodeRef()));
    
    // Update the version so any data file objects will flush with the
    // changes.
    ++update_version;

    cacheManage();
  }


  /**
   * Sets this transaction to an initial empty state.
   */
  protected void setToEmpty() {

    // Write a root node to the store,
    try {
      // Create an empty head node
      TreeLeaf head_leaf = createEmptyLeaf(Key.HEAD_KEY);
      // Insert a tree identification pattern
      head_leaf.put(0, new byte[] { 1, 1, 1, 1 }, 0, 4);
      // Create an empty tail node
      TreeLeaf tail_leaf = createEmptyLeaf(Key.TAIL_KEY);
      // Insert a tree identification pattern
      tail_leaf.put(0, new byte[] { 1, 1, 1, 1 }, 0, 4);

      // Create a branch,
      TreeBranch root_branch = createEmptyBranch();
      root_branch.set(head_leaf.getReference(), 4,
                      Key.TAIL_KEY,
                      tail_leaf.getReference(), 4);

      setRootNodeRef(root_branch.getReference());

    }
    catch (IOException e) {
      throw new Error(e);
    }

  }

  /**
   * Called during commit to signify that this object has been committed and
   * so sets it to read only and prevents the 'dispose' method from deleting
   * the nodes that were inserted by this transaction.
   */
  protected void notifyCommitted() {
    if (non_committable) {
      throw new AssertionError("Assertion failed, commit non-commitable.");
    }
    if (getRootNodeRef().isInMemory()) {
      throw new AssertionError("Assertion failed, tree on heap.");
    }
    committed = true;
    read_only = true;
  }

  /**
   * Package protected getDataFile method used for accessing system data
   * files.
   */
  protected AddressableDataFile unsafeGetDataFile(Key key, char mode) {
    // Check if the transaction disposed,
    if (disposed) {
      throw new IllegalStateException("Transaction is disposed");
    }
    // Create and return the data file object for this key.
    return new TranDataFile(key, (mode == 'r'));
  }

  /**
   * Package protected getDataFile method used for accessing system data
   * files.
   */
  protected DataRange unsafeGetDataRange(Key min_key, Key max_key) {
    // Check if the transaction disposed,
    if (disposed) {
      throw new IllegalStateException("Transaction is disposed");
    }
    // Create and return the data file object for this key.
    return new TranDataRange(min_key, max_key);
  }

  /**
   * Disposes this transaction.  If the transaction has NOT been committed
   * (committed == false) then all the nodes in the inserted list will be
   * disposed.
   */
  protected void dispose() throws IOException {
    // If it's not already disposed,
    if (!disposed) {
      // Walk the tree and dispose all nodes on the heap,
      disposeHeapNodes(getRootNodeRef());
      if (!committed) {
        // Then dispose all nodes that were inserted during the operation of
        // this transaction
        if (node_inserts != null) {
          int sz = getNodeInserts().size();
          for (int i = 0; i < sz; ++i) {
            NodeReference node_id = getNodeInserts().get(i);
            actualDisposeNode(node_id);
          }
        }
      }
      // If this was committed then we don't dispose any nodes now but wait
      // until the version goes out of scope and then delete the nodes.  This
      // process is handled by the TreeSystem implementation.

      disposed = true;
    }
  }

  // ----- Critical stop passthrough methods -----

  /**
   * Checks the critical stop state.
   */
  private void checkCriticalStop() {
    tree_store.checkCriticalStop();
  }
  /**
   * Pass-through for critical stop exception,
   */
  private Error handleIOException(IOException e) {
    throw tree_store.handleIOException(e);
  }
  private Error handleVMError(VirtualMachineError e) {
    throw tree_store.handleVMError(e);
  }

  // -----

  /**
   * Returns true if the key is out of range of values that are allowed for
   * user keys. (This information should be included in a spec).
   */
  public static boolean outOfUserDataRange(Key key) {
    // These types reserved for system use,
    if (key.getType() >= (short) 0x07F80) {
      return true;
    }
    // Primary key has a reserved group of values at min value
    if (key.getPrimary() <= Long.MIN_VALUE + 16) {
      return true;
    }
    return false;
  }

  /**
   * Returns an iterator over all user Key objects visible to this transaction.
   * The returned iterator is consistent provided there are no updates made to
   * the transaction.  If an update happens then the behaviour of the iterator
   * is undefined.  Note that the returned object maintains the contract that
   * any changes committed to other transaction are isolated from this view of
   * the database.
   * <p>
   * This method is useful for tasks such as making a backup of the user data
   * stored in the database.  I expect in most implementations there will
   * be an order specification to the keys, however the contract does not
   * necessitate any specific order.
   * <p>
   * Note that some databases may have very large key sets and it may
   * take a long time to iterate through all the data.
   * <p>
   * The returned java.util.Iterator does not support the 'remove' method.
   */
  public Iterator<Key> allKeys() {
    return new KeyIterator(getDataRange());
  }
  
  // ---------- Main public methods ----------

  /**
   * Returns true if a DataFile with the given key exists.
   */
  public boolean dataFileExists(Key key) {
    checkCriticalStop();
    try {

      // All key types above 0x07F80 are reserved for system data
      if (outOfUserDataRange(key)) {
        throw new RuntimeException("Key is reserved for system data.");
      }
      // If the key exists, the position will be >= 0
//      return (keyEndPosition(Key.HEAD_KEY, key, 0, root_node_ref, 0) >= 0);
      return keyEndPosition(key) >= 0;

    }
    catch (IOException e) {
      throw handleIOException(e);
    }
    catch (VirtualMachineError e) {
      throw handleVMError(e);
    }
  }

  /**
   * Returns a DataFile that represents the given key.  This method can be
   * called multiple times to create multiple DataFile objects.  If a DataFile
   * is modified, the behaviour of accessing or modifying other data files
   * previously created with the same key as the data file modified is
   * undefined.
   * <p>
   * Mode is the mode the data file will be in.  If mode is 'r' then the
   * DataFile is read-only and may not be written to.
   * <p>
   * This method can be used to create a new DataFile, delete an existing
   * DataFile (set the DataFile size to 0), and check if a data file
   * exists (if the DataFile size is non-zero).
   */
  @Override
  public AddressableDataFile getDataFile(Key key, char mode) {
    checkCriticalStop();
    try {

      // All key types greater than 0x07F80 are reserved for system data
      if (outOfUserDataRange(key)) {
        throw new IllegalArgumentException("Key is reserved for system data");
      }

      // Use the unsafe method after checks have been performed
      return unsafeGetDataFile(key, mode);

    }
    catch (VirtualMachineError e) {
      throw handleVMError(e);
    }
  }

  /**
   * {@inhericDoc}
   */
  @Override
  public DataRange getDataRange(Key min_key, Key max_key) {

    checkCriticalStop();
    try {

      // All key types greater than 0x07F80 are reserved for system data
      if (outOfUserDataRange(min_key) ||
          outOfUserDataRange(max_key)) {
        throw new IllegalArgumentException("Key is reserved for system data");
      }

      // Use the unsafe method after checks have been performed
      return unsafeGetDataRange(min_key, max_key);

    }
    catch (VirtualMachineError e) {
      throw handleVMError(e);
    }

  }

  /**
   * {@inhericDoc}
   */
  @Override
  public DataRange getDataRange() {
    // The full range of user data
    return getDataRange(USER_DATA_MIN, USER_DATA_MAX);
  }

  /**
   * {@inhericDoc}
   */
  @Override
  public void prefetchKeys(Key[] keys) {
    checkCriticalStop();

    try {
      for (Key k : keys) {
        prefetch_keymap.put(k, "");
      }
    }
    catch (VirtualMachineError e) {
      throw handleVMError(e);
    }

  }

//  /**
//   * {@inhericDoc}
//   */
//  @Override
//  public void deleteKeyRange(Key key_min_bound, Key key_max_bound) {
//    try {
//
//      if (key_min_bound.compareTo(key_max_bound) > 0) {
//        throw new IllegalArgumentException("key_min_bound > key_max_bound");
//      }
//
//      // Increment the update version,
//      ++update_version;
//
//      // Update the lowest sized changed key
//      if (key_min_bound.compareTo(lowest_size_changed_key) < 0) {
//        lowest_size_changed_key = key_min_bound;
//      }
//
//      // The absolute start and end position of the key range.
//      long abs_start_pos = keyEndPosition(previousKeyOrder(key_min_bound));
//      long abs_end_pos = keyEndPosition(key_max_bound);
//
//      if (abs_start_pos < 0) {
//        abs_start_pos = -(abs_start_pos + 1);
//      }
//      if (abs_end_pos < 0) {
//        abs_end_pos = -(abs_end_pos + 1);
//      }
//
//      // Remove the data,
//      removeAbsoluteBounds(abs_start_pos, abs_end_pos);
//
//      // Do a cache operation
//      cacheManage();
//
//    }
//    catch (IOException e) {
//      throw handleIOException(e);
//    }
//    catch (VirtualMachineError e) {
//      throw handleVMError(e);
//    }
//  }

  /**
   * Sets this transaction to read-only.  After this is called, all data
   * change operations will generate an exception.
   */
  public void setReadOnly() {
    checkCriticalStop();
    read_only = true;
  }

  /**
   * Static function that copies an amount of data from the source DataFile to
   * the target DataFile through a byte buffer.
   */
  private static void byteBufferCopyTo(
              DataFile source, DataFile target, long size) throws IOException {
    long pos = target.position();
    // Make room to insert the data
    target.shift(size);
    target.position(pos);
    // Set a 1k buffer
    byte[] buf = new byte[1024];
    // While there is data to copy,
    while (size > 0) {
      // Read an amount of data from the source
      int to_read = (int) Math.min((long) buf.length, size);
      // Read it into the buffer
      source.get(buf, 0, to_read);
      // Write from the buffer out to the target
      target.put(buf, 0, to_read);
      // Update the ref
      size = size - to_read;
    }
  }



  // ---------- Inner classes ----------

  /**
   * An implementation of a DataFile.
   */
  private class TranDataFile implements AddressableDataFile {

    // The Key
    private final Key key;

    // The current absolute position
    private long p;

    // The current version of the bounds information.  If it is out of date
    // it must be updated.
    private long version;
    // The current absolute start position
    private long start;
    // The current absolute position (changes when modification happens)
    private long end;

    // Tree stack
    private final TreeSystemStack stack;

    // A small buffer used for converting primitives
    private final byte[] convert_buffer;

    // True if locally, this transaction is read only
    private final boolean file_read_only;
    

    TranDataFile(Key key, boolean file_read_only) {
      this.stack = new TreeSystemStack(TreeSystemTransaction.this);
      this.key = key;
      this.p = 0;
      this.convert_buffer = new byte[8];

      this.version = -1;
      this.file_read_only = file_read_only;
      this.start = -1;
      this.end = -1;
    }

    /**
     * Returns the TreeSystem object for this data file.
     */
    TreeSystem getTreeSystem() {
      return tree_store;
    }

    /**
     * Returns the transaction object for this data file.
     */
    TreeSystemTransaction getTransaction() {
      return TreeSystemTransaction.this;
    }

    /**
     * Sets up the correct bounds.
     */
    private void ensureCorrectBounds() throws IOException {
      if (update_version > version) {
        
        // If version is -1, we force a key position lookup.  Version is -1
        // when the file is created or it undergoes a large structural change
        // such as a copy.
        if (version == -1 || key.compareTo(lowest_size_changed_key) >= 0) {

          long[] bounds = getDataFileBounds(key);
          start = bounds[0];
          end = bounds[1];

        }
        else {
          // If version doesn't equal -1, and this key is lower than the lowest
          // size changed key, then 'start' and 'end' should be correct.
//          System.out.println("lowest_updated_key = " + lowest_size_changed_key);
        }
        // Reset the stack and set the version to the most recent
        stack.reset();
        version = update_version;
      }
    }

    /**
     * Checks the pointer position to ensure that the bounds between the
     * current pointer and pointer + len falls within the address space of
     * this key.
     */
    private void checkAccessSize(int len) {
      if (p < 0 || p > (end - start - len)) {
        String msg = MessageFormat.format(
                "Position out of bounds (p = {0}, size = {1}, read_len = {2})",
                p, end - start, len);
        throw new IndexOutOfBoundsException(msg);
      }
    }
    
    /**
     * Generates an exception if this is a read only data file.
     */
    private void initWrite() {
      // Generate exception if this is read-only.
      // Either the transaction is read only or the file is read only
      if (read_only) {
        throw new RuntimeException("Read only transaction.");
      }
      if (file_read_only) {
        throw new RuntimeException("Read only data file.");
      }

      // On writing, we update the versions
      if (version >= 0) {
        ++version;
      }
      ++update_version;
    }

    /**
     * Updates the lowest size changed key.
     */
    private void updateLowestSizeChangedKey() {
      // Update the lowest sized changed key
      if (key.compareTo(lowest_size_changed_key) < 0) {
        lowest_size_changed_key = key;
      }
    }
    
    /**
     * Ensures there are at least n bytes in the file.  If there aren't then
     * space is made for the additional bytes.
     * <p>
     * Note that 'end_point' is an absolute position.
     */
    private void ensureBounds(long end_point) throws IOException {
      // The number of bytes to expand by
      long to_expand_by = end_point - end;
      
      // If we need to expand,
      if (to_expand_by > 0) {
        final long size_diff = to_expand_by;
        // Go to the end position,
        stack.setupForPosition(key, Math.max(start, end - 1));
        // Did we find a leaf for this key?
        if (!stack.getCurrentLeafKey().equals(key)) {
          // No, so add empty nodes after to make up the space
          stack.addSpaceAfter(key, to_expand_by);
        }
        else {
          // Otherwise, try to expand the current leaf,
          to_expand_by -= stack.expandLeaf(to_expand_by);
          // And add nodes for the remaining
          stack.addSpaceAfter(key, to_expand_by);
        }
        end = end_point;

        // Update the state because this key changed the relative offset of
        // the keys ahead of it.
        updateLowestSizeChangedKey();
      }
    }




    /**
     * Copies size bytes from this file from the current position to the
     * 'target_position' with the given target key.  This may write out
     * nodes to disk from this data as part of the copy operation.  This is
     * not able to copy data within the same data file, however it is able to
     * copy the data to the same Key if the DataFile is created by a
     * different transaction.
     * <p>
     * Positions are all absolutes.
     * <p>
     * Conditions that must hold true before calling this method;
     * 'target_data_file' comes from the same TreeSystem.  'size' must not
     * exceed the remaining data in the file from the position.
     * 'target_data_file' does not share the same Key as this object unless
     * the transaction is different.
     */
    private void copyDataTo(long position,
                         TranDataFile target_data_file, long target_position,
                         long size) throws IOException {

      // If transactions are the same (data is being copied within the same
      // transaction context).
      TreeSystemStack target_stack;
      TreeSystemStack source_stack;
      // Keys
      final Key target_key = target_data_file.key;
      final Key source_key = key;

      boolean modify_pos_on_shift = false;
      if (target_data_file.getTransaction() == getTransaction()) {
        // We set the source and target stack to the same
        source_stack = target_data_file.stack;
        target_stack = source_stack;
        // If same transaction and target_position is before the position we
        // set the modify_pos_on_shift boolean.  This will update the absolute
        // position when data is copied.
        modify_pos_on_shift = (target_position <= position);
      }
      else {
        // Otherwise, set the target stack to the target file's stack
        source_stack = stack;
        target_stack = target_data_file.stack;
      }

      
      // Compact the key we are copying from, and in the destination,
      compactNodeKey(source_key);
      target_data_file.compactNodeKey(target_key);

      
      // The process works as follows;
      // 1. If we are not positioned at the start of a leaf, copy all data up
      //    to the next leaf to the target.
      // 2. Split the target leaf at the new position if the leaf can be
      //    split into 2 leaf nodes.
      // 3. Copy every full leaf to the target as a new leaf element.
      // 4. If there is any remaining data to copy, insert it into the target.

      // Set up for the position
      source_stack.setupForPosition(source_key, position);
      // If we aren't at the start of the leaf, then copy the data to the
      // target.
      int leaf_off = source_stack.getLeafOffset();
      if (leaf_off > 0) {
        // We copy the remaining data in the leaf to the target
        // The amount of data to copy from the leaf to the target
        int to_copy = (int) Math.min(size, source_stack.leafSize() - leaf_off);
        if (to_copy > 0) {
          // Read into a buffer
          byte[] buf = new byte[to_copy];
          source_stack.getCurrentLeaf().get(leaf_off, buf, 0, to_copy);
          // Make enough room to insert this data in the target
          target_stack.shiftData(target_key, target_position, to_copy);
          // Update the position if necessary
          if (modify_pos_on_shift) {
            position += to_copy;
          }
          // Write the data to the target stack
          target_stack.writeFrom(target_key, target_position, buf, 0, to_copy);
          // Increment the pointers
          position += to_copy;
          target_position += to_copy;
          size -= to_copy;
        }
      }

      // If this is true, the next iteration will use the byte buffer leaf copy
      // routine.  Set if a link to a node failed for whatever reason.
      boolean use_byte_buffer_copy_for_next = false;

      // The loop
      while (size > 0) {

        // We now know we are at the start of a leaf with data left to copy.
        source_stack.setupForPosition(source_key, position);
        // Lets assert that
        if (source_stack.getLeafOffset() != 0) {
          throw new RuntimeException("Expected to be at the start of a leaf.");
        }

        // If the source is a heap node or we are copying less than the data
        // that's in the leaf then we use the standard shift and write.
        TreeLeaf current_leaf = source_stack.getCurrentLeaf();
        // Check the leaf size isn't 0
        if (current_leaf.getSize() <= 0) {
          throw new RuntimeException("Leaf is empty.");
        }
        // If the remaining copy is less than the size of the leaf we are
        // copying from, we just do a byte array copy
        if (use_byte_buffer_copy_for_next || size < current_leaf.getSize()) {
          // Standard copy through a byte[] buf,
          use_byte_buffer_copy_for_next = false;
          int to_copy = (int) Math.min(size, current_leaf.getSize());
          // Read into a buffer
          byte[] buf = new byte[to_copy];
          current_leaf.get(0, buf, 0, to_copy);
          // Make enough room in the target
          target_stack.shiftData(target_key, target_position, to_copy);
          if (modify_pos_on_shift) {
            position += to_copy;
          }
          // Write the data and finish
          target_stack.writeFrom(target_key, target_position, buf, 0, to_copy);
          // Update pointers
          position += to_copy;
          target_position += to_copy;
          size -= to_copy;
        }
        else {
          // We need to copy a complete leaf node,
          // If the leaf is on the heap, write it out
          if (isHeapNode(current_leaf.getReference())) {
            source_stack.writeLeafOnly(source_key);
            // And update any vars
            current_leaf = source_stack.getCurrentLeaf();
          }

          // Ok, source current leaf isn't on the heap, and we are copying a
          // complete leaf node, so we are elegible to play with pointers to
          // copy the data.
          target_stack.setupForPosition(target_key, target_position);
          boolean insert_next_before = false;
          // Does the target key exist?
          boolean target_key_exists =
                      target_stack.getCurrentLeafKey().equals(target_key);
          if (target_key_exists) {
            // If the key exists, is target_position at the end of the span?
            insert_next_before =
                    target_stack.getLeafOffset() <
                    target_stack.getCurrentLeaf().getSize();
          }
          
          // If target isn't currently on a boundary
          if (!target_stack.isAtEndOfKeyData() &&
               target_stack.getLeafOffset() != 0) {
            // If we aren't on a boundary we need to split the target leaf
            target_stack.splitLeaf(target_key, target_position);
          }
          // If the key exists we set up the position to the previous left
          // to insert the new leaf, otherwise we set it up to the default
          // position to insert.
//          if (target_key_exists) {
////            target_stack.moveToPreviousLeaf(target_key);
//            target_stack.setupForPosition(target_key, target_position - 1);
//          }
//          else {
//            target_stack.setupForPosition(target_key, target_position);
//          }
          // Copy the leaf,
          // Try to link to this leaf
          boolean link_successful =
                  getTreeSystem().linkLeaf(target_key, current_leaf.getReference());
          // If the link was successful,
          if (link_successful) {
            // Insert the leaf into the tree
            target_stack.insertLeaf(target_key, current_leaf, insert_next_before);
            // Update the pointers
            int copied_size = current_leaf.getSize();
            // Update if we inserting stuff before
            if (modify_pos_on_shift) {
              position += copied_size;
            }
            position += copied_size;
            target_position += copied_size;
            size -= copied_size;
          }
          // If the link was not successful,
          else {
            // We loop back and use the byte buffer copy,
            use_byte_buffer_copy_for_next = true;
          }
        }

      }

    }

    /**
     * Compacts the node key in this tree.
     */
    private void compactNodeKey(Key key) throws IOException {
      TreeSystemTransaction.this.compactNodeKey(key);
    }

    /**
     * Shifts elements at the given position forward or backwards by the given
     * amount.
     * <p>
     * Note that 'position' is an absolute position.
     */
    private void shiftData(long position, final long shift_offset)
                                                           throws IOException {

      // Make some assertions
      long end_pos = position + shift_offset;
      if (position < start || position > end) {
//        System.out.println("position = " + position);
//        System.out.println("start = " + start);
//        System.out.println("end = " + end);
        throw new RuntimeException("Position is out of bounds.");
      }
      // Make sure the ending position can't be before the start
      if (end_pos < start) {
        throw new RuntimeException("Can't shift to before start boundary.");
      }
      stack.shiftData(key, position, shift_offset);
      end += shift_offset;
      if (end < start) {
        throw new RuntimeException("Assertion failed: end < start");
//        start = end;
      }

      // Update the state because this key changed the relative offset of
      // the keys ahead of it.
      updateLowestSizeChangedKey();
    }
    
    
    
    public long size() {
      checkCriticalStop();
      try {

        ensureCorrectBounds();
        return end - start;

      }
      catch (IOException e) {
        throw handleIOException(e);
      }
      catch (VirtualMachineError e) {
        throw handleVMError(e);
      }
    }

    public long position() {
      return p;
    }

    public void position(long position) {
      this.p = position;
    }

    // ---------- Accessor methods ----------

    public byte get() {
      checkCriticalStop();
      try {

        byte b;

        ensureCorrectBounds();
        checkAccessSize(1);
        stack.setupForPosition(key, start + p);
        ++p;
        b = stack.getCurrentLeaf().get(stack.getLeafOffset());

        return b;

      }
      catch (IOException e) {
        throw handleIOException(e);
      }
      catch (VirtualMachineError e) {
        throw handleVMError(e);
      }
    }

    public void get(byte[] buf, int off, int len) {
      checkCriticalStop();
      try {

        ensureCorrectBounds();
        checkAccessSize(len);
        stack.readInto(key, start + p, buf, off, len);
        p += len;

      }
      catch (IOException e) {
        throw handleIOException(e);
      }
      catch (VirtualMachineError e) {
        throw handleVMError(e);
      }
    }

    public short getShort() {
      checkCriticalStop();
      try {

        short s;

        ensureCorrectBounds();
        checkAccessSize(2);
        stack.readInto(key, start + p, convert_buffer, 0, 2);
        p += 2;
        s = ByteArrayUtil.getShort(convert_buffer, 0);

        return s;

      }
      catch (IOException e) {
        throw handleIOException(e);
      }
      catch (VirtualMachineError e) {
        throw handleVMError(e);
      }
    }

    public int getInt() {
      checkCriticalStop();
      try {
      
        int i;

        ensureCorrectBounds();
        checkAccessSize(4);
        stack.readInto(key, start + p, convert_buffer, 0, 4);
        p += 4;
        i = ByteArrayUtil.getInt(convert_buffer, 0);

        return i;

      }
      catch (IOException e) {
        throw handleIOException(e);
      }
      catch (VirtualMachineError e) {
        throw handleVMError(e);
      }
    }

    public long getLong() {
      checkCriticalStop();
      try {

        long l;

        ensureCorrectBounds();
        checkAccessSize(8);
        stack.readInto(key, start + p, convert_buffer, 0, 8);
        p += 8;
        l = ByteArrayUtil.getLong(convert_buffer, 0);

        return l;

      }
      catch (IOException e) {
        throw handleIOException(e);
      }
      catch (VirtualMachineError e) {
        throw handleVMError(e);
      }
    }

    public char getChar() {
      checkCriticalStop();
      try {

        char c;

        ensureCorrectBounds();
        checkAccessSize(2);
        stack.readInto(key, start + p, convert_buffer, 0, 2);
        p += 2;
        c = ByteArrayUtil.getChar(convert_buffer, 0);

        return c;
      
      }
      catch (IOException e) {
        throw handleIOException(e);
      }
      catch (VirtualMachineError e) {
        throw handleVMError(e);
      }
    }


//    public DataFile getImmutableSubset(long p1, long p2) {
//
//      ensureCorrectBounds();
//      long size = (end - start);
//      
//      // Check points are within the bounds,
//      if (p1 < 0 || p2 > size) {
//        throw new RuntimeException("Out of bounds.");
//      }
//      if (p1 > p2) {
//        throw new RuntimeException("p1 > p2");
//      }
//      // Find the absolute positions in this data file of the points, 
//      long abs_p1 = start + p1;
//      long abs_p2 = start + p2;
//      // Create and return the subset file
//      return new ImmutableDataFile(this.key, abs_p1, abs_p2);
//
//    }


    public void setSize(long size) {
      checkCriticalStop();
      try {

        initWrite();
        ensureCorrectBounds();
  //      checkAccessSize(0);

  //      System.out.println("start = " + start);
  //      System.out.println("end = " + end);
  //      System.out.println("size = " + size);
  //      System.out.println("size - end = " + (size - end));

        long current_size = end - start;
        shiftData(end, size - current_size);

        cacheManage();

      }
      catch (IOException e) {
        throw handleIOException(e);
      }
      catch (VirtualMachineError e) {
        throw handleVMError(e);
      }
    }

    public void delete() {
      checkCriticalStop();
      try {

        initWrite();
        ensureCorrectBounds();

        shiftData(end, start - end);

        cacheManage();
      
      }
      catch (IOException e) {
        throw handleIOException(e);
      }
      catch (VirtualMachineError e) {
        throw handleVMError(e);
      }
    }

    public void shift(long offset) {
      checkCriticalStop();
      try {

        initWrite();
        ensureCorrectBounds();
        checkAccessSize(0);

        shiftData(start + p, offset);

        cacheManage();

      }
      catch (IOException e) {
        throw handleIOException(e);
      }
      catch (VirtualMachineError e) {
        throw handleVMError(e);
      }
    }

    public void put(byte b) {
      checkCriticalStop();
      try {

        initWrite();
        ensureCorrectBounds();
        checkAccessSize(0);

        // Ensure that there is address space available for writing this.
        ensureBounds(start + p + 1);
        convert_buffer[0] = b;
        stack.writeFrom(key, start + p, convert_buffer, 0, 1);
        ++p;

        cacheManage();

      }
      catch (IOException e) {
        throw handleIOException(e);
      }
      catch (VirtualMachineError e) {
        throw handleVMError(e);
      }
    }

    public void put(byte[] buf, int off, int len) {
      checkCriticalStop();
      try {

        initWrite();
        ensureCorrectBounds();
        checkAccessSize(0);

        // Ensure that there is address space available for writing this.
        ensureBounds(start + p + len);
        stack.writeFrom(key, start + p, buf, off, len);
        p += len;

        cacheManage();

      }
      catch (IOException e) {
        throw handleIOException(e);
      }
      catch (VirtualMachineError e) {
        throw handleVMError(e);
      }
    }

    public void put(byte[] buf) {
      put(buf, 0, buf.length);
    }

    public void putShort(short s) {
      checkCriticalStop();
      try {

        initWrite();
        ensureCorrectBounds();
        checkAccessSize(0);

        // Ensure that there is address space available for writing this value.
        ensureBounds(start + p + 2);

        ByteArrayUtil.setShort(s, convert_buffer, 0);
        stack.writeFrom(key, start + p, convert_buffer, 0, 2);
        p += 2;

        cacheManage();
      
      }
      catch (IOException e) {
        throw handleIOException(e);
      }
      catch (VirtualMachineError e) {
        throw handleVMError(e);
      }
    }

    public void putInt(int i) {
      checkCriticalStop();
      try {

        initWrite();
        ensureCorrectBounds();
        checkAccessSize(0);

        // Ensure that there is address space available for writing this value.
        ensureBounds(start + p + 4);

        ByteArrayUtil.setInt(i, convert_buffer, 0);
        stack.writeFrom(key, start + p, convert_buffer, 0, 4);
        p += 4;

        cacheManage();

      }
      catch (IOException e) {
        throw handleIOException(e);
      }
      catch (VirtualMachineError e) {
        throw handleVMError(e);
      }
    }

    public void putLong(long l) {
      checkCriticalStop();
      try {

        initWrite();
        ensureCorrectBounds();
        checkAccessSize(0);

        // Ensure that there is address space available for writing this value.
        ensureBounds(start + p + 8);

        ByteArrayUtil.setLong(l, convert_buffer, 0);
        stack.writeFrom(key, start + p, convert_buffer, 0, 8);
        p += 8;

        cacheManage();

      }
      catch (IOException e) {
        throw handleIOException(e);
      }
      catch (VirtualMachineError e) {
        throw handleVMError(e);
      }
    }

    public void putChar(char c) {
      checkCriticalStop();
      try {

        initWrite();
        ensureCorrectBounds();
        checkAccessSize(0);

        // Ensure that there is address space available for writing this value.
        ensureBounds(start + p + 2);

        ByteArrayUtil.setChar(c, convert_buffer, 0);
        stack.writeFrom(key, start + p, convert_buffer, 0, 2);
        p += 2;

        cacheManage();

      }
      catch (IOException e) {
        throw handleIOException(e);
      }
      catch (VirtualMachineError e) {
        throw handleVMError(e);
      }
    }

    public void copyFrom(DataFile from, long size) {
      checkCriticalStop();
      try {

        // The actual amount of data to really copy
        size = Math.min(from.size() - from.position(), size);
        // Return if we aren't doing anything
        if (size <= 0) {
          return;
        }

//        if (true) {
//          // Use byte buffer copy,
//          byteBufferCopyTo(from, this, size);
//          return;
//        }


        // If the data file is not addressable,
        if (!(from instanceof AddressableDataFile)) {
          // Use byte buffer copy,
          byteBufferCopyTo(from, this, size);
          return;
        }

        AddressableDataFile adf_from = (AddressableDataFile) from;
        // Ask the destination for information about the content to copy,
        Object meta = adf_from.getBlockLocationMeta(from.position(), size);

        // If the meta is not of the expected class,
        if (meta == null || !(meta instanceof TranLocationMeta)) {
          // Use byte buffer copy,
          byteBufferCopyTo(from, this, size);
          return;
        }

        // Cast it,
        TranLocationMeta ameta = (TranLocationMeta) meta;

        // If the tree systems are different, then byte buffer copy.
        if (getTreeSystem() != ameta.getTreeSystem()) {
          byteBufferCopyTo(from, this, size);
          return;
        }

        TranDataFile t_from = ameta.getDataFile();

        // Fail condition (same key and same transaction),
        if (t_from.key.equals(key) &&
            ameta.getTransaction() == getTransaction()) {
          throw new IllegalArgumentException(
                          "Can not use 'copyFrom' to copy data within a file");
        }

        // initWrite on this and target. The reason we do this is because we
        // may change the root node on either source or target.  We need to
        // initWrite on this object even though the data may not change,
        // because we may be writing out data from the heap as part of the
        // copy operation and the root node may change
        initWrite();
        t_from.initWrite();
        // Make sure internal vars are setup correctly
        ensureCorrectBounds();
        t_from.ensureCorrectBounds();
        // Remember the source and target positions
//        long init_spos = t_from.position();
        long init_spos = ameta.getStartPosition();
        long init_tpos = position();
        // The target shares the same tree system, therefore we may be able
        // to optimize the copy.
//        System.out.println("OPT copy");
        t_from.copyDataTo(t_from.start + init_spos,
                          this, start + init_tpos,
                          size);
        // Update the positions
        t_from.position(init_spos + size);
        position(init_tpos + size);
        // Reset version to force a bound update
        this.version = -1;
        t_from.version = -1;
        this.updateLowestSizeChangedKey();
        this.getTransaction().cacheManage();

//        // If the target isn't a TranDataFile then use standard byte buffer copy.
//        if (!(from instanceof TranDataFile)) {
//          byteBufferCopyTo(from, this, size);
//          return;
//        }
//        // If the tree systems are different, then byte buffer copy.
//        TranDataFile t_from = (TranDataFile) from;
//        if (getTreeSystem() != t_from.getTreeSystem()) {
//          byteBufferCopyTo(from, this, size);
//          return;
//        }
//        // Fail condition (same key and same transaction),
//        if (t_from.key.equals(key) &&
//            t_from.getTransaction() == getTransaction()) {
//          throw new IllegalArgumentException(
//                          "Can not use 'copyFrom' to copy data within a file");
//        }
//
//        // initWrite on this and target. The reason we do this is because we
//        // may change the root node on either source or target.  We need to
//        // initWrite on this object even though the data may not change,
//        // because we may be writing out data from the heap as part of the
//        // copy operation and the root node may change
//        initWrite();
//        t_from.initWrite();
//        // Make sure internal vars are setup correctly
//        ensureCorrectBounds();
//        t_from.ensureCorrectBounds();
//        // Remember the source and target positions
//        long init_spos = t_from.position();
//        long init_tpos = position();
//        // Ok, the target shares the same tree system, therefore we may be able
//        // to optimize the copy.
//        t_from.copyDataTo(t_from.start + t_from.position(),
//                          this, start + position(),
//                          size);
//        // Update the positions
//        t_from.position(init_spos + size);
//        position(init_tpos + size);
//        // Reset version to force a bound update
//        this.version = -1;
//        t_from.version = -1;
//        this.updateLowestSizeChangedKey();
//        this.getTransaction().cacheManage();

      }
      catch (IOException e) {
        throw handleIOException(e);
      }
      catch (VirtualMachineError e) {
        throw handleVMError(e);
      }
    }

    public void replicateFrom(DataFile from) {
      // TODO: Placeholder implementation,
      this.position(0);
      this.delete();
      from.position(0);
      this.copyFrom(from, from.size());
    }

    // Legacy
    public void copyTo(DataFile target, long size) {
      target.copyFrom(this, size);
    }

    // Legacy
    public void replicateTo(DataFile target) {
      target.replicateFrom(this);
    }

    public Object getBlockLocationMeta(long start_position, long end_position) {
      return new TranLocationMeta(getTreeSystem(), getTransaction(),
                                  this, start_position, end_position);
    }

//    public void copyTo(DataFile target, long size) {
//      checkCriticalStop();
//      try {
//
//        // The actual amount of data to really copy
//        size = Math.min(size() - position(), size);
//        // Return if we aren't doing anything
//        if (size <= 0) {
//          return;
//        }
//
//        // If the target isn't a TranDataFile then use standard byte buffer copy.
//        if (!(target instanceof TranDataFile)) {
//          byteBufferCopyTo(this, target, size);
//          return;
//        }
//        // If the tree systems are different, then byte buffer copy.
//        TranDataFile t_target = (TranDataFile) target;
//        if (getTreeSystem() != t_target.getTreeSystem()) {
//          byteBufferCopyTo(this, target, size);
//          return;
//        }
//        // Fail condition (same key and same transaction),
//        if (t_target.key.equals(key) &&
//            t_target.getTransaction() == getTransaction()) {
//          throw new IllegalArgumentException(
//                            "Can not use 'copyTo' to copy data within a file");
//        }
//
//        // initWrite on this and target. The reason we do this is because we
//        // may change the root node on either source or target.  We need to
//        // initWrite on this object even though the data may not change,
//        // because we may be writing out data from the heap as part of the
//        // copy operation and the root node may change
//        initWrite();
//        t_target.initWrite();
//        // Make sure internal vars are setup correctly
//        ensureCorrectBounds();
//        t_target.ensureCorrectBounds();
//        // Remember the source and target positions
//        long init_spos = position();
//        long init_tpos = t_target.position();
//        // Ok, the target shares the same tree system, therefore we may be able
//        // to optimize the copy.
//        copyDataTo(start + position(),
//                    t_target, t_target.start + t_target.position(),
//                    size);
//        // Update the positions
//        position(init_spos + size);
//        t_target.position(init_tpos + size);
//        // Reset version to force a bound update
//        this.version = -1;
//        t_target.version = -1;
//        t_target.updateLowestSizeChangedKey();
//        t_target.getTransaction().cacheManage();
//
//      }
//      catch (IOException e) {
//        throw handleIOException(e);
//      }
//      catch (VirtualMachineError e) {
//        throw handleVMError(e);
//      }
//    }
//
//    public void replicateTo(DataFile target) {
//      // TODO: Placeholder implementation,
//      target.position(0);
//      target.delete();
//      position(0);
//      copyTo(target, size());
//    }

  }


  /**
   * Implementation of DataRange.
   */
  private class TranDataRange implements DataRange {

    // The lower and upper bounds of the range
    private final Key lower_key;
    private final Key upper_key;

    // The current absolute position
    private long p;

    // The current version of the bounds information.  If it is out of date
    // it must be updated.
    private long version;
    // The current absolute start position
    private long start;
    // The current absolute position (changes when modification happens)
    private long end;

    // Tree stack
    private final TreeSystemStack stack;



    /**
     * Constructor.
     */
    TranDataRange(Key lower_key, Key upper_key) {
      this.stack = new TreeSystemStack(TreeSystemTransaction.this);
      this.lower_key = previousKeyOrder(lower_key);
      this.upper_key = upper_key;
      this.p = 0;
//      this.convert_buffer = new byte[8];

      this.version = -1;
      this.start = -1;
      this.end = -1;
    }

    /**
     * Returns the TreeSystem object for this data range.
     */
    TreeSystem getTreeSystem() {
      return tree_store;
    }

    /**
     * Returns the transaction object for this data range.
     */
    TreeSystemTransaction getTransaction() {
      return TreeSystemTransaction.this;
    }

    /**
     * Sets up the correct bounds.
     */
    private void ensureCorrectBounds() throws IOException {
      if (update_version > version) {
        // If version is -1, we force a key position lookup. Version is -1
        // when the range is created or it undergoes a large structural change.
        if (version == -1) {
          // Calculate absolute upper bound,
          end = absKeyEndPosition(upper_key);
          // Calculate the lower bound,
          start = absKeyEndPosition(lower_key);
        }
        else {
          if (upper_key.compareTo(lowest_size_changed_key) >= 0) {
            // Calculate absolute upper bound,
            end = absKeyEndPosition(upper_key);
          }
          if (lower_key.compareTo(lowest_size_changed_key) > 0) {
            // Calculate the lower bound,
            start = absKeyEndPosition(lower_key);
          }
        }
        // Reset the stack and set the version to the most recent
        stack.reset();
        version = update_version;
      }
    }

    /**
     * Checks the pointer position to ensure that the bounds between the
     * current pointer and pointer + len falls within the address space of
     * this key.
     */
    private void checkAccessSize(int len) {
      if (p < 0 || p > (end - start - len)) {
        String msg = MessageFormat.format(
                "Position out of bounds (p = {0}, size = {1}, read_len = {2})",
                p, end - start, len);
        throw new IndexOutOfBoundsException(msg);
      }
    }

    /**
     * Generates an exception if this is a read only data file.
     */
    private void initWrite() {
      // Generate exception if the backed transaction is read-only.
      if (read_only) {
        throw new RuntimeException("Read only transaction.");
      }

      // On writing, we update the versions
      if (version >= 0) {
        ++version;
      }
      ++update_version;
    }

    // -----

    public long size() {
      checkCriticalStop();
      try {

        ensureCorrectBounds();
        return end - start;

      }
      catch (IOException e) {
        throw handleIOException(e);
      }
      catch (VirtualMachineError e) {
        throw handleVMError(e);
      }
    }

    public long position() {
      return p;
    }

    public void position(long position) {
      this.p = position;
    }

    public Key keyAtPosition() {
      checkCriticalStop();
      try {

        ensureCorrectBounds();
        checkAccessSize(1);

        stack.setupForPosition(Key.TAIL_KEY, start + p);
        return stack.getCurrentLeafKey();

      }
      catch (IOException e) {
        throw handleIOException(e);
      }
      catch (VirtualMachineError e) {
        throw handleVMError(e);
      }
    }

    public long positionOnKeyStart() {
      checkCriticalStop();
      try {

        ensureCorrectBounds();
        checkAccessSize(1);

        stack.setupForPosition(Key.TAIL_KEY, start + p);
        Key cur_key = stack.getCurrentLeafKey();
        long start_of_cur =
                         absKeyEndPosition(previousKeyOrder(cur_key)) - start;
        p = start_of_cur;
        return p;

      }
      catch (IOException e) {
        throw handleIOException(e);
      }
      catch (VirtualMachineError e) {
        throw handleVMError(e);
      }
    }

    public long positionOnNextKey() {
      checkCriticalStop();
      try {

        ensureCorrectBounds();
        checkAccessSize(1);

        stack.setupForPosition(Key.TAIL_KEY, start + p);
        Key cur_key = stack.getCurrentLeafKey();
        long start_of_next = absKeyEndPosition(cur_key) - start;
        p = start_of_next;
        return p;

      }
      catch (IOException e) {
        throw handleIOException(e);
      }
      catch (VirtualMachineError e) {
        throw handleVMError(e);
      }
    }

    public long positionOnPreviousKey() {
      checkCriticalStop();
      try {

        ensureCorrectBounds();
        checkAccessSize(0);

        // TODO: This seems rather complicated. Any way to simplify?

        // Special case, if we are at the end,
        long start_of_cur;
        if (p == (end - start)) {
          start_of_cur = p;
        }
        //
        else {
          stack.setupForPosition(Key.TAIL_KEY, start + p);
          Key cur_key = stack.getCurrentLeafKey();
          start_of_cur = absKeyEndPosition(previousKeyOrder(cur_key)) - start;
        }
        // If at the start then we can't go to previous,
        if (start_of_cur == 0) {
          throw new IndexOutOfBoundsException("On first key");
        }
        // Decrease the pointer and find the key and first position of that
        --start_of_cur;
        stack.setupForPosition(Key.TAIL_KEY, start + start_of_cur);
        Key prev_key = stack.getCurrentLeafKey();
        long start_of_prev =
                       absKeyEndPosition(previousKeyOrder(prev_key)) - start;

        p = start_of_prev;
        return p;

      }
      catch (IOException e) {
        throw handleIOException(e);
      }
      catch (VirtualMachineError e) {
        throw handleVMError(e);
      }
    }

    public AddressableDataFile getDataFile(char mode) {
      checkCriticalStop();
      try {

        ensureCorrectBounds();
        checkAccessSize(1);

        stack.setupForPosition(Key.TAIL_KEY, start + p);
        Key cur_key = stack.getCurrentLeafKey();

        return TreeSystemTransaction.this.getDataFile(cur_key, mode);

      }
      catch (IOException e) {
        throw handleIOException(e);
      }
      catch (VirtualMachineError e) {
        throw handleVMError(e);
      }
    }

    public AddressableDataFile getDataFile(Key key, char mode) {
      checkCriticalStop();
      try {

        // Check the key is within range,
        if (key.compareTo(lower_key) < 0 ||
            key.compareTo(upper_key) > 0) {
          throw new IndexOutOfBoundsException("Key out of bounds");
        }

        return getDataFile(key, mode);

      }
      catch (VirtualMachineError e) {
        throw handleVMError(e);
      }
    }

    public void replicateFrom(DataRange from) {
      if (from instanceof TranDataRange) {
        // If the tree systems are different we fall back
        TranDataRange t_from = (TranDataRange) from;
        if (getTreeSystem() == t_from.getTreeSystem()) {
          // Fail condition (same transaction),
          if (t_from.getTransaction() == getTransaction()) {
            throw new IllegalArgumentException(
                                   "'replicateFrom' on the same transaction");
          }

          // Ok, different transaction, same tree system source, both
          // TranDataRange objects, so we can do an efficient tree copy.

          // PENDING,


        }
      }

      // The fallback method,
      // This uses the standard API to replicate all the keys in the target
      // range.
      // Note that if the target can't contain the keys because they fall
      //  outside of its bound then the exception comes from the target.
      delete();
      long sz = from.size();
      long pos = 0;
      while (pos < sz) {
        from.position(pos);
        Key key = from.keyAtPosition();
        DataFile df = from.getDataFile('r');
        DataFile target_df = getDataFile(key, 'w');
        target_df.replicateFrom(df);
        pos = from.positionOnNextKey();
      }

    }

    public void delete() {
      checkCriticalStop();
      try {

        initWrite();
        ensureCorrectBounds();

        if (end > start) {
          // Remove the data,
          removeAbsoluteBounds(start, end);
        }
        if (end < start) {
          // Should ever happen?
          throw new RuntimeException("end < start");
        }

        cacheManage();

      }
      catch (IOException e) {
        throw handleIOException(e);
      }
      catch (VirtualMachineError e) {
        throw handleVMError(e);
      }
    }

  }


  /**
   * Key iterator implementation over a data range.
   */
  private static class KeyIterator implements Iterator<Key> {

    private final DataRange range;

    KeyIterator(DataRange range) {
      this.range = range;
    }

    public boolean hasNext() {
      return range.position() < range.size();
    }

    public Key next() {
      Key key = range.keyAtPosition();
      range.positionOnNextKey();
      return key;
    }

    public void remove() {
      throw new UnsupportedOperationException(
                                      "Remove not supported on key iterator");
    }

  }


  /**
   * An object that describes the addressable range of a block of data.
   */
  private static class TranLocationMeta {

    private final TreeSystem ts_impl;
    private final TreeSystemTransaction ts_transaction_impl;
    private final TranDataFile data_file;
    private final long start_pos;
    private final long end_pos;

    private TranLocationMeta(TreeSystem ts_impl,
                             TreeSystemTransaction ts_transaction_impl,
                             TranDataFile data_file,
                             long start_pos, long end_pos) {
      this.ts_impl = ts_impl;
      this.ts_transaction_impl = ts_transaction_impl;
      this.data_file = data_file;
      this.start_pos = start_pos;
      this.end_pos = end_pos;
    }

    private long getStartPosition() {
      return start_pos;
    }

    private long getEndPosition() {
      return end_pos;
    }

    private TranDataFile getDataFile() {
      return data_file;
    }

    private TreeSystem getTreeSystem() {
      return ts_impl;
    }

    private TreeSystemTransaction getTransaction() {
      return ts_transaction_impl;
    }

  }

  // ---------- Statics ----------

  public static final Key USER_DATA_MIN =
               new Key(Short.MIN_VALUE, Integer.MIN_VALUE, Long.MIN_VALUE + 1);

  public static final Key USER_DATA_MAX =
               new Key((short) 0x07F7F, Integer.MAX_VALUE, Long.MAX_VALUE);


  // ---------- Debugging ----------

  // Fast size calculation by adding the count of all elements in the root node
  public long fastSizeCalculate() throws IOException {
    TreeNode node = fetchNode(getRootNodeRef());
    if (node instanceof TreeBranch) {
      TreeBranch branch = (TreeBranch) node;
      int sz = branch.size();
      // Add up the sizes of the children in the branch
      long r_size = 0;
      for (int i = 0; i < sz; ++i) {
        r_size += branch.getChildLeafElementCount(i);
      }
      return r_size;
    }
    else {
      TreeLeaf leaf = (TreeLeaf) node;
      return leaf.getSize();
    }
  }

  // Discovers the height of the tree
  private long treeHeight(NodeReference ref) throws IOException {
    TreeNode node = fetchNode(ref);
    if (node instanceof TreeBranch) {
      TreeBranch branch = (TreeBranch) node;
      return treeHeight(branch.getChild(0)) + 1;
    }
    else {
      return 1;
    }
  }

  // Recursively sums the size of all the leaf nodes,
  private long sizeCalculate(NodeReference ref) throws IOException {
    TreeNode node = fetchNode(ref);
    if (node instanceof TreeBranch) {
      TreeBranch branch = (TreeBranch) node;
      int sz = branch.size();
      // Add up the sizes of the children in the branch
      long r_size = 0;
      for (int i = 0; i < sz; ++i) {
        long c_size = sizeCalculate(branch.getChild(i));
        r_size += c_size;
      }
      return r_size;
    }
    else {
      TreeLeaf leaf = (TreeLeaf) node;
      return leaf.getSize();
    }
  }

  // Recursively sums the number of children in all branch nodes,
  private long branchChildCount(NodeReference ref) throws IOException {
    TreeNode node = fetchNode(ref);
    if (node instanceof TreeBranch) {
      TreeBranch branch = (TreeBranch) node;
      int sz = branch.size();
      // Add up the sizes of the children in the branch
      long r_size = 0;
      for (int i = 0; i < sz; ++i) {
        long c_size = branchChildCount(branch.getChild(i));
        r_size += c_size;
      }
      return r_size + sz;
    }
    else {
      return 0;
    }
  }

  // Recursively discovers the total number of branch nodes in the tree,
  private long branchCount(NodeReference ref) throws IOException {
    TreeNode node = fetchNode(ref);
    if (node instanceof TreeBranch) {
      TreeBranch branch = (TreeBranch) node;
      int sz = branch.size();
      // Add up the sizes of the children in the branch
      long r_size = 0;
      for (int i = 0; i < sz; ++i) {
        long c_size = branchCount(branch.getChild(i));
        r_size += c_size;
      }
      return r_size + 1;
    }
    else {
      return 0;
    }
  }

  // Recursively discovers the total number of leaf nodes in the tree,
  private long leafCount(NodeReference ref) throws IOException {
    TreeNode node = fetchNode(ref);
    if (node instanceof TreeBranch) {
      TreeBranch branch = (TreeBranch) node;
      int sz = branch.size();
      // Add up the sizes of the children in the branch
      long r_size = 0;
      for (int i = 0; i < sz; ++i) {
        long c_size = leafCount(branch.getChild(i));
        r_size += c_size;
      }
      return r_size;
    }
    else {
      return 1;
    }
  }

  // Returns the leaf reference to the far left of the given reference
  private NodeReference fetchFarLeftLeaf(NodeReference ref) throws IOException {
    TreeNode node = fetchNode(ref);
    if (node instanceof TreeBranch) {
      TreeBranch branch = (TreeBranch) node;
      return fetchFarLeftLeaf(branch.getChild(0));
    }
    else {
      return ref;
    }
  }

  // Checks the integrity of the tree by checking the every key reference
  // points to a leaf that is to the far left of the child before.  Also
  // checks the byte size elements.
  private long checkIntegrity(NodeReference ref) throws IOException {
    TreeNode node = fetchNode(ref);
    if (node instanceof TreeBranch) {
      TreeBranch branch = (TreeBranch) node;
      int sz = branch.size();
//      // Check the keys are valid
//      for (int i = 1; i < sz; ++i) {
//        Key key = branch.getKeyValue(i);
//        NodeReference far_left_ref = fetchFarLeftLeaf(branch.getChild(i));
//        if (!key.equals(((TreeLeaf) fetchNode(far_left_ref)).getKey())) {
//          throw new RuntimeException(
//                              "Integrity failed; tree reference is invalid.");
//        }
//      }
      // Add up the sizes of the children in the branch
      long r_size = 0;
      for (int i = 0; i < sz; ++i) {
        long c_size = checkIntegrity(branch.getChild(i));
        if (c_size != branch.getChildLeafElementCount(i)) {
          throw new RuntimeException("Size mis-match error.");
        }
        r_size += c_size;
      }
      return r_size;
    }
    else {
      TreeLeaf leaf = (TreeLeaf) node;
      return leaf.getSize();
    }
  }
  
  // Checks the integrity of the tree by performing a size check and a key
  // integrity test.
  public long checkIntegrity() throws IOException {
    return checkIntegrity(getRootNodeRef());
  }


  public void printStatistics() throws IOException {
    NodeReference root_ref = getRootNodeRef();
    long tree_height = treeHeight(root_ref);
    long leaf_count = leafCount(root_ref);
    long total_size = sizeCalculate(root_ref);
    long branch_child_count = branchChildCount(root_ref);
    long branch_count = branchCount(root_ref);

    System.out.println("Tree height = " + tree_height);
    System.out.println("Leaf count = " + leaf_count);
    System.out.println("Branch count = " + branch_count);
    System.out.println("Total byte count = " + total_size);
    System.out.println("Branch child count = " + branch_child_count);
    System.out.println("---------");
    System.out.println("Total node count = " + (leaf_count + branch_count));
    System.out.println("Average leaf size = " + ((float) total_size / leaf_count));
    System.out.println("Average branch size = " + ((float) branch_child_count / branch_count));
  }


  private void debugOutputString(StringBuffer buf, int indent, String str) {
    for (int i = 0; i < indent; ++i) {
      buf.append(' ');
    }
    buf.append(str);
  }
  
  private void debugOutputNode(final Key left_key,
                               NodeReference pointer, int indent,
                               StringBuffer buf) throws IOException {
    TreeNode node = fetchNode(pointer);
    if (node instanceof TreeBranch) {
      TreeBranch branch = (TreeBranch) node;
      // Output each child
      debugOutputString(buf, indent, "Branch Node count = " +
                        branch.getLeafElementCount() +
                        " [ children = " + branch.size() + " ]\r\n");
      for (int i = 0; i < branch.size(); ++i) {
//        buf.append(branch.getChildLeafElementCount(i) + " ");
        Key cl_key = (i > 0) ? branch.getKeyValue(i) : left_key;
        debugOutputNode(cl_key, branch.getChild(i), indent + 2, buf);
        if (i < branch.size() - 1) {
          debugOutputString(buf, indent + 2, "Key " + branch.getKeyValue(i + 1) + "\r\n");
        }
      }
    }
    else {
      TreeLeaf leaf = (TreeLeaf) node;
      StringBuffer b = new StringBuffer();
      NodeReference ref = leaf.getReference();
      String ref_str;
      if (ref.isSpecial()) {
//      if ((ref & 0x01000000000000000L) != 0) {
        ref_str = "special:" + ref;
      }
      else {
        ref_str = "" + ref;
      }
      b.append("Leaf Node ( " + left_key + " ) ( " + ref_str + " ) [ ");
      b.append("size = " + leaf.getSize() + " ");
      for (int i = 0; i < Math.min(leaf.getSize(), 20); ++i) {
        b.append("" + leaf.get(i));
        b.append(",");
      }
      b.append(" ]\r\n");
      debugOutputString(buf, indent, b.toString());
    }
  }

  public StringBuffer debugOutput(NodeReference pointer) throws IOException {
    StringBuffer buf = new StringBuffer();
    debugOutputNode(Key.HEAD_KEY, pointer, 0, buf);
    return buf;
  }

  public StringBuffer debugOutput() throws IOException {
    return debugOutput(getRootNodeRef());
  }

  public void printDebugOutput() throws IOException {
    printDebugOutput(getRootNodeRef());
  }
  
  public void printDebugOutput(NodeReference pointer) throws IOException {
    System.out.println(debugOutput(pointer).toString());
  }

}
