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

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.io.IOException;
import com.mckoi.store.AreaWriter;

/**
 * Temporary heap space for nodes in a versioning BTree.  This represents
 * address space that is allocated exclusively for the temporary organization
 * of nodes during tree manipulation operations.
 * <p>
 * CONCURRENCY: Note that this class is <b>NOT</b> thread safe.  It is intended
 *   to be used as a temporary cache of nodes for a transaction during a series
 *   of tree manipulation operations.  We may want to introduce some thread
 *   safety into this object so that an external thread can call these objects
 *   and flush nodes out of memory to optimize memory conditions.
 * 
 * @author Tobias Downer
 */

public class TreeNodeHeap {

  /**
   * True if we are to keep statics.
   */
  private static final boolean KEEP_STATS = true;
  
  /**
   * A unique pointer value that updates whenever a new value is created.
   */
  private long node_id_seq;

  /**
   * The heap that contains all the nodes referenced by pointer.
   */
  private final HashNode[] hash;

  /**
   * The end of the node access list.
   */
  private HashNode hash_end;
  
  /**
   * The start of the node access list.
   */
  private HashNode hash_start;

  /**
   * Statics - the total number of nodes being stored in the heap.
   */
  private int total_branch_node_count = 0;
  private int total_leaf_node_count = 0;

  // Total memory used by this node heap
  private long memory_used = 0;

//  /**
//   * True when a cache flush is being processed.
//   */
//  private boolean processing_flush;

  /**
   * The maximum limit of memory before a node heap flush is enacted.
   */
  private long max_memory_limit;

  /**
   * Constructs the btree.
   */
  public TreeNodeHeap(int hash_size, long max_memory_limit) {
    hash = new HashNode[hash_size];
    this.node_id_seq = 2;
    this.max_memory_limit = max_memory_limit;
  }

  /**
   * Returns the total number of branch nodes being stored in this heap.
   */
  public int getTotalBranchNodes() {
    return total_branch_node_count;
  }
  
  /**
   * Returns the total number of branch nodes being stored in this heap.
   */
  public int getTotalLeafNodes() {
    return total_leaf_node_count;
  }

  /**
   * Returns the amount of memory used by this node heap.  Note that this value
   * is an approximate value.  It actually returns the total number of bytes
   * needed to store all the nodes, but not including memory associated with
   * object maintenance.
   */
  public long getTotalMemoryUse() {
    return memory_used;
  }

  /**
   * Calculates an index in the hash array of the given pointer.
   */
  private int calcHashValue(NodeReference p) {
//    final int pp = ((int) -p & 0x0FFFFFFF);
//    return pp % hash.length;

    int hc = p.hashCode();
    if (hc < 0) {
      hc = -hc;
    }
    return hc % hash.length;
  }

  /**
   * Puts the given node into the hash.
   */
  private void putInHash(HashNode node) {
    NodeReference node_ref = node.getReference();
//    System.out.println("PUTTING IN CACHE: " + node_ref);
    int hash_index = calcHashValue(node_ref);
    HashNode old_node = hash[hash_index];
    hash[hash_index] = node;
    node.setNextHash(old_node);

    // Add it to the start of the linked list,
    if (hash_start != null) {
      hash_start.setPrevious(node);
    }
    else {
      hash_end = node;
    }
    node.setNext(hash_start);
    node.setPrevious(null);
    hash_start = node;

    // Update the 'memory_used' variable
    memory_used += node.getHeapSizeEstimate();
    if (KEEP_STATS) {
      if (node instanceof TreeBranch) {
        ++total_branch_node_count;
//        memory_used += ((((TreeBranch) node).maxSize() * 3) - 1) * 8;
      }
      else {
        ++total_leaf_node_count;
//        memory_used += ((TreeLeaf) node).getCapacity();
      }
    }
  }

  /**
   * Creates a new pointer for a node on the heap.
   */
  private NodeReference nextNodePointer() {
    long p = node_id_seq;
    ++node_id_seq;
    // ISSUE: What happens if the node id sequence overflows?
    //   The boundary is large enough that if we were to create a billion
    //   nodes a second continuously, it would take 18 years to overflow.
    node_id_seq = node_id_seq & 0x0FFFFFFFFFFFFFFFL;

    return NodeReference.createInMemoryNode(p);
  }


  public TreeNode fetchNode(NodeReference pointer) {
    // Fetches the node out of the heap hash array.
    int hash_index = calcHashValue(pointer);
    HashNode hash_node = hash[hash_index];
    while (hash_node != null &&
           !hash_node.getReference().equals(pointer)) {
      hash_node = hash_node.getNextHash();
    }

//    // Move this to the head of the list,
//    if (hash_start != hash_node) {
//
//      HashNode next_node = hash_node.getNext();
//      HashNode previous_node = hash_node.getPrevious();
//
//      hash_node.setNext(hash_start);
//      hash_node.setPrevious(null);
//      hash_start = hash_node;
//      hash_node.getNext().setPrevious(hash_node);
//
//      if (next_node != null) {
//        next_node.setPrevious(previous_node);
//      }
//      else {
//        hash_end = previous_node;
//      }
//      previous_node.setNext(next_node);
//
//    }
    
    return hash_node;
  }

  public TreeBranch createEmptyBranch(TreeSystemTransaction tran,
                                      int max_branch_children) {
    NodeReference p = nextNodePointer();
    HeapTreeBranch node = new HeapTreeBranch(tran, p, max_branch_children);
    putInHash(node);
    return node;
  }

  public TreeLeaf createEmptyLeaf(TreeSystemTransaction tran,
                                    Key key, int max_leaf_size) {
    NodeReference p = nextNodePointer();
    HeapTreeLeaf node = new HeapTreeLeaf(tran, p, max_leaf_size);
    putInHash(node);
    return node;
  }

  /**
   * Copies an existing TreeNode and creates a heap node.
   */
  public TreeNode copy(TreeNode node_to_copy,
          int max_branch_size, int max_leaf_size, TreeSystemTransaction tran)
                                                           throws IOException {
    // Create a new pointer for the copy
    NodeReference p = nextNodePointer();
    HashNode node;
    if (node_to_copy instanceof TreeLeaf) {
      node = new HeapTreeLeaf(tran, p, (TreeLeaf) node_to_copy, max_leaf_size);
    }
    else {
      node = new HeapTreeBranch(tran, p,
                                (TreeBranch) node_to_copy, max_branch_size);
    }
    putInHash(node);
    // Return pointer to node
    return (TreeNode) node;
  }

  public void delete(NodeReference pointer) {
    int hash_index = calcHashValue(pointer);
    HashNode hash_node = hash[hash_index];
    HashNode previous = null;
    while (hash_node != null &&
           !(hash_node.getReference().equals(pointer))) {
      previous = hash_node;
      hash_node = hash_node.getNextHash();
    }
    if (hash_node == null) {
      throw new RuntimeException("Node not found!");
    }
    if (previous == null) {
      hash[hash_index] = hash_node.getNextHash();
    }
    else {
      previous.setNextHash(hash_node.getNextHash());
    }

    // Remove from the double linked list structure,
    // If removed node at head.
    if (hash_start == hash_node) {
      hash_start = hash_node.getNext();
      if (hash_start != null) {
        hash_start.setPrevious(null);
      }
      else {
        hash_end = null;
      }
    }
    // If removed node at end.
    else if (hash_end == hash_node) {
      hash_end = hash_node.getPrevious();
      if (hash_end != null) {
        hash_end.setNext(null);
      }
      else {
        hash_start = null;
      }
    }
    else {
      hash_node.getPrevious().setNext(hash_node.getNext());
      hash_node.getNext().setPrevious(hash_node.getPrevious());
    }

    // Update the 'memory_used' variable
    memory_used -= hash_node.getHeapSizeEstimate();
    if (KEEP_STATS) {
      if (hash_node instanceof TreeBranch) {
        --total_branch_node_count;
//        memory_used -= ((((TreeBranch) hash_node).maxSize() * 3) - 1) * 8;
      }
      else {
        --total_leaf_node_count;
//        memory_used -= ((TreeLeaf) hash_node).getCapacity();
      }
    }
  }

  /**
   * Called very frequently to handle flushing of the node heap to disk when
   * the heap becomes full.
   */
  void manageNodeCache() throws IOException {
    ArrayList<HashNode> to_flush = null;
    int all_node_count = 0;
    // If the memory use is above some limit then we need to flush out some
    // of the nodes,
    if (memory_used > max_memory_limit) {
      all_node_count = total_branch_node_count + total_leaf_node_count;
      // The number to clean,
      int to_clean = (int) (all_node_count * 0.30);

      // Make an array of all nodes to flush,
      to_flush = new ArrayList(to_clean);
      // Pull them from the back of the list,
      HashNode node = hash_end;
      while (to_clean > 0 && node != null) {
        to_flush.add(node);
        node = node.getPrevious();
        --to_clean;
      }
    }

    // If there are nodes to flush,
    if (to_flush != null) {
      // Read each group and call the node flush routine,
//      System.out.println("We have " + to_flush.size() + " nodes to flush!");

      // The mapping of transaction to node list
      HashMap<TreeSystemTransaction, ArrayList<NodeReference>> tran_map =
                                                                new HashMap();
      // Read the list backwards,
      for (int i = to_flush.size() - 1; i >= 0; --i) {
        HashNode node = (HashNode) to_flush.get(i);
        // Get the transaction of this node,
        TreeSystemTransaction tran = node.getTransaction();
        // Find the list of this transaction,
        ArrayList<NodeReference> node_list = tran_map.get(tran);
        if (node_list == null) {
          node_list = new ArrayList(to_flush.size());
          tran_map.put(tran, node_list);
        }
        // Add to the list
        node_list.add(node.getReference());
      }
      // Now read the key and dispatch the clean up to the transaction objects,
      Iterator<TreeSystemTransaction> key = tran_map.keySet().iterator();
      while (key.hasNext()) {
        TreeSystemTransaction tran = key.next();
        ArrayList<NodeReference> node_list = tran_map.get(tran);
        // Convert to a 'NodeReference[]' array,
        int sz = node_list.size();
        NodeReference[] refs = new NodeReference[sz];
        for (int i = 0; i < sz; ++i) {
          refs[i] = node_list.get(i);
        }
        // Sort the references,
        Arrays.sort(refs);
//        System.out.println("Memory used = " + memory_used);
//        System.out.println("All node count = " + all_node_count);
//        System.out.println("Flushing: " + refs.length);
        // Tell the transaction to clean up these nodes,
        tran.flushNodesToStore(refs);
      }

    }
  }
  
  // ----- Inner classes -----
  
  private static interface HashNode extends TreeNode {
    HashNode getNextHash();
    void setNextHash(HashNode node);

    HashNode getPrevious();
    void setPrevious(HashNode node);
    HashNode getNext();
    void setNext(HashNode node);
    
    TreeSystemTransaction getTransaction();
  }

  private static class HeapTreeBranch extends TreeBranch implements HashNode {

    private HashNode next_hash;
    private HashNode next_list;
    private HashNode previous_list;

    private final TreeSystemTransaction tran;
    
    HeapTreeBranch(TreeSystemTransaction tran,
                   NodeReference node_ref, int max_children) {
      super(node_ref, max_children);
      this.tran = tran;
    }

    HeapTreeBranch(TreeSystemTransaction tran,
                   NodeReference node_ref,
                   TreeBranch branch, int max_children) {
      super(node_ref, branch, max_children);
      this.tran = tran;
    }
    
    public HashNode getNextHash() {
      return next_hash;
    }

    public void setNextHash(HashNode node) {
      next_hash = node;
    }

    public TreeSystemTransaction getTransaction() {
      return tran;
    }

    public HashNode getPrevious() {
      return previous_list;
    }
    
    public void setPrevious(HashNode node) {
      previous_list = node;
    }
    
    public HashNode getNext() {
      return next_list;
    }
    
    public void setNext(HashNode node) {
      next_list = node;
    }
    
    public int getHeapSizeEstimate() {
      return super.getHeapSizeEstimate() + (8 * 4);
    }

  }

  private static class HeapTreeLeaf extends TreeLeaf implements HashNode {

    private HashNode next_hash;
    private HashNode next_list;
    private HashNode previous_list;

    private final TreeSystemTransaction tran;

    private byte[] data;

    private NodeReference node_ref;
    private int size;


    HeapTreeLeaf(TreeSystemTransaction tran,
                 NodeReference node_ref, int max_capacity) {
      super();
      this.node_ref = node_ref;
      this.size = 0;
      this.tran = tran;
      this.data = new byte[max_capacity];
    }

    HeapTreeLeaf(TreeSystemTransaction tran,
                 NodeReference node_ref, TreeLeaf to_copy, int max_capacity)
                                                           throws IOException {
      super();
      this.node_ref = node_ref;
      this.size = to_copy.getSize();
      this.tran = tran;
      // Copy the data into an array in this leaf.
      this.data = new byte[max_capacity];
      if (getSize() > max_capacity) {
        throw new RuntimeException("getSize() > max_capacity (" + getSize() + " > " + max_capacity + ")");
      }
      to_copy.get(0, data, 0, getSize());
    }

    // ---------- Implemented from TreeLeaf ----------

    public NodeReference getReference() {
      return node_ref;
    }

    public int getSize() {
      return size;
    }

    public int getCapacity() {
      return data.length;
    }

    public void get(int position, byte[] buf, int off, int len) {
      System.arraycopy(data, position, buf, off, len);
    }

    public byte get(int position) {
      return data[position];
    }

    public void writeDataTo(AreaWriter writer) throws IOException {
      writer.put(data, 0, getSize());
    }

    public void shift(int position, int off) {
      if (off != 0) {
        final int new_size = getSize() + off;
        System.arraycopy(data, position,
                         data, position + off, getSize() - position);
        // Set the size
        size = new_size;
      }
    }

    public void put(int position, byte[] buf, int off, int len) {
      System.arraycopy(buf, off, data, position, len);
      if (position + len > size) {
        size = position + len;
      }
    }

    public void setSize(int new_size) {
      if (new_size >= 0 && new_size <= getCapacity()) {
        size = new_size;
      }
      else {
        throw new RuntimeException("Leaf size error: " + new_size);
      }
    }

    public int getHeapSizeEstimate() {
      // The size of the member variables + byte estimate for heap use for
      // Java object maintenance.
      return 8 + 4 + data.length + 64 + (8 * 4);
    }

    // ---------- Implemented from HashNode ----------

    public HashNode getNextHash() {
      return next_hash;
    }

    public void setNextHash(HashNode node) {
      next_hash = node;
    }

    public TreeSystemTransaction getTransaction() {
      return tran;
    }

    public HashNode getPrevious() {
      return previous_list;
    }
    
    public void setPrevious(HashNode node) {
      previous_list = node;
    }
    
    public HashNode getNext() {
      return next_list;
    }
    
    public void setNext(HashNode node) {
      next_list = node;
    }

  }

}


