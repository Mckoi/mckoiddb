/**
 * com.mckoi.treestore.TreeBranch  09 Oct 2004
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

import java.io.IOException;

/**
 * A branch of the tree in a TreeStore.  A branch contains pointers to other
 * nodes in the tree, a key extent (the number of bytes in the subtree), and 
 * a key value the deliminates each pointer.  For example,
 * <code>
 * +---------+--------+  +----------------------+  +---------+--------+
 * | POINTER | EXTENT |  | KEY_VALUE (16 bytes) |  | POINTER | EXTENT |  ....
 * +---------+--------+  +----------------------+  +---------+--------+
 * </code>
 * The tree is designed such that searching the tree for a particular key is
 * a simple walk down the tree.  Locating the position of a byte in the
 * combined ordered set of all data is also a simple tree walk.
 * <p>
 * Note that POINTER is 16 bytes, EXTENT is 8 bytes, KEY_VALUE is 16 bytes.
 * In total, each pointer/extent/key is 5 long values in size.
 *
 * @author Tobias Downer
 */

public class TreeBranch implements TreeNode {

  /**
   * A pointer in the map to the storage container for this node.  If it is
   * a number &lt; 0 then the reference is to a mutable node in the heap.  If
   * the value is &gt;= 0, the node is immutable and stored on disk.
   */
  private NodeReference node_ref;

  /**
   * This array contains the layout of [pointer, extent] [key_value] as
   * described in the class description.
   */
  private long[] children;

  /**
   * The number of pointers in this branch.
   */
  private int children_count;

  /**
   * Constructs an empty TreeBranch for a heap node.
   */
  public TreeBranch(NodeReference node_ref, int max_children_count) {
    if (!node_ref.isInMemory()) {
      throw new RuntimeException("Only heap node permitted.");
    }
    if ((max_children_count % 2) != 0) {
      throw new RuntimeException("max_children_count must be a multiple of 2.");
    }
    if (max_children_count > 65530) {
      throw new RuntimeException("Branch children count is limited to 65530");
    }
    if (max_children_count < 6) {
      // While I did test with 4, tree balancing is rather tough at 4 so we
      // should have this at at least 6.
      throw new RuntimeException("max_children_count must be >= 6");
    }
    this.node_ref = node_ref;

    children = new long[(max_children_count * 5) - 2];
    children_count = 0;
  }

  /**
   * Copy constructor.  Typically used to make an immutible node on disk
   * mutable in memory.
   */
  public TreeBranch(NodeReference node_ref, TreeBranch branch,
                    int max_children_count) {
    this(node_ref, max_children_count);
    System.arraycopy(branch.children, 0, children, 0,
                     Math.min(branch.children.length, children.length));
    children_count = branch.children_count;
    SELFCHECK();
  }

  /**
   * When constructing a branch from an image on disk.
   */
  public TreeBranch(NodeReference node_ref,
                    long[] node_data, int node_data_size) {
    if (node_ref.isInMemory()) {
      throw new RuntimeException("Only store nodes permitted.");
    }
    this.node_ref = node_ref;
    this.children = node_data;
    this.children_count = (node_data_size + 2) / 5;
    SELFCHECK();
  }

  /**
   * Internal method that finds the size of the child at the given position in
   * the array.
   */
  private long internalGetChildSize(int p) {
    long size = children[p + 2];
    return size;
  }

  /**
   * Returns true if this leaf is mutable (is currently located in memory).
   */
  public boolean isMutable() {
    return node_ref.isInMemory();
  }

  /**
   * Generates an exception if this node is not mutable (is not on the mutable
   * heap).
   */
  public void checkMutable() {
    if (!isMutable()) {
      throw new RuntimeException("Node is immutible.");
    }
  }

  /**
   * Returns the address of this node.  If the address is less than 0 then the
   * node is located on the mutable node heap and this object is mutable.  If
   * the address is greater or equal to 0 then the node is immutable and in the
   * store.
   */
  public NodeReference getReference() {
    return node_ref;
  }
  
  /**
   * Returns the node data of this branch node.  Note that the returned array
   * must NOT be manipulated in any way.  This method is provided for
   * serialization of nodes.
   */
  public final long[] getNodeData() {
    return children;
  }

  /**
   * Returns the number of elements in the long[] array returned by
   * 'getNodeData()' needed to encapsulate all the information within this
   * node.
   */
  public final int getNodeDataSize() {
    return (children_count * 5) - 2;
  }

  /**
   * Returns the total number of elements in the leaves of this branch.
   */
  public final long getLeafElementCount() {
//    // Add up all the elements of the children
//    long leaf_element_count = 0;
//    final int end = (children_count * 5) - 2;
////    System.out.println("children_count = " + children_count);
////    System.out.println("end = " + end);
////    System.out.println("children.length = " + children.length);
//    for (int i = 2; i < end; i += 5) {
//      leaf_element_count += children[i];
//    }
//    return leaf_element_count;

    int elements = children_count;
    long size = 0;
    int p = 0;
    for (;elements > 0; --elements) {
      size += children[p + 2];
      p += 5;
    }
    return size;
  }

  /**
   * Sets the value of the total number of elements in the leaves of the given
   * child of this branch.
   */
  final void setChildLeafElementCount(long count, int child_i) {
    checkMutable();
    if (child_i >= size()) {
      throw new RuntimeException("Child request out of bounds.");
    }
    children[(child_i * 5) + 2] = count;
  }

  /**
   * Sets the key value to the left of the given child element in this branch.
   */
  final void setKeyValueToLeft(Key key, int child_i) {
    checkMutable();
    if (child_i >= size()) {
      throw new RuntimeException("Key request out of bounds.");
    }
    children[(child_i * 5) - 2] = key.encodedValue(1);
    children[(child_i * 5) - 1] = key.encodedValue(2);
  }

//  final void setKeyValueToLeft(Key k, int child_i) {
//    setKeyValueToLeft(k.encodedValue(1), k.encodedValue(2), child_i);
//  }

  /**
   * Returns the current children on this branch.
   */
  public final int size() {
    return children_count;
  }

  /**
   * Returns the maximum number of children on this branch.
   */
  public final int maxSize() {
    return (children.length + 2) / 5;
  }

  /**
   * Return the nth child leaf pointer.
   */
  public final NodeReference getChild(int n) {
    if (n >= size()) {
      throw new RuntimeException("Child request out of bounds.");
    }
    final int p = (n * 5);
    return new NodeReference(children[p], children[p + 1]);
  }

  /**
   * Returns the key value before the given child where n > 0.
   */
  public final Key getKeyValue(int n) {
    if (n >= size()) {
      throw new RuntimeException("Key request out of bounds.");
    }
    long key_v1 = children[(n * 5) - 2];
    long key_v2 = children[(n * 5) - 1];
    return new Key(key_v1, key_v2);
  }

  /**
   * Returns true if this leaf is completely full.
   */
  public final boolean isFull() {
    return size() == maxSize();
  }

  /**
   * Returns true if this leaf is less than half full.
   */
  public final boolean isLessThanHalfFull() {
    return (size() < (maxSize() / 2));
  }

  /**
   * Returns true if this node is empty.
   */
  public final boolean isEmpty() {
    return (size() == 0);
  }

  /**
   * Returns the midpoint key value.
   */
  public final Key midpointKey() {
    int n = children.length / 2;
    return new Key(children[n - 1], children[n]);
  }

  /**
   * Check we didn't make a self referencing branch.
   */
  private final void SELFCHECK() {
//    if (TreeSystem.PRAGMATIC_CHECKS == true) {
//      int sz = size();
//      for (int i = 0; i < sz; ++i) {
//        if (getChild(i).equals(getReference())) {
//          throw new Error("Self referencing branch!");
//        }
//      }
//    }
  }

  /**
   * Moves the last half of this branch node into the destination node.  Note
   * that the midpoint extent is lost when we move the data, so it should be
   * recorded and handled accordingly before this method is called.
   */
  public final void moveLastHalfInto(TreeBranch dest) {
    final int midpoint = children.length / 2;

    // Check mutable
    checkMutable();
    dest.checkMutable();

    // Check this is full
    if (!isFull()) {
      throw new RuntimeException("Branch node is not full.");
    }
    // Check destination is empty
    if (dest.size() != 0) {
      throw new RuntimeException("Destination branch node is not empty.");
    }

    // Copy,
    System.arraycopy(children, midpoint + 1, dest.children, 0, midpoint - 1);

    // New child count in each branch node.
    int new_child_count = maxSize() / 2;
    // Set the size of this and the destination node
    children_count = new_child_count;
    dest.children_count = new_child_count;

    SELFCHECK();
  }

  /**
   * Shifts 'count' elements from the branch 'right' into this node where
   * 'mid_value' is the current middle value between the nodes.  This is used
   * to balance the tree.
   */
  public final Key mergeLeft(final TreeBranch right, final Key mid_value,
                             final int count) {
    // Check mutable
    checkMutable();

    // If we moving all from right,
    if (count == right.size()) {
      // Move all the elements into this node,
      int dest_p = children_count * 5;
      int right_len = (right.children_count * 5) - 2;
      System.arraycopy(right.children, 0, children, dest_p, right_len);
      children[dest_p - 2] = mid_value.encodedValue(1);
      children[dest_p - 1] = mid_value.encodedValue(2);
      // Update children_count
      children_count += right.children_count;

      SELFCHECK();
      return null;
    }
    else if (count < right.size()) {
      right.checkMutable();
      long new_midpoint_value1, new_midpoint_value2;

      // Shift elements from right to left
      // The amount to move that will leave the right node at min threshold
      int dest_p = size() * 5;
      int right_len = (count * 5) - 2;
      System.arraycopy(right.children, 0, children, dest_p, right_len);
      // Redistribute the right elements
      int right_redist = (count * 5);
      // The midpoint value becomes the extent shifted off the end
      new_midpoint_value1 = right.children[right_redist - 2];
      new_midpoint_value2 = right.children[right_redist - 1];
      // Shift the right child
      System.arraycopy(right.children, right_redist, right.children, 0,
                       right.children.length - right_redist);
      children[dest_p - 2] = mid_value.encodedValue(1);
      children[dest_p - 1] = mid_value.encodedValue(2);
      children_count += count;
      right.children_count -= count;

      // Return the new midpoint value
      SELFCHECK();
      return new Key(new_midpoint_value1, new_midpoint_value2);
    }
    else {
      throw new RuntimeException("count > right.size()");
    }
  }

  /**
   * Redistributes the contents of both nodes amongst each other.  This node
   * or the given node must be less than half full.  When this method returns,
   * this node will always contain entries, and the given node may not contain
   * any entries.
   */
  public final Key merge(TreeBranch right, Key mid_value) {
    // Check mutable
    checkMutable();
    right.checkMutable();

    // How many elements in total?
    final int total_elements = size() + right.size();
    // If total elements is smaller than max size,
    if (total_elements <= maxSize()) {
      // Move all the elements into this node,
      int dest_p = children_count * 5;
      int right_len = (right.children_count * 5) - 2;
      System.arraycopy(right.children, 0, children, dest_p, right_len);
      children[dest_p - 2] = mid_value.encodedValue(1);
      children[dest_p - 1] = mid_value.encodedValue(2);
      // Update children_count
      children_count += right.children_count;
      right.children_count = 0;

      SELFCHECK();
      return null;
    }
    else {
      long new_midpoint_value1, new_midpoint_value2;

      // Otherwise distribute between the nodes,
      final int max_shift = (maxSize() + right.maxSize()) - total_elements;
      if (max_shift <= 2) {
        return mid_value;
      }
      int min_threshold = maxSize() / 2;
//      final int half_total_elements = total_elements / 2;
      if (size() < right.size()) {
        // Shift elements from right to left
        // The amount to move that will leave the right node at min threshold
        int count = Math.min(right.size() - min_threshold, max_shift);
        int dest_p = size() * 5;
        int right_len = (count * 5) - 2;
//        System.out.println("right.children = " + right.children.length);
//        System.out.println("children = " + children.length);
//        System.out.println("dest_p = " + dest_p);
//        System.out.println("right_len = " + right_len);
        System.arraycopy(right.children, 0, children, dest_p, right_len);
        // Redistribute the right elements
        int right_redist = (count * 5);
        // The midpoint value becomes the extent shifted off the end
        new_midpoint_value1 = right.children[right_redist - 2];
        new_midpoint_value2 = right.children[right_redist - 1];
        // Shift the right child
        System.arraycopy(right.children, right_redist, right.children, 0,
                         right.children.length - right_redist);
        children[dest_p - 2] = mid_value.encodedValue(1);
        children[dest_p - 1] = mid_value.encodedValue(2);
        children_count += count;
        right.children_count -= count;

      }
      else {
        // Shift elements from left to right
        // The amount to move that will leave the left node at min threshold
        int count = Math.min(size() - min_threshold, max_shift);
//        int count = Math.min(half_total_elements - right.size(), max_shift);

        // Make room for these elements
        int right_redist = (count * 5);
        System.arraycopy(right.children, 0, right.children, right_redist,
                         right.children.length - right_redist);
        int src_p = (size() - count) * 5;
        int left_len = (count * 5) - 2;
        System.arraycopy(children, src_p, right.children, 0, left_len);
        right.children[right_redist - 2] = mid_value.encodedValue(1);
        right.children[right_redist - 1] = mid_value.encodedValue(2);
        // The midpoint value becomes the extent shifted off the end
        new_midpoint_value1 = children[src_p - 2];
        new_midpoint_value2 = children[src_p - 1];
        // Update children counts
        children_count -= count;
        right.children_count += count;
      }

      SELFCHECK();
      return new Key(new_midpoint_value1, new_midpoint_value2);
    }

  }

//  /**
//   * Returns the nearest sibling child of the given child index.  Used when
//   * redistributing child nodes of this branch.
//   */
//  public final int nearestSibling(int child_i) {
//    if (child_i == 0) {
//      return 1;
//    }
//    else {
//      return child_i - 1;
//    }
//  }

  /**
   * Update the nth child pointer in this branch. This method overrides any
   * mutable checks, so should only be used carefully when setting up a
   * branch object.
   */
  public final void setChildOverride(NodeReference child_pointer, int n) {
    children[(n * 5) + 0] = child_pointer.getHighLong();
    children[(n * 5) + 1] = child_pointer.getLowLong();
  }

  /**
   * Update the nth child pointer in this branch.
   */
  public final void setChild(NodeReference child_pointer, int n) {
    checkMutable();
    setChildOverride(child_pointer, n);
    SELFCHECK();
  }

  /**
   * Assuming a blank branch, this will set up an initial 2 child branch.
   */
  public final void set(NodeReference child1,
                        long child1_count,
                        Key key,
                        NodeReference child2,
                        long child2_count) {
    checkMutable();
    // Set the values
    children[0] = child1.getHighLong();
    children[1] = child1.getLowLong();
    children[2] = child1_count;
    children[3] = key.encodedValue(1);
    children[4] = key.encodedValue(2);
    children[5] = child2.getHighLong();
    children[6] = child2.getLowLong();
    children[7] = child2_count;
    // Increase the child count.
    children_count += 2;
    SELFCHECK();
  }
  
  /**
   * Inserts two children pointers and an extent into this branch at the given
   * <b>child</b> position.
   */
  public final void insert(NodeReference child1,
                           long child1_count,
                           Key key,
                           NodeReference child2,
                           long child2_count,
                           int n) {
    checkMutable();
    // Shift the array by 5
    int p1 = (n * 5) + 3;
    int p2 = (n * 5) + 8;
//    System.out.println("p1 = " + p1);
//    System.out.println("p2 = " + p2);
//    System.out.println("children.length = " + children.length);
    System.arraycopy(children, p1, children, p2, children.length - p2);
    // Insert the values
    children[p1 - 3] = child1.getHighLong();
    children[p1 - 2] = child1.getLowLong();
    children[p1 - 1] = child1_count;
    children[p1 + 0] = key.encodedValue(1);
    children[p1 + 1] = key.encodedValue(2);
    children[p1 + 2] = child2.getHighLong();
    children[p1 + 3] = child2.getLowLong();
    children[p1 + 4] = child2_count;
    // Increase the child count.
    ++children_count;
    SELFCHECK();
  }

  /**
   * Removes the given child index from this branch and the left key reference.
   * If child_i = 0 then the far left child is deleted and its right key
   * reference.
   */
  final void removeChild(int child_i) {
    checkMutable();
    if (child_i == 0) {
      System.arraycopy(children, 5, children, 0, children.length - 5);
    }
    else if (child_i + 1 < children_count) {
      int p1 = (child_i * 5) + 3;
      System.arraycopy(children, p1, children, p1 - 5, children.length - p1);
    }
    --children_count;
    SELFCHECK();
  }

  /**
   * Returns the number of bytes represented by the child number.  This value
   * is either the sum of all bytes in the sub-tree or the sum of the bytes
   * in the leaf being pointed to.
   */
  public final long getChildLeafElementCount(final int child_i) {
    return internalGetChildSize(child_i * 5); // children[(child_i * 5) + 2];
  }

  /**
   * Returns the offset of the given child in this branch, where 0 is the
   * offset of the first child, [child 1 leaf count] is the offset of the
   * second child, etc.  This is used to determine the offset of some value
   * within a branch.
   */
  public final long childOffset(int child_i) {
    long offset = 0;
    int p = 0;
    for (;child_i > 0; --child_i) {
      offset += internalGetChildSize(p); //children[p + 2];
      p += 5;
    }
    return offset;
  }

  /**
   * Given an offset and key, returns the index of the child that will contain
   * the position.  For example, if this branch has two children nodes of sizes
   * 50 and 60, this will return 0 for all offsets between 0 - 49 and 1 for
   * all offsets between 50 - 109 and -1 if the offset is outside these bounds
   * (offset is less than 0 or greater than the leaf element count).
   */
  public final int childAtOffset(final Key key, final long offset)
                                                           throws IOException {

    if (offset >= 0) {
      int sz = size();
      long left_offset = 0;
      for (int i = 0; i < sz; ++i) {
        left_offset += getChildLeafElementCount(i);
        // If the relative point must be within this child
        if (offset < left_offset) {
          return i;
        }
        // This is a boundary condition, we need to use the key to work out
        // which child to take
        else if (offset == left_offset) {
          // If the end has been reached,
          if (i == sz - 1) {
            return i;
          }
          else {
            Key key_val = getKeyValue(i + 1);
            int n = key_val.compareTo(key);
            // If the key being inserted is less than the new leaf node,
            if (n > 0) {
              // Go left,
              return i;
            }
            else {
              // Otherwise go right
              return i + 1;
            }
          }
        }
      }
    }

    return -1;
  }

  /**
   * Searches for the first child in this branch that contains the value, and
   * returns an index in this branch of the child.  The returned int is either
   * a positive value indicating the child to search for the first, or a
   * negative value that indicates that child -(return_val + 1) and
   * (-(return_val + 1) + 1) must be searched.  This happens when an extent is
   * equal to the value being searched and so the left then right routes must
   * be searched.
   * <p>
   * A further test of the child node is required to determine if the value is
   * found there or not.
   */
  public final int searchFirst(Key key) throws IOException {
    int low = 1;
    int high = size() - 1;

    while (true) {

      if (high - low <= 2) {
        for (int i = low; i <= high; ++i) {
          int cmp = getKeyValue(i).compareTo(key);
          if (cmp > 0) {
            // Value is less than extent so take the left route
            return i - 1;
          }
          else if (cmp == 0) {
            // Equal so need to search left and right of the extent
            // This is (-(i + 1 - 1))
            return -i;
          }
        }
        // Value is greater than extent so take the right route
        return high;
      }

      int mid = (low + high) / 2;
      int cmp = getKeyValue(mid).compareTo(key);

      if (cmp < 0) {
        low = mid + 1;
      }
      else if (cmp > 0) {
        high = mid - 1;
      }
      else {
        high = mid;
      }
    }
    
  }

  /**
   * Searches for the last child in this branch that contains the value, and
   * returns an index in this branch of the child.
   * <p>
   * A further test of the child node is required to determine if the value is
   * found there or not.
   */
  public final int searchLast(Key key) throws IOException {

    int low = 1;
    int high = size() - 1;

    while (true) {

      if (high - low <= 2) {
        for (int i = high; i >= low; --i) {
          int cmp = getKeyValue(i).compareTo(key);
          if (cmp <= 0) {
            return i;
          }
        }
        return low - 1;
      }

      int mid = (low + high) / 2;
      int cmp = getKeyValue(mid).compareTo(key);

      if (cmp < 0) {
        low = mid + 1;
      }
      else if (cmp > 0) {
        high = mid - 1;
      }
      else {
        low = mid;
      }
    }

  }

  /**
   * Returns a heap size estimate for this tree branch.
   */
  public int getHeapSizeEstimate() {
    // The size of the member variables + byte estimate for heap use for
    // Java object maintenance.
    return 8 + 4 + (children.length * 8) + 64;
  }

}
