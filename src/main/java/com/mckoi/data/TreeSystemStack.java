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

import com.mckoi.store.AreaWriter;
import java.io.IOException;

/**
 * Represents the path between the root node and a leaf node as a stack and
 * provides various functions for modifying the leaf, adding new nodes to
 * the tree, and querying the data.  All operations will be valid over
 * subsequent operations provided the tree is not modified by another
 * process.
 *
 * @author Tobias Downer
 */

class TreeSystemStack {

  /**
   * The TreeSystemTransaction.
   */
  private final TreeSystemTransaction ts;

  /**
   * The size of the stack in element.
   */
  private int stack_size;
  /**
   * The stack contents.  The stack has three entries for each node on the
   * path.  The child index, the left offset of the node and the node
   * reference.
   */
  private long[] stack;
  /**
   * The size of individual stack frames in number of longs.
   */
  private final static int STACK_FRAME_SIZE = 4;
  /**
   * The current leaf node at the end of the stack or null if the stack is
   * empty.
   */
  private TreeLeaf current_leaf;
  /**
   * The Key of the current leaf.
   */
  private Key current_leaf_key;
  /**
   * The current offset into the leaf that a position will be.
   */
  private int leaf_offset;

  /**
   * Constructs the object.
   */
  TreeSystemStack(TreeSystemTransaction ts) {
    this.ts = ts;
    this.stack_size = 0;
    this.stack = new long[STACK_FRAME_SIZE * 13];
    this.current_leaf = null;
    this.current_leaf_key = null;
    this.leaf_offset = 0;
  }

  // Pass through methods to tree system transaction,

  private TreeNode fetchNode(NodeReference node_ref) throws IOException {
    return ts.fetchNode(node_ref);
  }

  private TreeNode unfreezeNode(TreeNode node) throws IOException {
    return ts.unfreezeNode(node);
  }

  private boolean isFrozen(NodeReference node_ref) {
    return ts.isFrozen(node_ref);
  }

  private boolean isHeapNode(NodeReference node_ref) {
    return ts.isHeapNode(node_ref);
  }

  private NodeReference writeNode(NodeReference node_ref) throws IOException {
    return ts.writeNode(node_ref);
  }

  private void deleteNode(NodeReference node_ref) throws IOException {
    ts.deleteNode(node_ref);
  }

  private void removeAbsoluteBounds(long pos_start, long pos_end)
                                                         throws IOException {
    ts.removeAbsoluteBounds(pos_start, pos_end);
  }

  private TreeBranch createEmptyBranch() {
    return ts.createEmptyBranch();
  }

  private TreeLeaf createEmptyLeaf(Key key) {
    return ts.createEmptyLeaf(key);
  }

  private TreeLeaf createSparseLeaf(Key key, byte bt, long size)
                                                        throws IOException {
    return ts.createSparseLeaf(key, bt, size);
  }

  private TreeSystem getTreeSystem() {
    return ts.getTreeSystem();
  }

  private void setTreeHeight(int height) {
    ts.setTreeHeight(height);
  }
  private int getTreeHeight() {
    return ts.getTreeHeight();
  }

  private void setRootNodeRef(NodeReference node_ref) {
    ts.setRootNodeRef(node_ref);
  }
  private NodeReference getRootNodeRef() {
    return ts.getRootNodeRef();
  }






  /**
   * Pushes an offset/node pointer onto the stack.
   */
  private void stackPush(int child_i, long offset,
                         NodeReference node_pointer) {

    if (stack_size + STACK_FRAME_SIZE >= stack.length) {
      // Expand the size of the stack.
      // The default size should be plenty for most iterators unless we
      // happen to be iterating across a particularly deep B+Tree.
      long[] new_stack = new long[stack.length * 2];
      System.arraycopy(stack, 0, new_stack, 0, stack.length);
      stack = new_stack;
    }
    stack[stack_size] = child_i;
    stack[stack_size + 1] = offset;
    stack[stack_size + 2] = node_pointer.getHighLong();
    stack[stack_size + 3] = node_pointer.getLowLong();
    stack_size += STACK_FRAME_SIZE;
  }

  /**
   * Returns a stack frame object that describes the item on the stack at
   * the given offset from the end.
   */
  private StackFrame stackEnd(int off) {
    return new StackFrame(stack, stack_size - ((off + 1) * STACK_FRAME_SIZE));
  }

  /**
   * Pops the last stack frame from the stack. Note that the returned
   * StackFrame object is invalidated if the stack is mutated in any
   * way (eg. a value is pushed onto the stack).
   */
  private StackFrame stackPop() {
    if (stack_size == 0) {
      throw new RuntimeException("Iterator stack underflow.");
    }
    stack_size -= STACK_FRAME_SIZE;
    return new StackFrame(stack, stack_size);
  }

  /**
   * Returns the total number of frames currently on the stack.
   */
  private int getFrameCount() {
    return stack_size / STACK_FRAME_SIZE;
  }

  /**
   * Returns true if the stack is empty.
   */
  private boolean stackEmpty() {
    return (stack_size == 0);
  }

  /**
   * Clears the stack.
   */
  private void stackClear() {
    stack_size = 0;
  }

  /**
   * Unfreezes all elements currently in the stack.
   */
  private void unfreezeStack() throws IOException {
    StackFrame frame = stackEnd(0);

    NodeReference old_child_node_ref = frame.getNodeReference();
    // If the leaf ref isn't frozen then we exit early
    if (!isFrozen(old_child_node_ref)) {
      return;
    }
    TreeLeaf leaf = (TreeLeaf) unfreezeNode(fetchNode(old_child_node_ref));
    NodeReference new_child_node_ref = leaf.getReference();
    frame.setNodeReference(new_child_node_ref);
    current_leaf = leaf;
    // NOTE: Setting current_leaf here does not change the key of the node
    //   so we don't need to update current_leaf_key.

    // Walk the rest of the stack from the end
    final int sz = getFrameCount();
    for (int i = 1; i < sz; ++i) {
      int changed_child_i = frame.getChildI();
      frame = stackEnd(i);
      NodeReference old_branch_ref = frame.getNodeReference();
      TreeBranch branch =
                      (TreeBranch) unfreezeNode(fetchNode(old_branch_ref));
      // Get the child_i from the stack,
      branch.setChild(new_child_node_ref, changed_child_i);

      // Change the stack entry
      frame.setNodeReference(branch.getReference());

      new_child_node_ref = branch.getReference();
    }

    // Set the new root node ref
    setRootNodeRef(new_child_node_ref);
//      root_node_ref = new_child_node_ref;
  }

  /**
   * Writes the leaf node, and if the node reference updated, traverses the
   * stack updating the branches that need updating.
   */
  public void writeLeafOnly(Key key) throws IOException {
    // Get the stack frame for the last entry.
    StackFrame frame = stackEnd(0);
    // The leaf
    final NodeReference leaf_ref = frame.getNodeReference();
    // Write it out
    NodeReference new_ref = writeNode(leaf_ref);
    // If new_ref = leaf_ref, then we didn't write a new node
    if (new_ref.equals(leaf_ref)) {
      return;
    }
    else {
      // Otherwise, update the references,
      frame.setNodeReference(new_ref);
      current_leaf = (TreeLeaf) fetchNode(new_ref);
      // Walk back up the stack and update the ref as necessary
      final int sz = getFrameCount();
      for (int i = 1; i < sz; ++i) {
        // Get the child_i from the stack,
        int changed_child_i = frame.getChildI();

        frame = stackEnd(i);

        NodeReference old_branch_ref = frame.getNodeReference();
        TreeBranch branch =
                     (TreeBranch) unfreezeNode(fetchNode(old_branch_ref));
        branch.setChild(new_ref, changed_child_i);

        // Change the stack entry
        new_ref = branch.getReference();
        frame.setNodeReference(new_ref);
      }

      // Set the new root node ref
      setRootNodeRef(new_ref);
//        root_node_ref = new_ref;
    }
  }

  /**
   * Called when the size properties of the current leaf has changed and
   * a size update for stack elements is necessary.
   * <p>
   * Assumes the stack is not frozen.
   */
  private void updateStackProperties(int size_diff) throws IOException {
    StackFrame frame = stackEnd(0);
    final int sz = getFrameCount();
    // Walk the stack from the end
    for (int i = 1; i < sz; ++i) {
      int child_i = frame.getChildI();
      frame = stackEnd(i);

      NodeReference node_ref = frame.getNodeReference();
      TreeBranch branch = (TreeBranch) fetchNode(node_ref);
      branch.setChildLeafElementCount(
              branch.getChildLeafElementCount(child_i) + size_diff, child_i);
    }
//      // Walk the stack from the end
//      for (int i = stack_size - 4; i >= 1; i -= 3) {
//        TreeBranch branch = (TreeBranch) fetchNode(stack[i]);
//        //int child_i = branch.childWithReference(node_ref);
//        int child_i = (int) stack[i + 1];
//        branch.setChildLeafElementCount(
//                branch.getChildLeafElementCount(child_i) + size_diff, child_i);
//      }
  }

  /**
   * Inserts a leaf node either immediately before or after the current leaf.
   */
  public void insertLeaf(final Key new_leaf_key,
                final TreeLeaf new_leaf, boolean before) throws IOException {
    int leaf_size = new_leaf.getSize();
    if (leaf_size <= 0) {
      throw new RuntimeException("size <= 0");
    }

    // The current absolute position and key
//      final long cur_absolute_pos = stack[stack_size - 2] + leaf_offset;
    final Key new_key = new_leaf_key;

//      TreeLeaf left, right;
//      long key_ref;
//
//      long current_leaf_ref = stack[stack_size - 1];

    // The frame at the end of the stack,
    StackFrame frame = stackEnd(0);


    Object[] nfo;
    Object[] r_nfo = new Object[5];
    Key left_key;
    long cur_absolute_pos;
    // If we are inserting the new leaf after,
    if (!before) {
      nfo = new Object[] {
                current_leaf.getReference(),
                (long) current_leaf.getSize(),
                new_leaf_key,
                new_leaf.getReference(),
                (long) new_leaf.getSize() };
      left_key = null;
      cur_absolute_pos = frame.getOffset() + current_leaf.getSize();
    }
    // Otherwise we are inserting the new leaf before,
    else {
      // If before and current_leaf key is different than new_leaf key, we
      // generate an error
      if (!current_leaf_key.equals(new_leaf_key)) {
        throw new RuntimeException("Can't insert different new key before.");
      }
      nfo = new Object[] {
                new_leaf.getReference(),
                (long) new_leaf.getSize(),
                current_leaf_key,
                current_leaf.getReference(),
                (long) current_leaf.getSize() };
      left_key = new_leaf_key;
      cur_absolute_pos = frame.getOffset() - 1;
    }

    boolean insert_two_nodes = true;

    final int sz = getFrameCount();
    // Walk the stack from the end
    for (int i = 1; i < sz; ++i) {
      // child_i is the previous frame's child_i
      int child_i = frame.getChildI();
      frame = stackEnd(i);
      // The child ref of this stack element
      final NodeReference child_ref = frame.getNodeReference();
      // Fetch it
      TreeBranch branch = (TreeBranch) unfreezeNode(fetchNode(child_ref));
//        long branch_ref = branch.getReference();
//        int child_i = branch.childWithReference(child_ref);

      // Do we have two nodes to insert into the branch?
      if (insert_two_nodes) {
        TreeBranch insert_branch;
        int insert_n = child_i;
        // If the branch is full,
        if (branch.isFull()) {
          // Create a new node,
          TreeBranch left_branch = branch;
          TreeBranch right_branch = createEmptyBranch();
          // Split the branch,
          Key midpoint_key = left_branch.midpointKey();
          // And move half of this branch into the new branch
          left_branch.moveLastHalfInto(right_branch);
          // We split so we need to return a split flag,
          r_nfo[0] = left_branch.getReference();
          r_nfo[1] = left_branch.getLeafElementCount();
          r_nfo[2] = midpoint_key;
          r_nfo[3] = right_branch.getReference();
          r_nfo[4] = right_branch.getLeafElementCount();
          // Adjust insert_n and insert_branch
          if (insert_n >= left_branch.size()) {
            insert_n -= left_branch.size();
            insert_branch = right_branch;
            r_nfo[4] = (Long) r_nfo[4] + new_leaf.getSize();
            // If insert_n == 0, we change the midpoint value to the left
            // key value,
            if (insert_n == 0 && left_key != null) {
              r_nfo[2] = left_key;
              left_key = null;
            }
          }
          else {
            insert_branch = left_branch;
            r_nfo[1] = (Long) r_nfo[1] + new_leaf.getSize();
          }
        }
        // If it's not full,
        else {
          insert_branch = branch;
          r_nfo[0] = insert_branch.getReference();
          insert_two_nodes = false;
        }
        // Insert the two children nodes
        insert_branch.insert( (NodeReference) nfo[0], (Long) nfo[1],
                              (Key) nfo[2],
                              (NodeReference) nfo[3], (Long) nfo[4],
                              insert_n );
        // Copy r_nfo to nfo
        for (int p = 0; p < r_nfo.length; ++p) {
          nfo[p] = r_nfo[p];
        }

        // Adjust the left key reference if necessary
        if (left_key != null && insert_n > 0) {
          insert_branch.setKeyValueToLeft(left_key, insert_n);
          left_key = null;
        }
      }
      else {
        branch.setChild((NodeReference) nfo[0], child_i);
        nfo[0] = branch.getReference();
        branch.setChildLeafElementCount(
              branch.getChildLeafElementCount(child_i) + leaf_size, child_i);

        // Adjust the left key reference if necessary
        if (left_key != null && child_i > 0) {
          branch.setKeyValueToLeft(left_key, child_i);
          left_key = null;
        }
      }

    } // For all elements in the stack,

    // At the end, if we still have a split then we make a new root and
    // adjust the stack accordingly
    if (insert_two_nodes) {
      TreeBranch new_root = createEmptyBranch();
      new_root.set((NodeReference) nfo[0], (Long) nfo[1],
                   (Key) nfo[2],
                   (NodeReference) nfo[3], (Long) nfo[4]);
      setRootNodeRef(new_root.getReference());
      if (getTreeHeight() != -1) {
        setTreeHeight(getTreeHeight() + 1);
      }
//        root_node_ref = new_root.getReference();
//        // The tree height has increased,
//        if (tree_height != -1) {
//          ++tree_height;
//        }
    }
    else {
      setRootNodeRef((NodeReference) nfo[0]);
//        root_node_ref = (NodeReference) nfo[0];
    }

    // Now reset the position,
    reset();
    setupForPosition(new_key, cur_absolute_pos);
  }

  /**
   * Redistributes the nodes in the 'branch' node assuming that 'child_i'
   * points to a child of the branch that needs rebalancing (typically
   * because the number of elements in the branch has fallen below some
   * threshold).  The 'child' object is the actual child branch.
   * <p>
   * Returns true if the effect of this operation is that the branch changed
   * in size (shrunk by 1).
   */
  private boolean redistributeBranchElements(TreeBranch branch,
                          int child_i, TreeBranch child) throws IOException {
    // We distribute the nodes in the child branch with the branch
    // immediately to the right.  If that's not possible, then we distribute
    // with the left.

    // If branch has only a single value, return immediately
    int branch_size = branch.size();
    if (branch_size == 1) {
      return false;
    }

    int left_i, right_i;
    TreeBranch left, right;
    if (child_i < branch_size - 1) {
      // Distribute with the right
      left_i = child_i;
      right_i = child_i + 1;
      left = child;
      right = (TreeBranch)
                       unfreezeNode(fetchNode(branch.getChild(child_i + 1)));
      branch.setChild(right.getReference(), child_i + 1);
    }
    else {
      // Distribute with the left
      left_i = child_i - 1;
      right_i = child_i;
      left = (TreeBranch)
                       unfreezeNode(fetchNode(branch.getChild(child_i - 1)));
      right = child;
      branch.setChild(left.getReference(), child_i - 1);
    }

    // Get the mid value key reference
    Key mid_key = branch.getKeyValue(right_i);

//      if (left.isEmpty()) {
//        System.out.println(" LEFT IS EMPTY");
//      }
//      if (right.isEmpty()) {
//        System.out.println(" RIGHT IS EMPTY");
//      }

    // Perform the merge,
    Key new_mid_key = left.merge(right, mid_key);
    // Reset the leaf element count
    branch.setChildLeafElementCount(left.getLeafElementCount(), left_i);
    branch.setChildLeafElementCount(right.getLeafElementCount(), right_i);

    // If after the merge the right branch is empty, we need to remove it
    if (right.isEmpty()) {
      // Delete the node
      deleteNode(right.getReference());
      // And remove it from the branch,
      branch.removeChild(right_i);
      return true;
    }
    else {
      // Otherwise set the key reference
      branch.setKeyValueToLeft(new_mid_key, right_i);
      return false;
    }

  }

  // ----- Public methods -----

  /**
   * Returns the current leaf node.
   */
  public TreeLeaf getCurrentLeaf() {
    return current_leaf;
  }

  /**
   * Returns the leaf of the current leaf node.
   */
  public Key getCurrentLeafKey() {
    return current_leaf_key;
  }

  /**
   * Returns the offset into the current leaf of the position that we moved
   * to.
   */
  public int getLeafOffset() {
    return leaf_offset;
  }

  /**
   * Returns true if the stack is positioned 1 past the end of the key used
   * in 'setupForPosition'.
   */
  public boolean isAtEndOfKeyData() {
    return getLeafOffset() >= leafSize();
  }

  /**
   * Walks the tree and sets up this stack for an absolute position in the
   * serialization of all information in the tree.  The 'key' parameter is
   * necessary for correctly choosing a path that falls on the boundary
   * of leaf nodes.  All operations on the stack are based on calling this
   * first.
   * <p>
   * Note that setupForPosition on a position that is 1 byte past the end of
   * the node sequence will position 'current_leaf' on the last node with the
   * same key, and leaf_offset will reference past the end of the leaf.
   * <p>
   * Giving this method a 'key' of Key.TAIL_KEY will make this method set
   * up the stack for the position regardless of the key of the leaf.
   */
  public void setupForPosition(Key key, long posit) throws IOException {

//      printDebugOutput();
//      System.out.println("setupForPosition " + key + ", " + posit);
    // If the current leaf is set
    if (current_leaf != null) {
//        // Exit early if we are already setup for this
//        final long leaf_start = stackEnd(1);
//        final long leaf_end = leaf_start + current_leaf.getSize();
//        if (key.equals(current_leaf_key) &&
//            posit >= leaf_start && posit < leaf_end) {
//          // Update leaf offset and return
//          leaf_offset = (int) (posit - leaf_start);
//          return;
//        }
//        // Pop the end of the stack
//        stackPop();
//        stackPop();
//        stackPop();

      StackFrame frame = stackEnd(0);
      final long leaf_start = frame.getOffset();
      final long leaf_end = leaf_start + current_leaf.getSize();
      // If the position is at the leaf end, or if the keys aren't equal, we
      // need to reset the stack.  This ensures that we correctly place the
      // pointer.
      if (!key.equals(Key.TAIL_KEY) &&
              (posit == leaf_end || !key.equals(current_leaf_key))) {
        stackClear();
        current_leaf = null;
        current_leaf_key = null;
      }
      else {
        // Check whether the position is within the bounds of the current leaf
        // If 'posit' is within this leaf
        if (posit >= leaf_start && posit < leaf_end) {
          // If the position is within the current leaf, set up the internal
          // vars as necessary.
          leaf_offset = (int) (posit - leaf_start);
          return;
        }
        else {
          // If it's not, we reset the stack and start fresh,
          stackClear();
//            // If it's not, we pop the top of the stack and null the current leaf.
//            stackPop();
//            stackPop();
//            stackPop();
          current_leaf = null;
          current_leaf_key = null;
        }
      }
    }

    // ISSUE: It appears looking at the code above, the stack will always be
    //   empty and current_leaf will always be null if we get here.

    // If the stack is empty, push the root node,
    if (stackEmpty()) {
      // Push the root node onto the top of the stack.
      stackPush(-1, 0, getRootNodeRef());
      // Set up the current_leaf_key to the default value
      current_leaf_key = Key.HEAD_KEY;
    }
    // Otherwise, we need to setup by querying the BTree.
    while (true) {
      if (stackEmpty()) {
        throw new RuntimeException("Position out of bounds.  p = " + posit);
      }

      // Pop the last stack frame,
      StackFrame frame = stackPop();
      final NodeReference node_pointer = frame.getNodeReference();
      final long left_side_offset = frame.getOffset();
      final int node_child_i = frame.getChildI();
//        final long node_pointer = stackPop();
//        final long left_side_offset = stackPop();
//        final int node_child_i = (int) stackPop();
      // Relative offset within this node
      final long relative_offset = posit - left_side_offset;

      // If the node is not on the heap,
      if (!isHeapNode(node_pointer)) {
        // The node is not on the heap. We optimize here.
        // If we know the node is going to be a leaf node, we set up a
        // temporary leaf node object with as much information as we know.

        // Check if we know this is a leaf
        int tree_height = getTreeHeight();
        if (tree_height != -1 &&
            (stack_size / STACK_FRAME_SIZE) + 1 == tree_height) {

          // Fetch the parent node,
          frame = stackEnd(0);
          NodeReference twig_node_pointer = frame.getNodeReference();
          TreeBranch twig = (TreeBranch) fetchNode(twig_node_pointer);
          long leaf_size =
                         twig.getChildLeafElementCount((int) node_child_i);

//              // We've reached a leaf node,
//              TreeNode node = fetchNode(node_pointer);
//              TreeLeaf leaf = (TreeLeaf) node;

          // This object holds off fetching the contents of the leaf node
          // unless it's absolutely required.
          TreeLeaf leaf =
                      new PlaceholderLeaf(ts, node_pointer, (int) leaf_size);

          current_leaf = leaf;
          stackPush(node_child_i, left_side_offset, node_pointer);
          // Set up the leaf offset and return
          leaf_offset = (int) relative_offset;

          return;
        }
      }

      // Fetch the node
      TreeNode node = fetchNode(node_pointer);
      if (node instanceof TreeLeaf) {
        // Node is a leaf node
        TreeLeaf leaf = (TreeLeaf) node;

        current_leaf = leaf;
        stackPush(node_child_i, left_side_offset, node_pointer);
        // Set up the leaf offset and return
        leaf_offset = (int) relative_offset;

        // Update the tree_height value,
        setTreeHeight(stack_size / STACK_FRAME_SIZE);
//          tree_height = (stack_size / STACK_FRAME_SIZE);
        return;
      }
      else {
        // Node is a branch node
        TreeBranch branch = (TreeBranch) node;
        int child_i = branch.childAtOffset(key, relative_offset);
        if (child_i != -1) {
          // Push the current details,
          stackPush(node_child_i, left_side_offset, node_pointer);
          // Found child so push the details
          stackPush(child_i,
                    branch.childOffset(child_i) + left_side_offset,
                    branch.getChild(child_i));
          // Set up the left key
          if (child_i > 0) {
            current_leaf_key = branch.getKeyValue(child_i);
          }
        }
      }
    } // while (true)
  }

  /**
   * Deletes the current node and adjusts the stack as necessary.  Resets the
   * stack.
   */
  public void deleteLeaf(Key key) throws IOException {

    // Set up the state
    StackFrame frame = stackEnd(0);
    NodeReference this_ref = frame.getNodeReference();
    TreeBranch branch_node = null;
    int delete_node_size = -1;
    Key left_key = null;

    // Walk back through the rest of the stack
    final int sz = getFrameCount();
    for (int i = 1; i < sz; ++i) {

      // Find the child_i for the child
      // This is the child_i of the child in the current branch
      int child_i = frame.getChildI();

      // Move the stack frame,
      frame = stackEnd(i);

      NodeReference child_ref = this_ref;
      this_ref = frame.getNodeReference();
      TreeBranch child_branch = branch_node;
      branch_node = (TreeBranch) unfreezeNode(fetchNode(this_ref));

      if (delete_node_size == -1) {
        delete_node_size =
                       (int) branch_node.getChildLeafElementCount(child_i);
      }

      // If the child branch is empty,
      if (child_branch == null || child_branch.isEmpty()) {
        // Delete the reference to it,
        if (child_i == 0 && branch_node.size() > 1) {
          left_key = branch_node.getKeyValue(1);
        }
        branch_node.removeChild(child_i);
        // Delete the child branch,
        deleteNode(child_ref);
      }
      // Not empty,
      else {
        // Replace with the new child node reference
        branch_node.setChild(child_branch.getReference(), child_i);
        // Set the element count
        long new_child_size =
                branch_node.getChildLeafElementCount(child_i) -
                delete_node_size;
        branch_node.setChildLeafElementCount(new_child_size, child_i);
        // Can we set the left key reference?
        if (child_i > 0 && left_key != null) {
          branch_node.setKeyValueToLeft(left_key, child_i);
          left_key = null;
        }

        // Has the size of the child reached the lower threshold?
        if (child_branch.size() <= 2) {
          // If it has, we need to redistribute the children,
          redistributeBranchElements(branch_node, child_i, child_branch);
        }

      }

    }

    // Finally, set the root node
    // If the branch node is a single element, we set the root as the child,
    if (branch_node.size() == 1) {
      // This shrinks the height of the tree,
      setRootNodeRef(branch_node.getChild(0));
      deleteNode(branch_node.getReference());
      if (getTreeHeight() != -1) {
        setTreeHeight(getTreeHeight() - 1);
      }
//        root_node_ref = branch_node.getChild(0);
//        if (tree_height != -1) {
//          tree_height = tree_height - 1;
//        }
    }
    else {
      // Otherwise, we set the branch node.
      setRootNodeRef(branch_node.getReference());
//        root_node_ref = branch_node.getReference();
    }

    // Reset the object
    reset();

  }

  /**
   * From the given position, removes nodes immediately before the current
   * node and removes up to 'n' elements from the file.  The stack must be
   * set up on the start of the node for this to be successful.
   */
  public void removeSpaceBefore(Key key,
                                long amount_to_remove) throws IOException {
//      if (leaf_offset != 0) {
//        System.out.println("leaf_offset = " + leaf_offset);
//        throw new RuntimeException("Invalid state.");
//      }

    StackFrame frame = stackEnd(0);
    long p = (frame.getOffset() + leaf_offset) - 1;
    while (true) {
      setupForPosition(key, p);
      int leaf_size = leafSize();
      if (amount_to_remove >= leaf_size) {
        deleteLeaf(key);
        amount_to_remove -= leaf_size;
      }
      else {
        if (amount_to_remove > 0) {
          setupForPosition(key, (p - amount_to_remove) + 1);
          trimAtPosition();
        }
        return;
      }
      p -= leaf_size;
    }
  }

  /**
   * Deletes all nodes between the current node and the node with the given
   * position, not including those nodes.  Returns the number of bytes
   * deleted.
   */
  public long deleteAllNodesBackTo(Key key,
                                   long back_position) throws IOException {

    StackFrame frame = stackEnd(0);

    long p = frame.getOffset() - 1;
    long bytes_removed = 0;

    while (true) {
      // Set up for the node,
//        System.out.println("SETUP FOR: " + p);
//        printDebugOutput();
      setupForPosition(key, p);
      // This is the stopping condition, when the start of the node is
      // before the back_position,
      if (stackEnd(0).getOffset() <= back_position) {
        return bytes_removed;
      }
      // The current leaf size
      int leaf_size = leafSize();
      // The bytes removed is the size of the leaf,
      bytes_removed += leafSize();
      // Otherwise, delete the leaf
      deleteLeaf(key);

      p -= leaf_size;
    }
  }

  /**
   * Splits the current leaf at the given position into 2 leaf nodes.
   */
  public void splitLeaf(Key key, long position) throws IOException {
    unfreezeStack();
    TreeLeaf source_leaf = getCurrentLeaf();
    int split_point = getLeafOffset();
    // The amount of data we are copying from the current key.
    int amount = source_leaf.getSize() - split_point;
    // Create a new empty node
    TreeLeaf empty_leaf = createEmptyLeaf(key);
    empty_leaf.setSize(amount);
    // Copy the data at the end of the leaf into a buffer
    byte[] buf = new byte[amount];
    source_leaf.get(split_point, buf, 0, amount);
    // And write it out to the new leaf
    empty_leaf.put(0, buf, 0, amount);
    // Set the new size of the node
    source_leaf.setSize(split_point);
    // Update the stack properties
    updateStackProperties(-amount);
    // And insert the new leaf after
    insertLeaf(key, empty_leaf, false);
  }

  /**
   * Add nodes to make up empty space after the current node.
   */
  public void addSpaceAfter(Key key, long space_to_add) throws IOException {
    while (space_to_add > 0) {
      // Create an empty sparse node
      TreeLeaf empty_leaf = createSparseLeaf(key, (byte) 0, space_to_add);
//        TreeLeaf empty_leaf = createEmptyLeaf(key);
//        empty_leaf.setSize(
//               (int) Math.min(space_to_add, (long) empty_leaf.getCapacity()));
      insertLeaf(key, empty_leaf, false);
      space_to_add -= empty_leaf.getSize();
    }
  }

  /**
   * Expand the current leaf by the given size or up to the capacity of the
   * leaf, and returns how much the leaf was expanded by.
   */
  public int expandLeaf(long amount) throws IOException {
    if (amount > 0) {
      unfreezeStack();
      int actual_expand_by = (int) Math.min(
                (long) current_leaf.getCapacity() - current_leaf.getSize(),
                amount);
      if (actual_expand_by > 0) {
        current_leaf.setSize(current_leaf.getSize() + actual_expand_by);
        updateStackProperties(actual_expand_by);
      }
      return actual_expand_by;
    }
    return 0;
  }

  /**
   * Trims the leaf to the given position.
   */
  public void trimAtPosition() throws IOException {
    unfreezeStack();
    int size_before = current_leaf.getSize();
    current_leaf.setSize(leaf_offset);
    updateStackProperties(leaf_offset - size_before);
  }

  /**
   * Shifts the data in current leaf at the current position foward or
   * backwards by the given amount.
   */
  public void shiftLeaf(long amount) throws IOException {
    if (amount != 0) {
      unfreezeStack();
      int size_before = current_leaf.getSize();
      current_leaf.shift(leaf_offset, (int) amount);
      updateStackProperties(current_leaf.getSize() - size_before);
    }
  }

  /**
   * Attempts to move to the next leaf with the same key.  Returns true if
   * successful or false if the pointer is within the last node.
   */
  public boolean moveToNextLeaf(Key key) throws IOException {
    long next_pos = stackEnd(0).getOffset() + current_leaf.getSize();
    setupForPosition(key, next_pos);
    return leaf_offset != current_leaf.getSize() &&
           current_leaf_key.equals(key);
  }

  /**
   * Attempts to move to the previous leaf with the same key.  Returns true
   * if successful or false if the pointer is within the first node.
   */
  public boolean moveToPreviousLeaf(Key key) throws IOException {
    long previous_pos = stackEnd(0).getOffset() - 1;
    setupForPosition(key, previous_pos);
    return current_leaf_key.equals(key);
  }

  /**
   * Returns the current leaf node size.
   */
  public int leafSize() {
    return current_leaf.getSize();
  }

  /**
   * Returns the amount of free space in the current leaf.
   */
  public int leafSpareSpace() {
    return getTreeSystem().getMaxLeafByteSize() - current_leaf.getSize();
  }

  /**
   * Reads a sequence of bytes from the current position in the sequence
   * into the given array.  This will change the state of the stack as
   * necessary.
   */
  public void readInto(Key key, long current_p,
                       byte[] buf, int off, int len) throws IOException {
    // While there is information to read into the array,
    while (len > 0) {
      // Set up the stack and internal variables for the given position,
      setupForPosition(key, current_p);
//        System.out.println("current_p=" + current_p);
//        System.out.println("current_leaf = " + getCurrentLeaf().getReference());
      // Read as much as we can from the current leaf capped at the leaf size
      // if necessary,
      int to_read = Math.min(len, current_leaf.getSize() - leaf_offset);
      if (to_read == 0) {
        System.out.println("current_p = " + current_p);
        System.out.println("key = " + key);
        System.out.println("off = " + off);
        System.out.println("len = " + len);
        System.out.println("current_leaf.getSize() = " + current_leaf.getSize());
        System.out.println("leaf_offset = " + leaf_offset);
        throw new RuntimeException("Read out of bounds.");
      }
      // Copy the leaf into the array,
      current_leaf.get(leaf_offset, buf, off, to_read);
      // Modify the pointers
      current_p += to_read;
      off += to_read;
      len -= to_read;
    }
  }

  /**
   * Writes a sequence of bytes from the current position in the sequence
   * from the array.  This will change the state of the stack as necessary
   * but will not cause new nodes to be created.
   */
  public void writeFrom(Key key, long current_p,
                        byte[] buf, int off, int len) throws IOException {
    // While there is information to read into the array,
    while (len > 0) {
      // Set up the stack and internal variables for the given position,
      setupForPosition(key, current_p);
      // Unfreeze all the nodes currently on the stack,
      unfreezeStack();
      // Read as much as we can from the current leaf capped at the leaf size
      // if necessary,
      int to_write = Math.min(len, current_leaf.getSize() - leaf_offset);
//        System.out.println("WRITE len = " + len);
//        System.out.println("WRITE current_leaf size = " + current_leaf.getSize());
//        System.out.println("WRITE leaf_ofset = " + leaf_offset);
      if (to_write == 0) {
        throw new RuntimeException("Write out of bounds.");
      }
      // Copy the leaf into the array,
      current_leaf.put(leaf_offset, buf, off, to_write);
      // Modify the pointers
      current_p += to_write;
      off += to_write;
      len -= to_write;
    }
  }

  /**
   * Shifts elements at the given position forward or backwards by the given
   * amount.
   * <p>
   * Note that 'position' is an absolute position.  This method does not
   * perform any assertions on whether the data being removed by the shift
   * is valid or not.  Removing more data than stored in the file will result
   * in undefined behaviour.
   */
  public void shiftData(final Key key,
                        long position, long shift_offset) throws IOException {

    // Return if nothing being shifted
    if (shift_offset == 0) {
      return;
    }

    // If we are removing a large amount of data,
    // TODO: Rather arbitrary value here...
    if (shift_offset < -(32 * 1024)) {
      // If removing more than 32k of data use the generalized tree pruning
      // algorithm which works well on large data.
      reset();
      removeAbsoluteBounds(position + shift_offset, position);
    }
    // shift_offset > 0
    else {
      // Set up for the given position
      setupForPosition(key, position);
      // If there is no leaf node for this key yet, it's an empty file so we
      // add new data and return.
      if (!getCurrentLeafKey().equals(key)) {
        // If we are expanding, then add the extra space and return
        // We can't shrink an empty file.
        if (shift_offset >= 0) {
          // No, so add empty nodes of the required size to make up the space
          addSpaceAfter(key, shift_offset);
        }
        return;
      }
      // If we are at the end of the data, we simply expand or reduce the data
      // by the shift amount
      if (isAtEndOfKeyData()) {
        if (shift_offset > 0) {
          // Expand,
          long to_expand_by = shift_offset;
          to_expand_by -= expandLeaf(to_expand_by);
          // And add nodes for the remaining
          addSpaceAfter(key, to_expand_by);
          // And return
          return;
        }
        else {
          // Remove the space immediately before this node up to the given
          // amount.
          removeSpaceBefore(key, -shift_offset);
          // And return
          return;
        }
      }
      else {
        // Can we shift data in the leaf and complete the operation?
        if ((shift_offset > 0 && leafSpareSpace() >= shift_offset) ||
            (shift_offset < 0 && getLeafOffset() + shift_offset >= 0)) {
          // We can simply shift the data in the node
          shiftLeaf(shift_offset);
          return;
        }
        // There isn't enough space in the current node,
        if (shift_offset > 0) {
          // If we are expanding,
          // The data to copy from the leaf
          int buf_size = leafSize() - getLeafOffset();
          byte[] buf = new byte[buf_size];
          readInto(key, position, buf, 0, buf_size);
          final long leaf_end = position + buf_size;
          // Record the amount of spare space available in this node.
          long space_available = leafSpareSpace();
          // Is there a node immediately after we can shift the data into?
          boolean successful = moveToNextLeaf(key);
          if (successful) {
            // We were successful at moving to the next node, so determine if
            // there is space available here to make the shift
            if (leafSpareSpace() + space_available >= shift_offset) {
              // Yes there is, so lets make room,
              shiftLeaf(shift_offset - space_available);
              // Move back
              setupForPosition(key, position);
              // Expand this node to max size
              expandLeaf(space_available);
              // And copy,
              writeFrom(key, position + shift_offset, buf, 0, buf_size);
              // Done,
              return;
            }
            else {
              // Not enough spare space available in the node with the
              // shift point and the next node, so we need to make new nodes,
              setupForPosition(key, position);
              // Expand this node to max size
              expandLeaf(space_available);
              // Add nodes after it
              addSpaceAfter(key, shift_offset - space_available);
              // And copy,
              writeFrom(key, position + shift_offset, buf, 0, buf_size);
              // Done,
              return;
            }
          }
          else {
            // If we were unsuccessful at moving data to the next leaf, we must
            // be at the last node in the file.

            // Expand,
            long to_expand_by = shift_offset;
            to_expand_by -= expandLeaf(to_expand_by);
            // And add nodes for the remaining
            addSpaceAfter(key, to_expand_by);
            // And copy,
            writeFrom(key, position + shift_offset, buf, 0, buf_size);
            // Done,
            return;
          }
        }
        // shift_offset is < 0
        else {
          // We need to reduce,
          // The data to copy from the leaf
          int buf_size = leafSize() - getLeafOffset();
          byte[] buf = new byte[buf_size];
          readInto(key, position, buf, 0, buf_size);
          // Set up to the point where we will be inserting the data into,
          setupForPosition(key, position);

          // Delete all the nodes between the current node and the destination
          // node, but don't delete either the destination node or this node
          // in the process.
          long bytes_removed = deleteAllNodesBackTo(key,
                                                     position + shift_offset);

          // Position
          setupForPosition(key, position + shift_offset);
          // Record the amount of spare space available in this node.
          long space_available = leafSpareSpace();
          // Expand the leaf
          expandLeaf(space_available);
          // Will we be writing over two nodes?
          boolean writing_over_two_nodes =
                       buf_size > (leafSize() - getLeafOffset());
          boolean writing_complete_node =
                      buf_size == (leafSize() - getLeafOffset());
          // Write the data,
          writeFrom(key, position + shift_offset, buf, 0, buf_size);
          // Move to the end of what we just inserted,
          setupForPosition(key, position + shift_offset + buf_size);
          // Trim the node,
          if (!writing_complete_node) {
            trimAtPosition();
          }
          if (!writing_over_two_nodes) {
            // Move to the end of what we just inserted,
            setupForPosition(key, position + shift_offset + buf_size);
            // Delete this node
            deleteLeaf(key);
          }
          // Finished
        }
      }
    }
  }




  /**
   * Resets the stack.  This should be called when the tree changes outside
   * of the manipulation methods in this tree, for example, when another
   * tree stack is manipulated.  If the tree stack is not reset after the
   * tree has changed then the use of this object is undefined.  It is not
   * necessary to call this after this tree has been manipulated.
   */
  public void reset() {
    stackClear();
    current_leaf = null;
    current_leaf_key = null;
  }


  /**
   * A placeholder tree leaf object for optimizing tree operations.
   */
  private static class PlaceholderLeaf extends TreeLeaf {

    private final TreeSystemTransaction ts;

    private TreeLeaf actual_leaf;
    private NodeReference node_ref;
    private int size;

    PlaceholderLeaf(TreeSystemTransaction ts,
                    NodeReference node_ref, int size) {
      super();
      this.ts = ts;
      this.node_ref = node_ref;
      this.size = size;
    }

    private TreeLeaf actualLeaf() throws IOException {
      if (actual_leaf == null) {
        actual_leaf = (TreeLeaf) ts.fetchNode(getReference());
      }
      return actual_leaf;
    }

    @Override
    public NodeReference getReference() {
      return node_ref;
    }

    @Override
    public int getSize() {
      return size;
    }

    @Override
    public void get(int position, byte[] buf, int off, int len) throws IOException {
      actualLeaf().get(position, buf, off, len);
    }

    @Override
    public byte get(int position) throws IOException {
      return actualLeaf().get(position);
    }

    @Override
    public int getCapacity() {
      // Not supported, this object will never be a leaf node.
      throw new UnsupportedOperationException();
    }

    @Override
    public void put(int position, byte[] buf, int off, int len) throws IOException {
      actualLeaf().put(position, buf, off, len);
    }

    @Override
    public void setSize(int size) throws IOException {
      actualLeaf().setSize(size);
    }

    @Override
    public void shift(int position, int offset) throws IOException {
      actualLeaf().shift(position, offset);
    }

    @Override
    public void writeDataTo(AreaWriter area) throws IOException {
      actualLeaf().writeDataTo(area);
    }

    public int getHeapSizeEstimate() {
      throw new UnsupportedOperationException();
    }

  }

  /**
   * A single stack frame item in a TreeStack.
   */
  private final static class StackFrame {

    private final long[] stack;
    private final int off;

    private StackFrame(long[] stack, int off) {
      this.stack = stack;
      this.off = off;
    }

    private int getChildI() {
      return (int) stack[off];
    }

    private long getOffset() {
      return stack[off + 1];
    }

    private NodeReference getNodeReference() {
      return new NodeReference(stack[off + 2], stack[off + 3]);
    }

    private void setNodeReference(NodeReference node_ref) {
      stack[off + 2] = node_ref.getHighLong();
      stack[off + 3] = node_ref.getLowLong();
    }

  }


}
