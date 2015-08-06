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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * A sequence of commands needed to write a part of a tree that's currently
 * stored on the heap out to the backing storage medium.
 *
 * @author Tobias Downer
 */

public class TreeWriteSequence {

  /**
   * The set of leaf nodes allocated by this sequence,
   */
  private final ArrayList<TreeNode> nodel = new ArrayList(256);

  /**
   * The set of branch nodes allocated by this sequence,
   */
  private final ArrayList<TreeNode> nodeb = new ArrayList(256);

  /**
   * The set of link commands in this sequence,
   */
  private final HashMap<Long, Integer> linkc = new HashMap(256);

  /**
   * The counts,
   */
  static final int BPOINT = 65536 * 16384;
  
  
  public List<TreeNode> getAllLeafNodes() {
    return nodel;
  }

  public List<TreeNode> getAllBranchNodes() {
    return nodeb;
  }
  
  public int lookupRef(int branch_id, int child_i) {
    // NOTE: Returns the reference for branches normalized on a node list that
    //  includes branch and leaf nodes together in order branch + leaf.

    branch_id = (branch_id + BPOINT);
    
    // Turn {branch_id, child_i} into a key,
    long key = ((long) branch_id << 16L) + child_i;
    int ref_id = linkc.get(key);
    if (ref_id >= BPOINT) {
      return ref_id - BPOINT;
    }
    else {
      return ref_id + nodeb.size();
    }
  }

  public void sequenceBranchLink(int branch_id, int child_i, int child_id) {
    // Turn {branch_id, child_i} into a key,
    long key = ((long) branch_id << 16L) + child_i;
    linkc.put(key, child_id);
  }

  public int sequenceNodeWrite(TreeNode node) {
    if (node instanceof TreeBranch) {
      nodeb.add(node);
      return (nodeb.size() - 1) + BPOINT;
    }
    else {
      nodel.add(node);
      return nodel.size() - 1;
    }
  }

  
  
  
  
//  
//  /**
//   * The set of leaf nodes allocated by this sequence,
//   */
//  private final ArrayList<TreeNode> nodel = new ArrayList(256);
//
//  /**
//   * The set of link commands in this sequence,
//   */
//  private final HashMap<Long, Integer> linkc = new HashMap(256);
//
//  
//
//
//  public List<TreeNode> getAllNodes() {
//    return nodel;
//  }
//
//  public int lookupRef(int branch_id, int child_i) {
//    
//    // Turn {branch_id, child_i} into a key,
//    long key = ((long) branch_id << 16L) + child_i;
//    return linkc.get(key);
//  }
//
//  public void sequenceBranchLink(int branch_id, int child_i, int child_id) {
//    // Turn {branch_id, child_i} into a key,
//    long key = ((long) branch_id << 16L) + child_i;
//    linkc.put(key, child_id);
//  }
//
//  public int sequenceNodeWrite(TreeNode node) {
//    nodel.add(node);
//    return nodel.size() - 1;
//  }
//
//  
  
  
}
