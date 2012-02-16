/**
 * com.mckoi.data.TreeWriteSequence  Dec 7, 2008
 *
 * Mckoi Database Software ( http://www.mckoi.com/ )
 * Copyright (C) 2000 - 2010  Diehl and Associates, Inc.
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
