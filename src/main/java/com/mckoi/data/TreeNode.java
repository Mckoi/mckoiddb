/**
 * com.mckoi.treestore.TreeNode  09 Oct 2004
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

/**
 * Represents a node in the tree.
 * 
 * @author Tobias Downer
 */

public interface TreeNode {

  /**
   * Returns the address of this node.  If the address is less than 0 then the
   * node is located on the mutable node heap and this object is mutable.  If
   * the address is greater or equal to 0 then the node is immutable and in the
   * store.
   */
  NodeReference getReference();

  /**
   * Returns a heap size estimate for the consumption of this tree node on
   * the Java Heap. This is used to estimate how much memory a cache of tree
   * nodes consumes. The calculation of this value should be fairly accurate,
   * being an overestimate if unsure.
   */
  int getHeapSizeEstimate();

}


