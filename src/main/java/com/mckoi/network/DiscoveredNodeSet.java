/**
 * com.mckoi.data.DiscoveredNodeSet  Feb 21, 2012
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

/**
 * An object used by the NetworkTreeSystem.discoverNodesInTree method that
 * is populated with the address location of all nodes in a tree as it is
 * traversed from the root node.
 *
 * @author Tobias Downer
 */

public interface DiscoveredNodeSet {

  /**
   * Adds a node reference to the set when the node has been discovered.
   * Returns true if the node was added to the set because it has not 
   * previously been discovered. Returns false if the node is already in the
   * set.
   */
  boolean add(NodeReference node_ref);

}
