/**
 * com.mckoi.network.NodeSet  Jul 16, 2009
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

package com.mckoi.network;

import com.mckoi.data.NodeReference;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

/**
 * A set of nodes from a block. This interface is used to create a binary
 * representation of a set of nodes grouped together. Grouping node
 * information together creates benefits such as improving compression and
 * reducing the number of requests for information (which has a high cost
 * with network latency).
 *
 * @author Tobias Downer
 */

public interface NodeSet {

  /**
   * Returns the set of node_ids of nodes in this set.
   */
  NodeReference[] getNodeIdSet();

  /**
   * Provides an iterator over the individual node items in this set, ordered
   * in the same order as the long array returned by 'getNodeIdSet()'.
   */
  public Iterator<NodeItemBinary> getNodeSetItems();

  /**
   * Writes the encoded form of this node set to the given DataOutputStream.
   */
  void writeEncoded(DataOutput dout) throws IOException;

}
