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
