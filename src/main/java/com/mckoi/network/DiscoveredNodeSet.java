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
