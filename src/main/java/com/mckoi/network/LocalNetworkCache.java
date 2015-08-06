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

import com.mckoi.data.TreeNode;
import java.util.List;

/**
 * A local cache of network topology information and node data. Implementations
 * may include persistent storage of information by the local services
 * available in the client instance, or weak caches with short lived (in time
 * and/or by session) storage, such as on the Java heap.
 * <p>
 * The node cache heavily exploits the property that a node address is
 * globally unique by value and with respect to all nodes valid now
 * and in the future. Implementations must also support caching of block to
 * server maps as provided by the manager server, with a time-to-live
 * component.
 *
 * @author Tobias Downer
 */

public interface LocalNetworkCache {

  // ----- Nodes -----

  /**
   * Returns the TreeNode value with the given DataAddress if it is stored in
   * this cache, or 'null' otherwise.
   */
  TreeNode getNode(DataAddress address);

  /**
   * Stores the TreeNode for the given DataAddress in the cache.
   */
  void putNode(DataAddress address, TreeNode node);

  /**
   * Removes the tree node referenced by the given DataAddress from the cache,
   * if it is stored within this cache. If the node is not stored in the
   * cache then nothing happens.
   */
  void deleteNode(DataAddress address, TreeNode node);

  // ----- PathInfo mapping -----

  /**
   * Returns the PathInfo stored with the given path_name if it's stored in
   * this cache, or 'null' otherwise.
   */
  PathInfo getPathInfo(String path_name);

  /**
   * Stored the PathInfo in the cache. If 'path_info' is null then the entry
   * is cleared in the cache.
   */
  void putPathInfo(String path_name, PathInfo path_info);

  // ----- Server mapping -----

  /**
   * Returns the list of servers in the network the block is stored on if the
   * information is stored in the cache, or 'null' if the information isn't
   * available in the cache. A cached element is valid until its TTL is
   * exceeded. If a request is made for a block that is in the cache but the
   * TTL is exceeded, then this method returns 'null' for that element.
   */
  List<BlockServerElement> getServersWithBlock(BlockId block_id);

  /**
   * Associates the set of servers with the given block identifier in the
   * cache with a TTL hint (time to live hint). The association is valid in
   * the cache until the current time is greater than the time the association
   * was made plus the TTL value.
   * <p>
   * Note that the 'servers' object should be considered immutable.
   */
  void putServersForBlock(BlockId block_id, List<BlockServerElement> servers,
                          int ttl_hint);

  /**
   * Removes a block_id to servers list association. This is intended to be
   * used in the case where we wish to force a status update on cached
   * status information.
   */
  void removeServersWithBlock(BlockId block_id);

}
