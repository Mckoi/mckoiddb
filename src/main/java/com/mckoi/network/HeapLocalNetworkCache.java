/**
 * com.mckoi.network.HeapLocalNetworkCache  Dec 7, 2008
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

import com.mckoi.data.TreeNode;
import com.mckoi.util.Cache;
import java.util.HashMap;
import java.util.List;

/**
 * A LocalNetworkCache implementation that stores nodes on the Java heap, and
 * automatically deletes nodes when a certain threshold of nodes is stored.
 *
 * @author Tobias Downer
 */

public class HeapLocalNetworkCache implements LocalNetworkCache {

  /**
   * The cache.
   */
  private final LocalCache heap_cache;

  /**
   * The block to servers list cache.
   */
  private final HashMap<BlockId, BlockCacheElement> s2block_cache;

  /**
   * The path name to path info cache.
   */
  private final HashMap<String, PathInfo> path_info_map;

  /**
   * Constructs the heap cache with a maximum size in bytes of all node data
   * that may be stored.
   */
  public HeapLocalNetworkCache(long max_cache_size) {
    this.heap_cache = new LocalCache(Cache.closestPrime(12000), max_cache_size);
    this.s2block_cache = new HashMap(1023);
    this.path_info_map = new HashMap(255);
  }

  /**
   * Default constructor that creates a heap cache with a maximum size of
   * 32 megabytes.
   */
  public HeapLocalNetworkCache() {
    this(32 * 1024 * 1024);
  }

  // ---------- Implemented from LocalNetworkCache ----------
  
  public void putNode(DataAddress addr, TreeNode node) {
    synchronized (heap_cache) {
      heap_cache.put(addr, node);
    }
  }

  public TreeNode getNode(DataAddress addr) {
    synchronized (heap_cache) {
      return (TreeNode) heap_cache.get(addr);
    }
  }

  public void deleteNode(DataAddress addr, TreeNode node) {
    synchronized (heap_cache) {
      heap_cache.remove(addr);
    }
  }

  public List<BlockServerElement> getServersWithBlock(BlockId block_id) {
    synchronized (s2block_cache) {
      BlockCacheElement ele = s2block_cache.get(block_id);
      if (ele == null || System.currentTimeMillis() > ele.time_to_end) {
        return null;
      }
      return ele.block_servers;
    }
  }

  public void putServersForBlock(BlockId block_id,
                            List<BlockServerElement> servers, int ttl_hint) {

    BlockCacheElement ele = new BlockCacheElement();
    ele.block_servers = servers;
    ele.time_to_end = System.currentTimeMillis() + ttl_hint;

    synchronized (s2block_cache) {
      s2block_cache.put(block_id, ele);
    }
  }

  public void removeServersWithBlock(BlockId block_id) {
    synchronized (s2block_cache) {
      s2block_cache.remove(block_id);
    }
  }


  public PathInfo getPathInfo(String path_name) {
    synchronized (path_info_map) {
      return path_info_map.get(path_name);
    }
  }

  public void putPathInfo(String path_name, PathInfo path_info) {
    synchronized (path_info_map) {
      path_info_map.put(path_name, path_info);
    }
  }


  // ------ Inner classes -----

  private static class BlockCacheElement {
    long time_to_end;
    List<BlockServerElement> block_servers;
  }

  // ISSUE: We have static default values plugged in here. Branch nodes we
  //   estimate to be 1024 bytes in size, leaf nodes 4096. The actual sizes
  //   may be very different.

  private static class LocalCache extends Cache {

    /**
     * An estimate of the size of the cache on the heap, in bytes.
     */
    private long size_estimate;

    /**
     * The maximum size of the local cache in bytes.
     */
    private final long max_cache_size;
    private final long clean_to;


    private LocalCache(int size, long max_cache_size) {
      super(size);
      size_estimate = 0;
      this.max_cache_size = max_cache_size;
      this.clean_to = (long) ((double) max_cache_size * (double) .75);
    }

    protected void checkClean() {
      // If we have reached maximum cache size, remove some elements from the
      // end of the list
      if (size_estimate >= max_cache_size) {
        clean();
      }
    }

    protected boolean shouldWipeMoreNodes() {
      return size_estimate >= clean_to;
    }

    protected void notifyObjectAdded(Object key, Object val) {
      size_estimate += ((TreeNode) val).getHeapSizeEstimate() + 64;
//      if (val instanceof TreeBranch) {
//        size_estimate += 1024;
//      }
//      else {
//        size_estimate += 4096;
//      }
    }

    protected void notifyObjectRemoved(Object key, Object val) {
      size_estimate -= ((TreeNode) val).getHeapSizeEstimate() + 64;
//      if (val instanceof TreeBranch) {
//        size_estimate -= 1024;
//      }
//      else {
//        size_estimate -= 4096;
//      }
    }

    protected void notifyAllCleared() {
      size_estimate = 0;
    }

  }

}
