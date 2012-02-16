/**
 * com.mckoi.network.JVMState  Jul 3, 2009
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

import java.util.HashMap;

/**
 * Stateful information in this JVM (Java Virtual Machine) relevant to
 * information about the network. It is intended this class is used to
 * provide useful information to processes running in this JVM that need
 * access to a MckoiDDB distributed network.
 *
 * @author Tobias Downer
 */

public class JVMState {

  /**
   * A HashMap of manager server to local network cache data.
   */
  private final static
          HashMap<ServiceAddress, LocalNetworkCache> service_cache_map;

  /**
   * A HashMap of manager server to ServiceStatusTracker object.
   */
  private final static
          HashMap<ServiceAddress, ServiceStatusTracker> tracker_cache_map;


  static {
    service_cache_map = new HashMap();
    tracker_cache_map = new HashMap();
  }


  /**
   * Returns a tracker for the given network.
   */
  public static ServiceStatusTracker getJVMServiceTracker(
                       ServiceAddress[] managers, NetworkConnector network) {

    synchronized (tracker_cache_map) {
      ServiceStatusTracker picked = null;
      int picked_count = 0;
      for (int i = 0; i < managers.length; ++i) {
        ServiceStatusTracker g = tracker_cache_map.get(managers[i]);
        if (g != null) {
          picked = g;
          ++picked_count;
        }
      }
      if (picked == null) {
        picked = new ServiceStatusTracker(network);
        for (int i = 0; i < managers.length; ++i) {
          tracker_cache_map.put(managers[i], picked);
        }
      }
      else if (picked_count != managers.length) {
        for (int i = 0; i < managers.length; ++i) {
          tracker_cache_map.put(managers[i], picked);
        }
      }
      return picked;
    }

  }


  /**
   * Creates a default LocalCacheMap object for the given manager servers. A
   * default object for this is a HeapLocalNetworkCache configured to use
   * 32 MB of memory.
   */
  private static LocalNetworkCache createDefaultCacheFor(
                 ServiceAddress[] managers, CacheConfiguration cache_config) {
    long global_node_heap_size = cache_config.getGlobalNodeCacheSize();
    LocalNetworkCache cache = new HeapLocalNetworkCache(global_node_heap_size);
    return cache;
  }

  /**
   * Given the network manager servers, returns a LocalNetworkCache object that
   * is being used by processes in this JVM.
   */
  public static LocalNetworkCache getJVMCacheForManager(
                 ServiceAddress[] managers, CacheConfiguration cache_config) {
    synchronized (service_cache_map) {
      LocalNetworkCache picked = null;
      int picked_count = 0;
      for (int i = 0; i < managers.length; ++i) {
        LocalNetworkCache g = service_cache_map.get(managers[i]);
        if (g != null) {
          picked = g;
          ++picked_count;
        }
      }
      if (picked == null) {
        picked = createDefaultCacheFor(managers, cache_config);
        for (int i = 0; i < managers.length; ++i) {
          service_cache_map.put(managers[i], picked);
        }
      }
      else if (picked_count != managers.length) {
        for (int i = 0; i < managers.length; ++i) {
          service_cache_map.put(managers[i], picked);
        }
      }
      return picked;
    }
  }

  /**
   * Sets a LocalNetworkCache object to be used for networks that have the
   * given managers ServiceAddress. This function can be used to customize the
   * cache settings (for example, to assign a different amount of memory for
   * the cache, or use a different method for accessing cached data).
   */
  public static void setJVMCacheForManager(
                            ServiceAddress[] managers, LocalNetworkCache lnc) {
    if (managers == null) {
      throw new NullPointerException();
    }
    synchronized (service_cache_map) {
      for (int i = 0; i < managers.length; ++i) {
        service_cache_map.put(managers[i], lnc);
      }
    }
  }

}
