/**
 * com.mckoi.network.CacheConfiguration  Nov 4, 2011
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

/**
 * An object that provides a number of configuration settings for that caches
 * managed by a JVMState.
 *
 * @author Tobias Downer
 */

public class CacheConfiguration {

  /**
   * The global node cache size.
   */
  private long global_node_cache_size = 32 * 1024 * 1024;

  /**
   * Sets the maximum size (in bytes) of the global node cache within this JVM.
   */
  public void setGlobalNodeCacheSize(long cache_size) {
    this.global_node_cache_size = cache_size;
  }

  /**
   * Returns the maximum size of the global node cache within this JVM in
   * bytes.
   */
  public long getGlobalNodeCacheSize() {
    return global_node_cache_size;
  }

}
