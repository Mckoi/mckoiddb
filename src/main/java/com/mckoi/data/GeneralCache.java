/**
 * com.mckoi.data.GeneralCache  Oct 14, 2012
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
 * A general cache for Java objects in a MckoiDDB implementation. This cache
 * may contain any sorts of key and value objects. There is no guarantee on
 * the length of time an item will stay in the cache. The cache may be wiped
 * often due to memory constraints.
 * <p>
 * The objective of this cache is to provide a temporary store for objects that
 * may have a high cost to make, and that are accessed frequently. Since this
 * cache may store all sorts of different types of items, it is important that
 * a key contains enough information to globally distinguish itself.
 * <p>
 * Implementations of this do not have to ensure that an item put into the
 * cache in one thread will be accessible in another thread.
 *
 * @author Tobias Downer
 */

public interface GeneralCache {

  /**
   * Puts an item in the cache. The key must provide a good hash code.
   */
  void put(GeneralCacheKey key, Object value);

  /**
   * Gets an item from the cache. Returns null if the item is not present in
   * the cache.
   */
  Object get(GeneralCacheKey key);

}
