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
