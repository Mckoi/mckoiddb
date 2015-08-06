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
