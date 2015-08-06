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

import com.mckoi.util.Cache;
import com.mckoi.util.ReferenceCache;

/**
 * An implementation of GeneralCache that uses a weak hash map to store the
 * keys/values.
 *
 * @author Tobias Downer
 */

public class WeakGeneralCache implements GeneralCache {

  private final ReferenceCache<GeneralCacheKey, Object> cache;
  
  public WeakGeneralCache() {
    cache = new ReferenceCache(Cache.closestPrime(320), ReferenceCache.WEAK);
  }

  @Override
  public void put(GeneralCacheKey key, Object value) {
    cache.put(key, value);
  }

  @Override
  public Object get(GeneralCacheKey key) {
    return cache.get(key);
  }

}
