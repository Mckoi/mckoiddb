/**
 * com.mckoi.data.WeakGeneralCache  Oct 15, 2012
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