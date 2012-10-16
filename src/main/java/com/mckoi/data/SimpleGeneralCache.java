/**
 * com.mckoi.data.SimpleGeneralCache  Oct 14, 2012
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

import java.util.HashMap;

/**
 * A very simple implementation of GeneralCache that simply stores the key
 * values in a hash map. This should only be used for testing purposes.
 *
 * @author Tobias Downer
 */

public class SimpleGeneralCache implements GeneralCache {

  private final HashMap<GeneralCacheKey, Object> map;
  
  public SimpleGeneralCache() {
    this.map = new HashMap();
  }

  @Override
  public void put(GeneralCacheKey key, Object value) {
    map.put(key, value);
  }

  @Override
  public Object get(GeneralCacheKey key) {
    return map.get(key);
  }

}
