/**
 * com.mckoi.odb.ReferenceCacheKey  Oct 14, 2012
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

package com.mckoi.odb;

import com.mckoi.data.GeneralCacheKey;

/**
 * A GeneralCacheKey implementation based on a Reference.
 *
 * @author Tobias Downer
 */

public class ReferenceCacheKey implements GeneralCacheKey {

  private final int type_code;
  private final Reference reference;

  ReferenceCacheKey(int type_code, Reference reference) {
    if (reference == null) throw new NullPointerException();
    this.type_code = type_code;
    this.reference = reference;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final ReferenceCacheKey other = (ReferenceCacheKey) obj;
    return (this.type_code == other.type_code &&
            this.reference.equals(other.reference));
  }

  @Override
  public int hashCode() {
    return reference.hashCode() ^ type_code;
  }

}
