/**
 * com.mckoi.odb.KeyAllocation  Jan 1, 2011
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

package com.mckoi.odb;

import com.mckoi.data.Key;

/**
 * An object that represents a Key that has been allocated to a Reference.
 *
 * @author Tobias Downer
 */

class KeyAllocation {

  private Key key;
  private Reference ref;

  public KeyAllocation(Key key, Reference ref) {
    this.key = key;
    this.ref = ref;
  }

  public Key getKey() {
    return key;
  }

  public Reference getRef() {
    return ref;
  }

  @Override
  public String toString() {
    return key.toString() + "->" + ref.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final KeyAllocation other = (KeyAllocation) obj;
    if (this.key != other.key && (this.key == null || !this.key.equals(other.key))) {
      return false;
    }
    if (this.ref != other.ref && (this.ref == null || !this.ref.equals(other.ref))) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 31 * hash + (this.key != null ? this.key.hashCode() : 0);
    hash = 31 * hash + (this.ref != null ? this.ref.hashCode() : 0);
    return hash;
  }

}
