/**
 * com.mckoi.treestore.IndexObjectCollator  07 Aug 2003
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
 * A comparator used in a search on an index that compares an Object with a
 * 64-bit index reference and determines if the Object value is larger, smaller
 * or the same as the referenced value. This typically is used as a device to
 * dereference a 64-bit key value into a collatable object in another
 * structure to build and query an index.
 *
 * @author Tobias Downer
 */

public interface IndexObjectCollator {

  /**
   * Compares a value at some specified reference and an Object value, and
   * returns a value &gt; 0 if the referenced value is greater, &lt; 0 if the
   * referenced value is less than, or == 0 if the referenced value is the same.
   */
  int compare(long ref, Object val);

}

