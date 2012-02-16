/**
 * com.mckoi.util.IndexComparator  01 Jul 2000
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

package com.mckoi.util;

/**
 * A comparator that is used within BlockIntegerList that compares two int
 * values which are indices to data that is being compared.  For example, we
 * may have an BlockIntegerList that contains indices to cells in the column
 * of a table.  To make a sorted list, we use this comparator to lookup the
 * index values in the list for sorting and searching.
 *
 * @author Tobias Downer
 */

public interface IndexComparator {

  /**
   * Returns > 0 if the value pointed to by index1 is greater than 'val',
   * or &lt; 0 if the value pointed to by index 1 is less than 'val'.  If the
   * indexed value is equal to 'val', it returns 0.
   */
  int compare(int index1, Object val);

  /**
   * Returns >0 if the value pointed to by index1 is greater than the value
   * pointed to by index2, or &tl; 0 if the value pointed to by index 1 is less
   * than the value pointed to by index 2.  If the indexed value's are equal,
   * it returns 0.
   */
  int compare(int index1, int index2);

}
