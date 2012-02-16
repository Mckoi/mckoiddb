/**
 * com.mckoi.data.ByteArray  Aug 4, 2010
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

package com.mckoi.data;

import java.io.DataInputStream;

/**
 * An immutable wrapper for a sequence of bytes that also has an order
 * specification.
 *
 * @author Tobias Downer
 */

public interface ByteArray extends Comparable<ByteArray> {

  /**
   * Returns the size of the byte array.
   */
  int length();

  /**
   * Returns the byte at position p.
   */
  byte getByteAt(int p);

  /**
   * Returns a DataInputStream for reading the content of the array from the
   * beginning.
   */
  DataInputStream getDataInputStream();

  /**
   * Compares this byte array to the given byte array. The default
   * implementation of this should be a lexicographic comparison that
   * is implemented as follows; Start at byte index 0. If the byte in
   * this array is greater than the byte in 'dest' then return 1. If it's
   * less then return -1. If it's the same, repeat the text for the next byte.
   * If there are no more bytes left to read in one of the arrays, return
   * the length of this array minus the length of the array in 'dest'.
   * <p>
   * Note that custom implementations of this interface may choose any kind
   * of comparison function that is desired.
   */
  @Override
  int compareTo(ByteArray dest);

  /**
   * Equality check for this byte array. The default implementation of this
   * should be a lexicographic equality test. 'o1.equals(o2) == true' is
   * the same as 'o1.compareTo(o2) == 0'. See the 'compareTo' method
   * description for details of this test.
   * <p>
   * Note that custom implementations of this interface may choose any kind
   * of comparison function that is desired.
   */
  @Override
  boolean equals(Object dest);

}
