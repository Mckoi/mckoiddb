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
