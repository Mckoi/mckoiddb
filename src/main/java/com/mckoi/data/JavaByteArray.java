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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

/**
 * A ByteArray implementation backed by a Java byte array. In this
 * implementation the 'compareTo' and 'equals' method use a lexicographic
 * comparison.
 *
 * @author Tobias Downer
 */

public class JavaByteArray implements ByteArray {

  private final byte[] buf;
  private final int offset;
  private final int len;

  /**
   * Constructs a JavaByteArray over a section of the given array. Note that
   * the array is not copied in this constructor so care should be taken not
   * to modify the array if it's not desirable that the content of this
   * object is changed.
   */
  public JavaByteArray(byte[] buf, int offset, int len) {
    this.buf = buf;
    this.offset = offset;
    this.len = len;
  }

  /**
   * Constructs a JavaByteArray over the given array. Note that the array is
   * not copied in this constructor so care should be taken not to modify the
   * array if it's not desirable that the content of this object is changed.
   */
  public JavaByteArray(byte[] buf) {
    this(buf, 0, buf.length);
  }

  public byte getByteAt(int p) {
    return buf[p + offset];
  }

  public int length() {
    return len;
  }

  public DataInputStream getDataInputStream() {
    return new DataInputStream(new ByteArrayInputStream(buf, offset, len));
  }

  /**
   * Compares this byte array to the given byte array. This implementation
   * is a lexicographic comparison that is implemented as follows; Start at
   * byte index 0. If the byte in
   * this array is greater than the byte in 'dest' then return 1. If it's
   * less then return -1. If it's the same, repeat the test for the next byte.
   * If there are no more bytes left to read in one of the arrays, return
   * the length of this array minus the length of the array in 'dest'.
   */
  public int compareTo(ByteArray o) {
    int len1 = length();
    int len2 = o.length();
    int clen = Math.min(len1, len2);
    for (int i = 0; i < clen; ++i) {
      byte v1 = getByteAt(i);
      byte v2 = o.getByteAt(i);
      if (v1 != v2) {
        if (v1 > v2) {
          return 1;
        }
        else {
          return -1;
        }
      }
    }
    return len1 - len2;
  }

  /**
   * See the 'compareTo' method for a description of the method used to
   * check equality.
   */
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ByteArray)) {
      return false;
    }
    ByteArray bao = (ByteArray) o;
    if (length() != bao.length()) {
      return false;
    }
    int sz = length();
    for (int i = 0; i < sz; ++i) {
      if (getByteAt(i) != bao.getByteAt(i)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns a string representation of this byte array as a list of byte
   * values.
   */
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("[");
    for (int i = 0; i < len; ++i) {
      b.append(Byte.toString(getByteAt(i)));
      b.append(" ");
    }
    b.append("]");
    return b.toString();
  }

}
