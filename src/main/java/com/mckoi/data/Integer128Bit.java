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

/**
 * A simple 128 bit value represented as 2 long values.
 *
 * @author Tobias Downer
 */

public class Integer128Bit implements Comparable<Integer128Bit> {

  /**
   * The value.
   */
  protected final long[] ref;

  /**
   * Constructor.
   */
  public Integer128Bit(long[] ref) {
    this.ref = ref.clone();
  }

  /**
   * Constructor.
   */
  public Integer128Bit(long high, long low) {
    this.ref = new long[2];
    ref[0] = high;
    ref[1] = low;
  }

  /**
   * Returns the high 64-bit part of this node reference.
   */
  public long getHighLong() {
    return ref[0];
  }

  /**
   * Returns the low 64-bit part of this node reference.
   */
  public long getLowLong() {
    return ref[1];
  }

  // ----- Utility -----

  @Override
  public int hashCode() {
    return (int) (ref[1] & 0x07FFFFFFFL);
  }

  @Override
  public boolean equals(Object ob) {
    if (ob == this) {
      return true;
    }
    if (!(ob instanceof Integer128Bit)) {
      return false;
    }
    Integer128Bit dest_ref = (Integer128Bit) ob;
    return (ref[1] == dest_ref.ref[1] &&
            ref[0] == dest_ref.ref[0]);
  }

  /**
   * This is a signed 128-bit comparison.
   */
  @Override
  public int compareTo(Integer128Bit that) {
    long thish = this.ref[0];
    long thath = that.ref[0];

    if (thish < thath) {
      return -1;
    }
    else if (thish > thath) {
      return 1;
    }
    else {
      // High 64-bits are equal, so compare low,
      long thisl = this.ref[1];
      long thatl = that.ref[1];

      // This comparison needs to be unsigned,
      // True if the signs are different
      boolean signdif = (thisl < 0) != (thatl < 0);

      if ((thisl < thatl) ^ signdif) {
        return -1;
      }
      else if ((thisl > thatl) ^ signdif) {
        return 1;
      }
      else {
        // Equal,
        return 0;
      }
    }
  }

  /**
   * The 'toString' method outputs the value as [high].[low] in hex format.
   */
  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append(Long.toHexString(ref[0]));
    b.append(".");
    b.append(Long.toHexString(ref[1]));
    return b.toString();
  }


  // ----- Tests -----
  // PENDING: Move this into test suite

  private static void spec(int x, int y) {
    System.out.println("spec(" + x + ", " + y + ")");
    if (x != y) {
      throw new RuntimeException("Test Failed");
    }
  }

  public static void main(String[] args) {

    spec(new Integer128Bit(new long[] { 0, 0 }).compareTo(
         new Integer128Bit(new long[] { 0, 0 })), 0);

    spec(new Integer128Bit(new long[] { 0, 1 }).compareTo(
         new Integer128Bit(new long[] { 0, 0 })), 1);

    spec(new Integer128Bit(new long[] { 0, 0 }).compareTo(
         new Integer128Bit(new long[] { 0, 900 })), -1);

    spec(new Integer128Bit(new long[] { 1, 0 }).compareTo(
         new Integer128Bit(new long[] { 0, 0 })), 1);

    spec(new Integer128Bit(new long[] { 1, 0 }).compareTo(
         new Integer128Bit(new long[] { 900, 0 })), -1);

    spec(new Integer128Bit(new long[] { 1, Long.MAX_VALUE }).compareTo(
         new Integer128Bit(new long[] { 0, 0 })), 1);

    spec(new Integer128Bit(new long[] { 0, 0 }).compareTo(
         new Integer128Bit(new long[] { 0, Long.MAX_VALUE })), -1);

    spec(new Integer128Bit(new long[] { 0, 0 }).compareTo(
         new Integer128Bit(new long[] { 0, Long.MAX_VALUE + 1 })), -1);

    spec(new Integer128Bit(new long[] { 0, Long.MAX_VALUE }).compareTo(
         new Integer128Bit(new long[] { 0, 0 })), 1);

    spec(new Integer128Bit(new long[] { 0, Long.MAX_VALUE + 1 }).compareTo(
         new Integer128Bit(new long[] { 0, 0 })), 1);

    spec(new Integer128Bit(new long[] { 0, Long.MAX_VALUE }).compareTo(
         new Integer128Bit(new long[] { 0, Long.MAX_VALUE + 1 })), -1);

    spec(new Integer128Bit(new long[] { 0, Long.MAX_VALUE + 1 }).compareTo(
         new Integer128Bit(new long[] { 0, Long.MAX_VALUE })), 1);

    spec(new Integer128Bit(new long[] { 0, Long.MAX_VALUE }).compareTo(
         new Integer128Bit(new long[] { 0, Long.MAX_VALUE })), 0);

    spec(new Integer128Bit(new long[] { 0, Long.MAX_VALUE + 1 }).compareTo(
         new Integer128Bit(new long[] { 0, Long.MAX_VALUE + 1 })), 0);

    spec(new Integer128Bit(new long[] { Long.MAX_VALUE, 0 }).compareTo(
         new Integer128Bit(new long[] { Long.MAX_VALUE + 1, 0 })), 1);

    spec(new Integer128Bit(new long[] { 0, 0x0FFFFFFFFFFFFFFFFL }).compareTo(
         new Integer128Bit(new long[] { 0, 0x0FFFFFFFFFFFFFFFEL })), 1);

    // This is -1 vs -2
    spec(new Integer128Bit(new long[] { -1, 0x0FFFFFFFFFFFFFFFFL }).compareTo(
         new Integer128Bit(new long[] { -1, 0x0FFFFFFFFFFFFFFFEL })), 1);

    spec(new Integer128Bit(new long[] { 1, 0 }).compareTo(
         new Integer128Bit(new long[] { 1, 100 })), -1);
  }

}
