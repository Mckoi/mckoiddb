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

package com.mckoi.odb;

import com.mckoi.data.Integer128Bit;

/**
 * An 128-bit reference in the Object Database API.
 *
 * @author Tobias Downer
 */

public final class Reference extends Integer128Bit {

  /**
   * Constructor.
   */
  public Reference(long high, long low) {
    super(high, low);
  }

  /**
   * Constructor.
   */
  public Reference(long[] ref) {
    super(ref);
  }

//  /**
//   * Returns the reference type primitive.
//   */
//  public ODBClass getODBClass() {
//    return null;
//  }
//
//  /**
//   * Returns this reference for the ODBReferenced interface.
//   */
//  public Reference getReference() {
//    return this;
//  }


  /**
   * Parses a reference from a string returned by 'toString'.
   */
  public static Reference fromString(String str) {
    String high = str.substring(0, 16);
    String low = str.substring(16);
    long hval = Long.parseLong(high, 16);
    long lval = Long.parseLong(low, 16);
    return new Reference(hval, lval);
  }


  private String toHexString(long v) {
    String vs = Long.toHexString(v);
    StringBuilder b = new StringBuilder(16);
    b.append("0000000000000000".substring(0, 16 - vs.length()));
    b.append(vs);
    return b.toString();
  }

  /**
   * The 'toString' method outputs the value as [high][low] in hex format.
   */
  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append(toHexString(ref[0]));
    b.append(toHexString(ref[1]));
    return b.toString();
  }

}
