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

package com.mckoi.network;

import com.mckoi.data.Integer128Bit;

/**
 * A block ID is an addressable entity that is represented as an 108-bit value.
 *
 * @author Tobias Downer
 */

public final class BlockId extends Integer128Bit {

  public BlockId(long high_v, long low_v) {
    super(high_v, low_v);
  }


  /**
   * Creates and returns a referencible address (the value shifted by 16 bits
   * to the left).
   */
  public long[] getReferenceAddress() {
    long[] out = new long[2];
    out[0] = (getHighLong() << 16) | ((getLowLong() >> 48) & 0x0FFFF);
    out[1] = getLowLong() << 16;
    return out;
  }

  /**
   * Adds a positive integer amount to this block id and returns the new
   * value as a BlockId.
   */
  public BlockId add(int positive_val) {
    if (positive_val < 0) {
      throw new IllegalArgumentException("positive_val < 0");
    }
    long low = getLowLong() + positive_val;
    long high = getHighLong();
    // If the new low value is positive, and the old value was negative,
    if (low >= 0 && getLowLong() < 0) {
      // We overflowed, so add 1 to the high val,
      ++high;
    }
    return new BlockId(high, low);
  }

}
