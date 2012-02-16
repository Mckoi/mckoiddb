/**
 * com.mckoi.network.BlockId  Jun 12, 2010
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
