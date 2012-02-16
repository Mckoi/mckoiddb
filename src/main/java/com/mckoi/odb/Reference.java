/**
 * com.mckoi.sdb.Reference  Aug 2, 2010
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
