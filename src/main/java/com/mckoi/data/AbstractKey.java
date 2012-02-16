/**
 * com.mckoi.data.AbstractKey  Nov 2, 2008
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

/**
 * An abstract 14 byte key object.
 *
 * @author Tobias Downer
 */

public class AbstractKey implements Comparable<AbstractKey> {

  /**
   * This short represents the key type as defined by the application layer.
   */
  private short type;

  /**
   * The secondary component of the key is a 32-bit value.
   */
  private int secondary_key;

  /**
   * The primary component of the key is a 64-bit value.
   */
  private long primary_key;

  /**
   * Constructs the key with a key type (16 bits), a secondary key value
   * (32 bits), and a primary key value (64 bits).
   */
  public AbstractKey(short type, int secondary_key, long primary_key) {
    this.type = type;
    this.secondary_key = secondary_key;
    this.primary_key = primary_key;
  }

  AbstractKey(long encoded_v1, long encoded_v2) {
    this.type = (short) (encoded_v1 >> 32);
    this.secondary_key = (int) (encoded_v1 & 0x0FFFFFFFF);
    this.primary_key = encoded_v2;
  }

  /**
   * Returns the type of the key (16 bits).
   */
  public short getType() {
    return type;
  }

  /**
   * Returns the secondary component of the key (32 bits).
   */
  public int getSecondary() {
    return secondary_key;
  }

  /**
   * Returns the primary component of the key (64 bits).
   */
  public long getPrimary() {
    return primary_key;
  }

  /**
   * Returns the encoded value for the nth part.
   */
  public long encodedValue(int n) {
    if (n == 1) {
      long v = (((long) type) & 0x0FFFFFFFFL) << 32;
      v |= (((long) secondary_key) & 0x0FFFFFFFFL);
      return v;
    }
    else if (n == 2) {
      return primary_key;
    }
    else {
      throw new RuntimeException("n is not valid.");
    }
  }

  /**
   * Compares this key with another key.  Returns a positive number if this
   * key is greater than the given key, a negative number if this key is less
   * than the given key, and 0 if the keys are equal.
   */
  public int compareTo(AbstractKey ob) {
    if (this == ob) {
      return 0;
    }
    AbstractKey key = (AbstractKey) ob;

    // Either this key or the compared key are not special case, so collate
    // on the key values,

    // Compare secondary keys
    int c = secondary_key < key.secondary_key ? -1
                   : (secondary_key == key.secondary_key ? 0
                                                         : 1);
    if (c == 0) {
      // Compare types
      c = type < key.type ? -1
                          : (type == key.type ? 0
                                              : 1);
      if (c == 0) {
        // Compare primary keys
        if (primary_key > key.primary_key) {
          return +1;
        }
        else if (primary_key < key.primary_key) {
          return -1;
        }
        return 0;
      }
    }
    return c;
  }

  /**
   * Returns true if the given key matches this key.
   */
  public boolean equals(Object ob) {
    if (this == ob) {
      return true;
    }
    AbstractKey dest_key = (AbstractKey) ob;
    return dest_key.type == type &&
           dest_key.secondary_key == secondary_key &&
           dest_key.primary_key == primary_key;
  }

  /**
   * Returns a hash code of the key.
   */
  public int hashCode() {
    int c = (int) ((secondary_key << 6) + (type << 3) + primary_key);
    return c;
  }

}
