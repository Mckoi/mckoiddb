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
 * A Key is a reference in an address space that destinguishes one data file
 * from another within a context. A key is an identity to some stored
 * information.
 * <p>
 * All keys have a type, a secondary and primary component.  In combination,
 * the key is 14 bytes of information in total.
 * <p>
 * Keys with a type value of 0x07F80 are special case keys used for system
 * specific information and should not be used by user structures.
 * 
 * @author Tobias Downer
 */
public final class Key extends AbstractKey {

  /**
   * Special case static key that always appears at the head of the tree.
   */
  public static final Key HEAD_KEY = new Key((short) 0x07F80, -2, -1);

  /**
   * Special case static key that always appears at the tail of the tree.
   */
  public static final Key TAIL_KEY = new Key((short) 0x07F80, -1, -1);

  /**
   * Constructs the key with a key type (16 bits), a secondary key value
   * (32 bits), and a primary key value (64 bits).
   */
  public Key(short type, int secondary_key, long primary_key) {
    super(type, secondary_key, primary_key);
  }

  Key(long encoded_v1, long encoded_v2) {
    super(encoded_v1, encoded_v2);
  }

  /**
   * Compares this key with another key.  Returns a positive number if this
   * key is greater than the given key, a negative number if this key is less
   * than the given key, and 0 if the keys are equal.
   */
  @Override
  public int compareTo(AbstractKey ob) {
    if (this == ob) {
      return 0;
    }
    Key key = (Key) ob;
    // Handle the special case head and tail keys,
    if (getType() == 0x07F80) {
      // This is special case,
      if (equals(HEAD_KEY)) {
        // This is less than any other key except head which it is equal to
        if (key.equals(HEAD_KEY)) {
          return 0;
        }
        else {
          return -1;
        }
      }
      // Must be a tail key,
      else if (equals(TAIL_KEY)) {
        // This is greater than any other key except tail which it is equal to
        if (key.equals(TAIL_KEY)) {
          return 0;
        }
        else {
          return 1;
        }
      }
      else {
        throw new RuntimeException("Unknown special case key");
      }
    }
    else if (key.getType() == 0x07F80) {
      // This is special case,
      if (key.equals(HEAD_KEY)) {
        // Every key is greater than head except head which it is equal to
        if (equals(HEAD_KEY)) {
          return 0;
        }
        else {
          return 1;
        }
      }
      // Must be a tail key,
      else if (key.equals(TAIL_KEY)) {
        // Every key is less than tail except tail which it is equal to
        if (equals(TAIL_KEY)) {
          return 0;
        }
        else {
          return -1;
        }
      }
      else {
        throw new RuntimeException("Unknown special case key");
      }
    }

    // Either this key or the compared key are not special case, so collate
    // on the key values,
    
    return super.compareTo(ob);
  }

  /**
   * Returns a string representation of the key.
   */
  public String toString() {
    if (equals(HEAD_KEY)) {
      return "HEAD";
    }
    else if (equals(TAIL_KEY)) {
      return "TAIL";
    }
    else {
      StringBuffer buf = new StringBuffer();
      buf.append("(");
      buf.append(getSecondary());
      buf.append("-");
      buf.append(getType());
      buf.append("-");
      buf.append(getPrimary());
      buf.append(")");
      return buf.toString();
    }
  }
  
}
