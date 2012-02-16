/**
 * com.mckoi.store.AreaUtil  21 Aug 2003
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

package com.mckoi.store;

import java.io.IOException;

/**
 * Various convenience methods to be used with Area and AreaWriter objects.
 *
 * @author Tobias Downer
 */

public class AreaUtil {

  // ----- Size -----
  
  /**
   * Returns the number of bytes required to be written out for the given
   * object.
   */
  public static int sizeOf(String str) {
    if (str == null) {
      return 1;
    }
    else {
      return 5 + (2 * str.length());
    }
  }
  
  /**
   * Returns the number of bytes required to be written out for the given
   * object.
   */
  public static int sizeOf(int[] arr) {
    if (arr == null) {
      return 1;
    }
    else {
      return 5 + (4 * arr.length);
    }
  }

  // ----- Writers -----

  /**
   * Writes a string out to an AreaWriter object, and handles 'null' strings.
   */
  public static void writeString(AreaWriter a, String str) throws IOException {
    if (str == null) {
      a.put((byte) 1);
    }
    else {
      a.put((byte) 0);
      int sz = str.length();
      a.putInt(sz);
      for (int i = 0; i < sz; ++i) {
        a.putChar(str.charAt(i));
      }
    }
  }

  /**
   * Writes an int array out to an AreaWriter object, and handles 'null'
   * arrays.
   */
  public static void writeIntArray(AreaWriter a, int[] arr) throws IOException {
    if (arr == null) {
      a.put((byte) 1);
    }
    else {
      a.put((byte) 0);
      a.putInt(arr.length);
      for (int i = 0; i < arr.length; ++i) {
        a.putInt(arr[i]);
      }
    }
  }

  // ----- Readers -----
  
  /**
   * Reads a string from the Area object at the current position that has
   * previously been written using the 'writeString' method.
   */
  public static String readString(Area a) throws IOException {
    byte b = a.get();
    if (b == 1) {
      return null;
    }
    else {
      int len = a.getInt();
      StringBuffer buf = new StringBuffer(len);
      for (int i = 0; i < len; ++i) {
        buf.append(a.getChar());
      }
      return new String(buf);
    }
  }

  /**
   * Reads an int array from the Area object at the current position that has
   * previously been written using the 'writeIntArray' method.
   */
  public static int[] readIntArray(Area a) throws IOException {
    byte b = a.get();
    if (b == 1) {
      return null;
    }
    else {
      int len = a.getInt();
      int[] arr = new int[len];
      for (int i = 0; i < len; ++i) {
        arr[i] = a.getInt();
      }
      return arr;
    }
  }

}

