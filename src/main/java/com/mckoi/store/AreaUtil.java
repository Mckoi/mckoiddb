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

