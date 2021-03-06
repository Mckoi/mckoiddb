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

package com.mckoi.util;

import java.util.Arrays;

/**
 * Management of a simple array of 64 bit values stored in an array.
 * 
 * @author Tobias Downer
 */

public class LongList {

  /**
   * The long array.
   */
  private long[] arr;
  
  /**
   * The number of elements in the list.
   */
  private int count;
  
  /**
   * Constructors.
   */
  public LongList(int size) {
    arr = new long[size];
    count = 0;
  }
  
  public LongList() {
    this(32);
  }

  // Copy constructor
  private LongList(LongList to_copy) {
    count = to_copy.count;
    arr = new long[count];
    System.arraycopy(to_copy.arr, 0, arr, 0, count);
  }


  /**
   * Ensure there is capacity to hold 'n' more elements in the list.
   */
  private void ensureCapacityForAdditions(int n) {
    int intended_size = count + n;
    if (intended_size > arr.length) {
      long[] old_arr = arr;

      int grow_size = old_arr.length + 1;
      // Put a cap on the new size.
      if (grow_size > 64000) {
        grow_size = 64000;
      }

      int new_size = Math.max(old_arr.length + grow_size, intended_size);
      arr = new long[new_size];
      System.arraycopy(old_arr, 0, arr, 0, count);
    }
  }

  /**
   * Returns the number of values stored in the list.
   */
  public int size() {
    return count;
  }

  /**
   * Clears the list of all elements;
   */
  public void clear() {
    count = 0;
  }
  
  /**
   * Returns the value at position n.
   */
  public long get(int n) {
    return arr[n];
  }
  
  /**
   * Removes the value at position n from the list and shifts all the values
   * after it into the space.
   */
  public void remove(int n) {
    if (count > 0) {
      count = count - 1;
      if (n < count) {
        System.arraycopy(arr, n + 1, arr, n, 1);
      }
    }
  }
  
  /**
   * Adds a new value to the end of the list.
   */
  public void add(long v) {
    ensureCapacityForAdditions(1);
    arr[count] = v;
    ++count;
  }
  
  /**
   * Makes an exact copy of this list.
   */
  public LongList copy() {
    return new LongList(this);
  }

  /**
   * Sorts the values in the list.
   */
  public void sort() {
    Arrays.sort(arr, 0, count);
  }

  /**
   * Uses a binary search algorithm to discover the position of the given
   * value in the list, or if the value isn't present then -(pos + 1) where
   * pos is the position to insert the value in the list to maintain sorted
   * order.  Assumes the list is sorted.
   */
  public int binarySearch(long v) {
    int low = 0;
    int high = count - 1;

    while (low <= high) {
      int mid = (low + high) >> 1;
      long mid_val = arr[mid];

      if (mid_val < v) {
        low = mid + 1;
      }
      else if (mid_val > v) {
        high = mid - 1;
      }
      else {
        return mid;
      }

    }
    return -(low + 1);
  }

  /**
   * Returns a new LongList that contains all the values that are in this
   * list but that are not in the given list.  Assumes that list 'd' is sorted.
   */
  public LongList notIn(LongList d) {
    // When no entries in this list, return an empty list
    if (size() == 0) {
      return new LongList(0);
    }

    LongList result = new LongList();
    for (int i = 0; i < size(); ++i) {
      // If list 'd' doesn't contain the value then add it to the result list.
      long v = get(i);
      if (d.binarySearch(v) < 0) {
        result.add(v);
      }
    }

    // Return the result list
    return result;
  }

  public String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append("[ ");
    for (int i = 0; i < size(); ++i) {
      buf.append(get(i));
      buf.append(", ");
    }
    buf.append("]");
    return new String(buf);
  }
  
}


