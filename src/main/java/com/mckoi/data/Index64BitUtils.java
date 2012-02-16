/**
 * com.mckoi.treestore.BTreeUtils  08 Sep 2004
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

package com.mckoi.data;

import com.mckoi.data.DataFileUtils.EmptyDataFile;

/**
 * Various Index64Bit static utility functions.
 * 
 * @author Tobias Downer
 */

public class Index64BitUtils {

  /**
   * Performs a pointer intersection of the first index with the second index
   * and subtracts any values not in the second index from the first index.
   * Assumes that both 'index1' and 'index2' are sorted by their key
   * values.  When the method returns, 'index1' will be less the pointers that
   * are not in 'index2'.  'index1' may be modified by this function, and 'index2'
   * will never be modified.
   */
  public static void pointerIntersection(Index64Bit index1, Index64Bit index2) {
    // We maintain iterators for both index1 and index2.
    Iterator64Bit i1 = index1.iterator();
    Iterator64Bit i2 = index2.iterator();

    if (i1.hasNext() && i2.hasNext()) {
    
      long v1 = i1.next();
      long v2 = i2.next();

      while (true) {
      
        // If they are both the same
        if (v1 == v2) {
          // Iterate both
          if (!i1.hasNext() || !i2.hasNext()) {
            break;
          }
          v1 = i1.next();
          v2 = i2.next();
        }
        // If v1 is less than v2
        else if (v1 < v2) {
          i1.remove();
          if (!i1.hasNext()) {
            break;
          }
          v1 = i1.next();
        }
        // If v1 is greater than v2
        else if (v1 > v2) {
          if (!i2.hasNext()) {
            // If there are no more in i2, we remove this value from i1 because
            // it's not equal to the last value
            i1.remove();
            break;
          }
          v2 = i2.next();
        }
      }

    }

    // Remove any remaining from i1
    while (i1.hasNext()) {
      i1.next();
      i1.remove();
    }

  }

  /**
   * Returns an immutable Index64Bit object that is empty.
   */
  public static Index64Bit emptyImmutableIndex() {
    return EMPTY_INDEX;
  }
  
  /**
   * Returns an immutable Index64Bit object containing a single value.
   */
  public static Index64Bit singleImmutableIndex(long val) {
    DataFile df = new LongArrayBackedDataFile(new long[] { val } );
    return new OrderedList64Bit(df, true);
  }

  /**
   * Returns a mutable Index64Bit object backed by an array on the heap that
   * can store a maximum of n values.  Attempting to store more than n values
   * in the structure will generate an error.
   * <p>
   * The returned Index64Bit uses 'System.arraycopy' to implement the shift
   * method when inserting data into the list, making it inappropriate for use
   * in large sets.
   */
  public static Index64Bit arrayBackedIndex(int max_size) {
    DataFile df = new LongArrayBackedDataFile(max_size);
    return new OrderedList64Bit(df, false);
  }

  /**
   * Immutable empty static index.
   */
  private static final OrderedList64Bit EMPTY_INDEX;
  static {
    EMPTY_INDEX = new OrderedList64Bit(new EmptyDataFile(), true);
  }

  /**
   * A DataFile backed by a long[] array tailored specifically for storing
   * values maintained by an OrderedSet64Bit object on the heap.  This does
   * not, in any way, represent a complete implementation of a DataFile
   * backed by a long[] array, however, it implements enough to meet the
   * requirements to back an OrderedSet64Bit object.
   */
  private static class LongArrayBackedDataFile extends EmptyDataFile {
    private long[] arr;
    private int count;
    private int pos;
    
    public LongArrayBackedDataFile(long[] array) {
      this.arr = array;
      count = array.length;
      pos = 0;
    }
    public LongArrayBackedDataFile(int array_size) {
      this.arr = new long[array_size];
      count = 0;
      pos = 0;
    }

    public long size() {
      return count * 8;
    }
    public void position(long position) {
      pos = (int) (position / 8);
    }
    public long position() {
      return pos;
    }

    public long getLong() {
      if (pos < 0 || pos >= count) {
        throw new RuntimeException("Position out of bounds");
      }
      
      long v = arr[pos];
      ++pos;
      return v;
    }

    public void setSize(long size) {
      int sz = (int) (size / 8);
      if (sz < 0) {
        throw new RuntimeException("size < 0");
      }
      if (sz > arr.length) {
        throw new RuntimeException("New size exceeds backed array size.");
      }
      count = sz;
    }
    
    public void delete() {
      setSize(0);
    }

    public void shift(long offset) {
      int dif = (int) (offset / 8);
      int fin_p = count + dif;
      if (fin_p < 0 || fin_p > arr.length) {
        throw new RuntimeException("Shift out of bounds");
      }
      if (pos > count) {
        throw new RuntimeException("Position out of bounds");
      }
      if (pos < count) {
        if (dif > 0) {
          System.arraycopy(arr, pos, arr, pos + dif, count - pos);
        }
        else if (dif < 0) {
          System.arraycopy(arr, pos + dif, arr, pos, count - (pos + dif));
        }
      }
      count += dif;
    }

    public void putLong(long l) {
      if (pos == count) {
        if (pos >= arr.length) {
          throw new RuntimeException("New size exceeds backed array size.");
        }
        ++count;
      }
      arr[pos] = l;
      ++pos;
    }

  }

}
