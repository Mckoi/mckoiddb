/**
 * com.mckoi.treestore.OrderedList64Bit  01 Nov 2008
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

/**
 * An ordered list of 64 bit values mapped over a DataFile object.  When list
 * modifications happen, the change is immediately mapped into into the
 * underlying data file. This object grows and shrinks the size of the
 * underlying data file as values are inserted and removed from the list.
 * <p>
 * This class is an implementation of Index64Bit.
 * <p>
 * This class offers a practical way of representing a sorted index of
 * objects within a database.  The collation of the list items can be
 * defined via the IndexObjectCollator interface, or the order may be defined
 * as a function of the index value itself, as per the Index64Bit contract.
 * <p>
 * This list object supports containing duplicate values.
 * 
 * @author Tobias Downer
 */

public class OrderedList64Bit implements Index64Bit {

  /**
   * A static IndexObjectCollator that compares by the key value itself,
   * enabling the ability to create a sorted list of key values.
   */
  public static final IndexObjectCollator KEY_COMPARATOR;

  static {
    KEY_COMPARATOR = new IndexObjectCollator() {
      public int compare(long ref, Object val) {
        long d_ref = ((Long) val).longValue();        
        if (ref > d_ref) {
          return 1;
        }
        else if (ref < d_ref) {
          return -1;
        }
        else {
          return 0;
        }
      }
    };
  }

  // ----- Members -----

  /**
   * The DataFile object that maps to the set.
   */
  private DataFile data;

  /**
   * True if the object is based on an immutable source.
   */
  private final boolean immutable_source;

  /**
   * Constructs the list wrapped to the given DataFile.  If immutable_source is
   * true, the object will fail if any mutation is attempted on the list.
   */
  public OrderedList64Bit(DataFile data, boolean immutable_source) {
    this.data = data;
    this.immutable_source = immutable_source;
  }

  /**
   * Same as the above constructor, only immutable_source is false by default.
   */
  public OrderedList64Bit(DataFile data) {
    this(data, false);
  }



  /**
   * Searches for the first value in the set that matches the given value.
   */
  private long searchFirst(Object value, IndexObjectCollator c,
                           long low, long high) {

    if (low > high) {
      return -1;
    }

    while (true) {
      // If low is the same as high, we are either at the first value or at
      // the position to insert the value,
      if ((high - low) <= 4) {
        for (long i = low; i <= high; ++i) {
          data.position(i * 8);
          long val = data.getLong();
          int res = c.compare(val, value);
          if (res == 0) {
            return i;
          }
          if (res > 0) {
            return -(i + 1);
          }
        }
        return -(high + 2);
      }

      // The index half way between the low and high point
      long mid = (low + high) >> 1;
      // Reaf the middle value from the data file,
      data.position(mid * 8);
      long mid_val = data.getLong();

      // Compare it with the value
      int res = c.compare(mid_val, value);
      if (res < 0) {
        low = mid + 1;
      }
      else if (res > 0) {
        high = mid - 1;
      }
      else {  // if (res == 0)
        high = mid;
      }
    }
  }

  /**
   * Searches for the last value in the set that matches the given value.
   */
  private long searchLast(Object value, IndexObjectCollator c,
                          long low, long high) {

    if (low > high) {
      return -1;
    }

    while (true) {
      // If low is the same as high, we are either at the last value or at
      // the position to insert the value,
      if ((high - low) <= 4) {
        for (long i = high; i >= low; --i) {
          data.position(i * 8);
          long val = data.getLong();
          int res = c.compare(val, value);
          if (res == 0) {
            return i;
          }
          if (res < 0) {
            return -(i + 2);
          }
        }
        return -(low + 1);
      }

      // The index half way between the low and high point
      long mid = (low + high) >> 1;
      // Reaf the middle value from the data file,
      data.position(mid * 8);
      long mid_val = data.getLong();

      // Compare it with the value
      int res = c.compare(mid_val, value);
      if (res < 0) {
        low = mid + 1;
      }
      else if (res > 0) {
        high = mid - 1;
      }
      else {  // if (res == 0)
        low = mid;
      }
    }

  }

  /**
   * Searches for the first and last positions of the given value in the
   * set over the given comparator.
   */
  private void searchFirstAndLast(Object value, IndexObjectCollator c,
                                  long[] result) {

    long low = 0;
    long high = size() - 1;

    if (low > high) {
      result[0] = -1;
      result[1] = -1;
      return;
    }

    while (true) {
      // If low is the same as high, we are either at the first value or at
      // the position to insert the value,
      if ((high - low) <= 4) {
        result[0] = searchFirst(value, c, low, high);
        result[1] = searchLast(value, c, low, high);
        return;
      }

      // The index half way between the low and high point
      long mid = (low + high) >> 1;
      // Reaf the middle value from the data file,
      data.position(mid * 8);
      long mid_val = data.getLong();

      // Compare it with the value
      int res = c.compare(mid_val, value);
      if (res < 0) {
        low = mid + 1;
      }
      else if (res > 0) {
        high = mid - 1;
      }
      else {  // if (res == 0)
        result[0] = searchFirst(value, c, low, high);
        result[1] = searchLast(value, c, low, high);
        return;
      }
    }

  }

//  /**
//   * Returns a version of this ordered set that is immutable and stable in so
//   * much as the content will not change even when there are operations on the
//   * ordered set it is derived from.
//   */
//  public OrderedList64Bit immutableCopy() {
//    try {
//      return new OrderedList64Bit(
//                       data.getImmutableSubset(0, size()), stop_handler, true);
//    }
//    catch (IOException e) {
//      // Handle the error
//      throw stop_handler.errorIO(e);
//    }
//  }

  // ----- Implemented from Index64Bit -----

  /**
   * {@inheritDoc}
   */
  public void clear() {
    // If immutable then generate an exception
    if (immutable_source) {
      throw new RuntimeException("Source is immutable.");
    }
    data.setSize(0);
  }

  /**
   * {@inheritDoc}
   */
  public void clear(long pos, long size) {
    // If immutable then generate an exception
    if (immutable_source) {
      throw new RuntimeException("Source is immutable.");
    }
    if (pos >= 0 && pos + size <= size()) {
      data.position((pos + size) * 8);
      data.shift(-(size * 8));
    }
    else {
      throw new RuntimeException("Clear out of bounds.");
    }
  }

  /**
   * {@inheritDoc}
   */
  public long size() {
    return data.size() / 8;
  }

  /**
   * {@inheritDoc}
   */
  public long searchFirst(Object value, IndexObjectCollator c) {

    long low = 0;
    long high = size() - 1;
  
    return searchFirst(value, c, low, high);
  }

  /**
   * {@inheritDoc}
   */
  public long searchLast(Object value, IndexObjectCollator c) {

    long low = 0;
    long high = size() - 1;

    return searchLast(value, c, low, high);
  }

  /**
   * Returns the 64 bit element at the given position in the index.  This
   * uses the DataFile 'position' function to address the value.
   */
  public long get(long position) {

    data.position(position * 8);
    return data.getLong();

  }

  /**
   * {@inheritDoc}
   */
  public Iterator64Bit iterator(final long start, final long end) {
    // Make sure start and end aren't out of bounds
    if (start < 0 || end > size() || start - 1 > end) {
      throw new ArrayIndexOutOfBoundsException();
    }

    return new OrderedSetIterator(data, start, end,
                                  immutable_source);
  }

  /**
   * {@inheritDoc}
   */
  public Iterator64Bit iterator() {
    return iterator(0, size() - 1);
  }

//  /**
//   * Returns a stable immutable iterator that is guaranteed not to change
//   * regardless of operations on this set.  Note that this may require memeory
//   * on the heap to temporarily store data in the set.  If the underlying
//   * transaction is disposed then the behaviour of the iterator returned here
//   * is undefined.
//   */
//  public Iterator64Bit immutableIterator(final long start, final long end) {
//    // Make sure start and end aren't out of bounds
//    if (start < 0 || end >= size() || start - 1 > end) {
//      throw new ArrayIndexOutOfBoundsException();
//    }
//
//    // If the source is immutable we can use the standard iterator method
//    if (immutable_source) {
//      return iterator(start, end);
//    }
//    else {
//      // Otherwise this isn't an immutable source,
//      try {
//        // The size of the iterator,
//        long size = (end - start);
//        // Return a new ordered set iterator over the immutable subset given
//        return new OrderedSetIterator(
//             data.getImmutableSubset(start, end), stop_handler, 0, size, true);
//      }
//      catch (IOException e) {
//        // Handle the error
//        throw stop_handler.errorIO(e);
//      }
//    }
//  }
//
//  /**
//   * Returns a stable immutable iterator that is guaranteed not to change
//   * regardless of operations on this set.  Note that this may require memeory
//   * on the heap to temporarily store data in the set.  If the underlying
//   * transaction is disposed then the behaviour of the iterator returned here
//   * is undefined.
//   */
//  public Iterator64Bit immutableIterator() {
//    return immutableIterator(0, size());
//  }

  /**
   * {@inheritDoc}
   */
  public void insert(Object value, long ref, IndexObjectCollator c) {
    // If immutable then generate an exception
    if (immutable_source) {
      throw new RuntimeException("Source is immutable.");
    }

    // Search for the position of the last value in the set, 
    long pos = searchLast(value, c);
    // If pos < 0 then the value was not found,
    if (pos < 0) {
      // Correct it to the point where the value must be inserted
      pos = -(pos + 1);
    }
    else {
      // If the value was found in the set, insert after the last value,
      ++pos;
    }

    // Insert the value by moving to the position, shifting the data 8 bytes
    // and writing the long value.
    data.position(pos * 8);
    data.shift(8);
    data.putLong(ref);
  }

  /**
   * {@inheritDoc}
   */
  public boolean insertUnique(Object value, long ref, IndexObjectCollator c) {
    // If immutable then generate an exception
    if (immutable_source) {
      throw new RuntimeException("Source is immutable.");
    }

    // Search for the position of the last value in the set, 
    long pos = searchLast(value, c);
    // If pos < 0 then the value was not found,
    if (pos < 0) {
      // Correct it to the point where the value must be inserted
      pos = -(pos + 1);
    }
    else {
      // If the value was found in the set, return false and don't change the
      // list.
      return false;
    }

    // Insert the value by moving to the position, shifting the data 8 bytes
    // and writing the long value.
    data.position(pos * 8);
    data.shift(8);
    data.putLong(ref);
    // Return true because we changed the list,
    return true;
  }

  /**
   * {@inheritDoc}
   */
  public void remove(Object value, long ref, IndexObjectCollator c) {
    // If immutable then generate an exception
    if (immutable_source) {
      throw new RuntimeException("Source is immutable.");
    }

    // Search for the position of the last value in the set, 
    long[] res = new long[2];
    searchFirstAndLast(value, c, res);
    long p1 = res[0];
    long p2 = res[1];
//    long p1 = searchFirst(value, c);
//    long p2 = searchLast(value, c);
    // If the value isn't found report the error,
    if (p1 < 0) {
      throw new RuntimeException(
                            "Value '" + value + "' was not found in the set.");
    }

    final Iterator64Bit i = iterator(p1, p2);
    while (i.hasNext()) {
      // Does the next value match the ref we are looking for?
      if (i.next() == ref) {
        // Remove the value and return
        i.remove();
        return;
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  public void add(long ref) {
    // If immutable then generate an exception
    if (immutable_source) {
      throw new RuntimeException("Source is immutable.");
    }

    data.position(data.size());
    data.putLong(ref);

  }

  /**
   * {@inheritDoc}
   */
  public void insert(long ref, long pos) {
    // If immutable then generate an exception
    if (immutable_source) {
      throw new RuntimeException("Source is immutable.");
    }
    if (pos >= 0) {
      long sz = size();
      // Shift and insert
      if (pos < sz) {
        data.position(pos * 8);
        data.shift(8);
        data.putLong(ref);
        return;
      }
      // Insert at end
      else if (pos == sz) {
        data.position(sz * 8);
        data.putLong(ref);
        return;
      }
    }
    throw new RuntimeException("Insert out of bounds.");
  }

  /**
   * {@inheritDoc}
   */
  public long remove(long pos) {
    // If immutable then generate an exception
    if (immutable_source) {
      throw new RuntimeException("Source is immutable.");
    }
    if (pos >= 0 && pos < size()) {
      data.position(pos * 8);
      // Read the value then remove it
      long ret_val = data.getLong();
      data.shift(-8);
      // Return the value
      return ret_val;
    }
    else {
      throw new RuntimeException("Remove out of bounds.");
    }
  }

  /**
   * {@inheritDoc}
   */
  public void insertSortKey(long ref) {
    insert(new Long(ref), ref, KEY_COMPARATOR);
  }

  /**
   * {@inheritDoc}
   */
  public void removeSortKey(long ref) {
    remove(new Long(ref), ref, KEY_COMPARATOR);
  }

  /**
   * {@inheritDoc}
   */
  public boolean containsSortKey(long ref) {
    return searchFirst(new Long(ref), KEY_COMPARATOR) >= 0;
  }
  

 
  
  
  // ----- Inner classes -----

  /**
   * An iterator against a range of values in this ordered set.
   */
  private static class OrderedSetIterator implements Iterator64Bit {

    /**
     * The underlying data file.
     */
    private DataFile data;

    /**
     * The start and end position inclusive.
     */
    private long start, end;

    /**
     * True if this iterator is on an immutable source.
     */
    private boolean immutable_source;

    /**
     * The current position.
     */
    private long p;

    /**
     * A value that represents the last position operation.
     */
    private int last_op;

    /**
     * Constructor.
     */
    OrderedSetIterator(DataFile data,
                       long p1, long p2, boolean immutable_source) {
      this.data = data;
      this.start = p1;
      this.end = p2;
      this.immutable_source = immutable_source;
      this.p = -1;
      this.last_op = 0;
    }

    /**
     * Gets the value at the current position.
     */
    public long get() {
      // Check the point is within the bounds of the iterator,
      if (p < 0 || start + p > end) {
        throw new ArrayIndexOutOfBoundsException();
      }

      // Change the position and fetch the data,
      data.position((start + p) * 8);
      return data.getLong();
    }

    // ----- Implemented from Iterator64Bit -----

    /**
     * Returns the total size of the set.
     */
    public long size() {
      return (end - start) + 1;
    }

    /**
     * Moves the iterator position to the given offset in the set where 0 will
     * move the iterator to the first position, and -1 to before the first
     * position.  Note that for 'next' to return the first value in the set, the
     * position must be set to -1.  For 'previous' to return the last value in
     * the set, the position must be set to size().
     *
     * @param position the position to move to.
     */
    public void position(long p) {
      this.p = p;
    }

    /**
     * Returns the current position of the iterator cursor.  A new iterator will
     * always return -1 (before the first entry).
     */
    public long position() {
      last_op = 0;
      return p;
    }

    /**
     * Returns true if there is a value that can be read from a call to the
     * 'next' method.  Returns false if the iterator is at the end of the set and
     * a value can not be read.
     */
    public boolean hasNext() {
      return (start + p < end);
    }

    /**
     * Returns the next 64-bit from the set and moves the iterator to point
     * to the next value in the set.
     */
    public long next() {
      ++p;
      last_op = 1;
      return get();
    }

    /**
     * Returns true if there is a value that can be read from a call to the
     * 'previous' method.  Returns false if the iterator is at the start of the
     * set and a value can not be read.
     */
    public boolean hasPrevious() {
      return (p > 0);
    }

    /**
     * Returns the previous 64-bit from the set and moves the iterator to point
     * to the previous value in the set.
     */
    public long previous() {
      --p;
      last_op = 2;
      return get();
    }

    /**
     * Returns a copy of this iterator over the same list of values, only with
     * a position that is independent of this object.
     */
    public Iterator64Bit copy() {
      return new OrderedSetIterator(data, start, end,
                                    immutable_source);
    }

    /**
     * Removes a value from the set at the current position.  This can be used
     * immediately after a 'next' or 'previous' call to remove the value that
     * was read.  Note that the call itself acts as a 'next' operation - after
     * this method is called the cursor will point to the next value in the
     * set.
     */
    public long remove() {
      long v;
      data.position((start + p) * 8);
      v = data.getLong();
      data.shift(-8);

      if (last_op == 1) {
        --p;
      }

      --end;

      // Returns the value we removed,
      return v;
    }

  }

}
