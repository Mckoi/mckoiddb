/**
 * com.mckoi.treestore.FixedSizeSerialSet  Dec 15, 2007
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

import com.mckoi.util.Cache;

/**
 * A set of fixed sized binary record elements arranged in a sequence over a
 * DataFile.  This abstract class provides various facilities for implementing
 * a DataFile structure to manage a collection of fixed size records.
 * <p>
 * The structure of the file is a simple arrangement of the record elements
 * in ordered sequence.  Records may be inserted or removed from any position.
 * <p>
 * Note that this class does not provide specific functions for reading or
 * writing the content of record data, leaving the record format up to the
 * implementation.  It does provide convenience methods for collating and
 * searching the records, however.
 * <p>
 * Also note this class implements a cache on calls to 'searchForRecord' to
 * improve key lookup.  If any operations change the result of
 * 'searchForRecord' for any value of 'key' then the cache should be
 * invalidated with a call to 'clearKeyCache'.
 *
 * @author Tobias Downer
 */

public abstract class FixedSizeSerialSet {

  /**
   * The DataFile object.
   */
  private final DataFile data;
  
  /**
   * The size in bytes of each record in the set.
   */
  private final int record_size;
  
  /**
   * Cache for key to position lookups.
   */
  private final Cache key_position_cache;
  
  
  /**
   * Constructor.
   */
  public FixedSizeSerialSet(DataFile data, int record_size) {
    assert(record_size > 0);

    this.data = data;
    this.record_size = record_size;
    this.key_position_cache = new Cache(513, 750, 15);
  }


  /**
   * Performs a binary search to find the position of the key assigned the
   * record assuming the records are sorted in the set by key value.
   */
  private long keyPosition(Object key) {
    long rec_start = 0;
    long rec_end = size();

    long low = rec_start;
    long high = rec_end - 1;

    while (low <= high) {

      if (high - low <= 2) {
        for (long i = low; i <= high; ++i) {
          int cmp = compareRecordTo(i, key);
          if (cmp == 0) {
            return i;
          }
          else if (cmp > 0) {
            return -(i + 1);
          }
        }
        return -(high + 2);
      }

      long mid = (low + high) / 2;
      int cmp = compareRecordTo(mid, key);

      if (cmp < 0) {
        low = mid + 1;
      }
      else if (cmp > 0) {
        high = mid - 1;
      }
      else {
        high = mid;
      }
    }
    return -(low + 1);  // key not found.
  }

  /**
   * Returns the DataFile object for access to the record data.
   */
  protected DataFile getDataFile() {
    return data;
  }

  /**
   * Moves the position of the DataFile object to the start of the given
   * record location.
   */
  protected void positionOn(long record_num) {
    getDataFile().position(record_num * getRecordSize());
  }

  /**
   * Inserts an empty record at the given record location ready for new data
   * to be inserted into it.
   */
  protected void insertEmpty(long record_num) {
    // If we are inserting the record at the end, we must grow the size of the
    // file by the size of the record.
    if (record_num == size()) {
      getDataFile().setSize((record_num + 1) * getRecordSize());
    }
    // Otherwise we shift the data in the file so that we have space to insert
    // the record.
    else {
      positionOn(record_num);
      getDataFile().shift(getRecordSize());
    }
  }
  
  /**
   * Removes the record at the given position.
   */
  protected void removeRecordAt(long record_num) {
    // Position on the record
    positionOn(record_num + 1);
    // Shift the data in the file
    getDataFile().shift(-getRecordSize());
  }
  
  /**
   * Clears the key cache.
   */
  protected void clearKeyCache() {
    key_position_cache.clear();
  }
  
  /**
   * Returns the size of the records in the file in bytes.
   */
  public int getRecordSize() {
    return record_size;
  }
  
  /**
   * Returns the number of records in this set.
   */
  public long size() {
    // The size of the data file divided by the record size.
    return getDataFile().size() / getRecordSize();
  }
  
  /**
   * performs a binary search for the given object key in the set.  If the
   * record is found then the record position is returned.  If the record is
   * not found then a negative value is returned which can be converted into
   * the location at which the record should be inserted into the set to
   * maintain correct ordering, with the calculation -(returned_val + 1).
   * <p>
   * Calls to this method are backed by an internal cache that maps the key
   * object to the position.
   */
  public long searchForRecord(Object key) {
    // Check the cache
    Long v = (Long) key_position_cache.get(key);
    long pos;
    if (v == null) {
      pos = keyPosition(key);
      if (pos >= 0) {
        key_position_cache.put(key, new Long(pos));
      }
    }
    else {
      pos = v.longValue();
    }
    return pos;
    
//    if (key_position_cache.containsKey(key)) {
//      return key_position_cache.get(key);
//    }
//    else {
//      long pos = keyPosition(key);
//      if (pos >= 0) {
//        key_position_cache.put(key, pos);
//      }
//      return pos;
//    }
  }

  /**
   * Remove the record with the given key from the set.  Returns true if the
   * record was found and removed, false otherwise.
   */
  public boolean remove(Object key) {
    // Search for the position of the record
    long pos = searchForRecord(key);
    if (pos >= 0) {
      // Record found, so remove it
      removeRecordAt(pos);
      // And remove from the cache
      key_position_cache.remove(key);
      return true;
    }
    return false;
  }

  /**
   * Returns true if the set contains a record with the given key value.
   */
  public boolean contains(Object key) {
    // Search for the position of the record
    long pos = searchForRecord(key);
    return pos >= 0;
  }

  // ----- Abstract methods -----

  /**
   * Given a position of a record within the set, returns an Object used as
   * collation reference of records within the set.
   */
  protected abstract Object getRecordKey(long record_pos);

  /**
   * Compares the record at the given position in the set with the record key
   * given, and returns a positive number of a &gt; b, 0 if a = b, or a negative
   * number if a &lt; b.
   */
  protected abstract int compareRecordTo(long record_pos, Object record_key);

}
