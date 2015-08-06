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
 * A DataRange is a sorted set of consecutive keys stored in a
 * KeyObjectTransaction. The order of the keys in the set is determined by
 * the natural ordering of the com.mckoi.data.Key class. For example, three
 * keys would be ordered as such; (0-0-4), (0-0-10), (0-0-104).
 * <p>
 * Keys that have no data will not be found in the set, therefore to remove
 * an item from the set a call to 'delete' or 'setSize(0)' in the DataFile
 * with the key is necessary. To create a key that doesn't exist, simply
 * fetch a DataFile with the desired key and put data in it.
 * <p>
 * When the backed transaction is changed, the changes are immediately
 * reflected in this object.
 * <p>
 * DataRange provides methods for determining the size of all the content of
 * data in the set, for finding the Key of the DataFile that contains data at
 * the given position in a composite of the content, and for replicating and
 * deleting the entire content composite of data referenced by the keys in the
 * set.
 * <p>
 * Implementations should provide efficient methods for all operations (ie.
 * no operations should be O(n) on massive key sets).
 *
 * @author Tobias Downer
 */

public interface DataRange {

  /**
   * Returns the size of the composite of all data content referenced by
   * the keys in the range. For example, a set with three keys whose content
   * sizes are 50, 100, 16 will return the total size of 166.
   */
  long size();

  // ----- Composite positioning / queries ----

  /**
   * Sets the position of the pointer within the address space of the
   * composite of all data in the range. The address space range is
   * 0 to size().
   * <p>
   * Setting the position outside of the bounds will not
   * causes an exception, but other operations that use the position will
   * generate an exception if the position is out of bounds.
   */
  void position(long position);

  /**
   * Moves the pointer to the location of the start of the key being referenced
   * by the current position, and returns the new position. The result of
   * 'keyAtPosition' will be no different if called before this operation to
   * if called after.
   * <p>
   * An OutOfBoundsException will be generated if the starting position is
   * less than 0 or greater or equal to size().
   */
  long positionOnKeyStart();

  /**
   * Moves the pointer to the location of the start of the next key in the
   * sequence from the key referenced by the current pointer, and returns the
   * new position. This can also be used to find the position of the end of
   * the current key being referenced.
   * <p>
   * An OutOfBoundsException will be generated if the starting position is
   * less than 0 or greater or equal to size().
   */
  long positionOnNextKey();

  /**
   * Moves the pointer to the location of the start of the previous key in the
   * sequence from the key referenced by the current pointer, and returns the
   * new position.
   * <p>
   * An OutOfBoundsException will be generated if the starting position is
   * less than 0 or greater than size().
   */
  long positionOnPreviousKey();

  /**
   * Returns the current position of the pointer within the address space.
   */
  long position();

  /**
   * Returns the Key of the data that stores the content at the current
   * position of the ordered composite of all data in the key set. For example,
   * consider a set of three keys which reference DataFiles with content with
   * the following sizes; (50, 100, 16). The key of the first item will be
   * returned for any position between 0 and 49, the second item key will be
   * returned for positions 50 to 149, and the third for positions 150 to 165.
   * <p>
   * An OutOfBoundsException will be generated for any position less than 0 or
   * greater or equal to size().
   */
  Key keyAtPosition();

  /**
   * Returns the DataFile of the content of the key referenced at the current
   * position. This is a convenience for calling;
   * 'transaction.getDataFile(data_range.keyAtPosition(), mode)'. The address
   * space of the returned DataFile is relative to the scope of the DataFile
   * itself, not the range composite.
   * <p>
   * Keep in mind that changing the size of the returned DataFile will change
   * the relative position of the content of all data after this file in the
   * range. Changing the size of a data file referenced in this range will not
   * change the position value. This means that changing the size of a
   * DataFile may causes the position to reference a different key.
   * <p>
   * An OutOfBoundsException will be generated for any position less than 0 or
   * greater or equal to size().
   */
  AddressableDataFile getDataFile(char mode);

  /**
   * Returns the DataFile of the content of the given key. This is a
   * convenience for calling;
   * 'transaction.getDataFile(key, mode)'. The address space of the returned
   * DataFile is relative to the scope of the DataFile itself, not the range
   * composite. An exception is immediately generated if the given key does
   * not fall within the bounds defined by this range.
   * <p>
   * Keep in mind that changing the size of the returned DataFile will change
   * the relative position of the content of all data after this file in the
   * range. Changing the size of a data file referenced in this range will not
   * change the position value. This means that changing the size of a
   * DataFile may causes the position to reference a different key.
   * <p>
   * An OutOfBoundsException will be generated for any position less than 0 or
   * greater or equal to size().
   */
  AddressableDataFile getDataFile(Key key, char mode);

  // ----- Modification -----

  /**
   * Deletes the entire content of data represented by this range. Care should
   * be taken when using this method because the content of all the keys in
   * the range will be lost.
   * <p>
   * A DataFile referenced by a key that was deleted by this method will have
   * a size of 0 when this method returns. This method is the same as calling
   * 'getDataFile(key, 'w').delete()' on all keys in the bounds.
   * <p>
   * Note that this is not a convenience function for deleting data file
   * content individually. This implementation should employ an efficient
   * method for removing (or unlinking) the data with keys in the range.
   */
  void delete();

  /**
   * Replicates the entire content of data in the 'from' range in this
   * DataRange. This DataRange must have a scope of keys to which this range
   * intersects completely. Any existing content in the key set that is
   * covered by this range will be deleted by this operation and the key
   * content from the 'from' range will be copied to this range.
   * <p>
   * This operation preserves key values on the replicated content.
   * <p>
   * This method will fail if this range is within the same transaction as
   * the 'from' range, or if the this range does not support keys in
   * the 'from' range. On failure, this method may have partially replicated
   * keys from the given range, or it may delete all the keys stored in
   * this range if there are any.
   */
  void replicateFrom(DataRange from);

}
