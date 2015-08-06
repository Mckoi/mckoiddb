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
 * An iterator interface for forward and backward traversal of a sequence of
 * 64-bit values. Iterator64Bit also supports removal of values from the
 * backed collection, and free location assignment of the iterator pointer.
 *
 * @author Tobias Downer
 */

public interface Iterator64Bit {

  /**
   * Returns the total number of 64-bit values in the group of values
   * defined by the scope of this iterator.
   */
  long size();

  /**
   * Moves the position location to the given offset in the iterator address
   * space where 0 will position the iterator to the first location, and -1 to
   * before the first location.  Note that for the 'next' method to return the
   * first value in the collection, the position should be set to -1.  For the
   * 'previous' method to return the last value in the set, the position must
   * be set to size().
   *
   * @param position the position to move to.
   */
  void position(long p);

  /**
   * Returns the current position of the iterator within the address space as
   * initially defined when the iterator was created and after any removal
   * operations (from calls to 'remove') have been performed.  A new iterator
   * will always return -1 (before the first entry).
   */
  long position();

  /**
   * Returns true if there is a value that can be read from a call to the
   * 'next' method.  Returns false if the iterator is at the end of the
   * collection and a value may not be read.
   */
  boolean hasNext();

  /**
   * Returns the next 64-bit from the backed collection and moves the position
   * location to the next value in the set.
   */
  long next();

  /**
   * Returns true if there is a value that can be read by a call to the
   * 'previous' method.  Returns false if the iterator is at the start of the
   * collection and a value may not be read.
   */
  boolean hasPrevious();

  /**
   * Returns the previous 64-bit from the backed collection and moves the
   * position location to the previous value in the collection.
   */
  long previous();

  /**
   * Returns a copy of this iterator over an identical collection of values but
   * with a positional pointer that can be moved independently of this
   * iterator.
   */
  Iterator64Bit copy();


  /**
   * Removes a value from the set at the current position.  This should be used
   * immediately after a 'next' or 'previous' call to remove the value that
   * was last accessed.  Note that the call itself acts as a 'next' operation -
   * when this method returns the interator position will point to the next
   * value in the collection.
   */
  long remove();

}

