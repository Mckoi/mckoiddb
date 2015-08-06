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
 * An Index64Bit is an addressable list of 64 bit values in which elements may
 * be inserted or deleted from any location, and the layout is such that it
 * enables the quick searching of elements in the list.  Such structures enable
 * the sorting and searching of more complex data elements because the 64 bit
 * values can map to objects in another addressable space.
 * <p>
 * Implementations that could support this interface
 * vary from things like simple heap arrays to complex tree structures.
 * 
 * @author Tobias Downer
 */

public interface Index64Bit {

  /**
   * Clears the index of all values.
   */
  void clear();

  /**
   * Clears the index of size number of values from the given position.
   */
  void clear(long pos, long size);

  /**
   * Returns the total number of elements in the index.
   */
  long size();

  /**
   * Assuming the index is ordered, searches for the first instance of the
   * specified value within the index and returns the position of the first
   * value, or returns -(position + 1) if the specified value is not found and
   * position is the place where the value would be inserted to maintain index
   * integrity (sort order).
   * <p>
   * The correct operation of this function depends on the index maintaining
   * a consistent order through its lifetime.  If the collation characteristics
   * change then the result of this function is undefined.
   *
   * @param value the higher order object to search for.
   * @param c the comparator between higher order objects and the 64 bit
   *   elements in the index.
   * @return the position of the first value in the index.
   */
  long searchFirst(Object value, IndexObjectCollator c);

  /**
   * Assuming the index is ordered, searches for the last instance of the
   * specified value within the index and returns the position of the last
   * value, or returns -(position + 1) if the specified value is not found and
   * position is the place where the value would be inserted to maintain index
   * integrity (sort order).
   * <p>
   * The correct operation of this function depends on the index maintaining
   * a consistent order through its lifetime.  If the collation characteristics
   * change then the result of this function is undefined.
   *
   * @param value the higher order object to search for.
   * @param c the comparator between higher order objects and the 64 bit
   *   elements in the index.
   * @return the position of the last value in the index.
   */
  long searchLast(Object value, IndexObjectCollator c);

  /**
   * Returns the 64 bit element at the given position in the index.  Note
   * that in many implementations this will not be an O(1) operation so
   * attention should be paid to the implementation details.  In cases where
   * you wishes to read a sequence of values from the list, use an iterator.
   */
  long get(long position);

  /**
   * Returns a Iterator64Bit that is limited between the boundaries specified
   * and allows for the access of all elements in the subset if the index.
   * <p>
   * IMPORTANT: While an iterator exists we assume that no modifications
   * will be made to the index.  If the index changes while an iterator is
   * active the behavior is unspecified.
   *
   * @param start the offset of the first position (inclusive).
   * @param end the offset of the last position (inclusive).
   */
  Iterator64Bit iterator(long start, long end);

  /**
   * Returns a Iterator64Bit that is setup to iterate the entire index from
   * start to end.
   * <p>
   * IMPORTANT: While an iterator exists we assume that no modifications
   * will be made to the index.  If the index changes while an iterator is
   * active the behavior is unspecified.
   */
  Iterator64Bit iterator();

  /**
   * Inserts a 64-bit element, ref, into the index at the ordered position.
   * If there are elements in the index that are the same value as the
   * one being inserted, the element is inserted after the end of the group
   * of equal elements in the set.
   * <p>
   * The correct operation of this function depends on the index maintaining
   * a consistent order through its lifetime.  If the collation characteristics
   * change then the result of this function is undefined.
   *
   * @param value the higher order object to insert.
   * @param ref the 64-bit value to insert into the index and which maps to
   *   'value'.
   * @param c the comparator between higher order objects and the 64 bit
   *   elements in the index.
   */
  void insert(Object value, long ref, IndexObjectCollator c);

  /**
   * Inserts a 64-bit element, ref, into the index at the ordered position.
   * If there there are elements in the index that are the same value as the
   * one being inserted, the method returns false and does not change the
   * list.
   * <p>
   * The correct operation of this function depends on the index maintaining
   * a consistent order through its lifetime.  If the collation characteristics
   * change then the result of this function is undefined.
   *
   * @param value the higher order object to insert.
   * @param ref the 64-bit value to insert into the index and which maps to
   *   'value'.
   * @param c the comparator between higher order objects and the 64 bit
   *   elements in the index.
   */
  boolean insertUnique(Object value, long ref, IndexObjectCollator c);

  /**
   * Removes a 64-bit element, ref, from the index at the ordered position.  If
   * there are multiple elements in the index with the same value, the first
   * element is removed.
   * <p>
   * The correct operation of this function depends on the index maintaining
   * a consistent order through its lifetime.  If the collation characteristics
   * change then the result of this function is undefined.
   *
   * @param value the higher order object to remove.
   * @param ref the 64-bit value to remove from the index and which maps to
   *   'value'.
   * @param c the comparator between higher order objects and the 64 bit
   *   elements in the index.
   */
  void remove(Object value, long ref, IndexObjectCollator c);

  /**
   * Adds a 64-bit value to the end of the index ignoring any ordering
   * scheme that may have previously been used to insert values into the
   * index.  This is a useful efficiency for the common case when the order of
   * values is already known.
   * 
   * @param ref the 64-bit value to insert into the index.
   */
  void add(long ref);

  /**
   * Inserts a 64-bit value at the given position in the list shifting
   * any values after the position forward by one position.
   * 
   * @param ref the 64-bit value to insert into the index.
   * @param pos the position to insert the value 'ref' into the list.
   */
  void insert(long ref, long pos);

  /**
   * Removes a 64-bit value from the given position in the list shifting
   * any values after the position backwards by one position.  Generates an
   * exception if pos is out of range or the value could not be removed for
   * another reason (such as the structure being read-only).
   * 
   * @param pos the position of the value to remove from the list.
   * @returns the value removed from the list.
   */
  long remove(long pos);

  /**
   * Inserts a 64-bit value at an ordered position in the index where the order
   * is the ascending collation of the 64-bit values.  For example, inserting
   * 6 into the list { 1, 3, 7, 9 } would result in the list { 1, 3, 6, 7, 9 }.
   * <p>
   * The correct operation of this function depends on the index maintaining
   * a consistent order through its lifetime.  If the collation characteristics
   * change then the result of this function is undefined.
   *
   * @param ref the 64-bit value to insert into the index.
   */
  void insertSortKey(long ref);

  /**
   * Removes the first 64-bit value from the order position in the index where
   * the order is the ascending collation of the 64-bit values.  Generates an
   * exception if the value is not found in the index.
   * <p>
   * The correct operation of this function depends on the index maintaining
   * a consistent order through its lifetime.  If the collation characteristics
   * change then the result of this function is undefined.
   *
   * @param ref the 64-bit value to insert into the index.
   */
  void removeSortKey(long ref);

  /**
   * Returns true if the set contains the given 64-bit value assuming the order
   * of the set is the ascending collation of the 64-bit values.  Returns false
   * if the value was not found.
   * <p>
   * The correct operation of this function depends on the index maintaining
   * a consistent order through its lifetime.  If the collation characteristics
   * change then the result of this function is undefined.
   */
  boolean containsSortKey(long ref);
  
}
