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

package com.mckoi.sdb;

import com.mckoi.data.Iterator64Bit;
import java.util.ConcurrentModificationException;
import java.util.ListIterator;

/**
 * A row cursor is a random access and bi-directional iterator for traversing
 * a set of rows in a table. RowCursor implements ListIterator and provides
 * additional functionality making it appropriate for interactive applications
 * (such as displaying the contents of a very large table dynamically in a GUI).
 *
 * @author Tobias Downer
 */

public class RowCursor implements ListIterator<SDBRow> {

  private final SDBTable table;
  private final long table_version;
  private final Iterator64Bit iterator;

  RowCursor(SDBTable table, long table_version, Iterator64Bit iterator) {
    this.table = table;
    this.table_version = table_version;
    this.iterator = iterator;
  }

  private void versionCheck() {
    if (table_version != table.getCurrentVersion()) {
      throw new ConcurrentModificationException();
    }
  }

  // ------

  /**
   * Returns the size of the set of rows encapsulated by this cursor.
   */
  public long size() {
    versionCheck();
    return iterator.size();
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
    versionCheck();
    iterator.position(p);
  }

  /**
   * Returns the current position of the iterator cursor.  A new iterator will
   * always return -1 (before the first entry).
   */
  public long position() {
    versionCheck();
    return iterator.position();
  }

  // ----- Implemented from ListIterator<SDBRow>

  /**
   * Returns true if a call to 'next' will be able to return the next value in
   * the list.
   */
  @Override
  public boolean hasNext() {
    versionCheck();
    return iterator.hasNext();
  }

  /**
   * Returns true if a call to 'previous' will be able to return the previous
   * value in the list.
   */
  @Override
  public boolean hasPrevious() {
    versionCheck();
    return iterator.hasPrevious();
  }

  /**
   * Moves the cursor to the next position in the list and returns the SDBRow
   * at the new position.
   */
  @Override
  public SDBRow next() {
    versionCheck();
    long rowid = iterator.next();
    return new SDBRow(table, rowid);
  }

  /**
   * Returns the position of the next value in the list.
   */
  @Override
  public int nextIndex() {
    versionCheck();
    return (int) (iterator.position() + 1);
  }

  /**
   * Moves the cursor to the previous position in the list and returns the
   * SDBRow at the new position.
   */
  @Override
  public SDBRow previous() {
    versionCheck();
    long rowid = iterator.previous();
    return new SDBRow(table, rowid);
  }

  /**
   * Returns the position of the previous value in the list.
   */
  @Override
  public int previousIndex() {
    versionCheck();
    return (int) (iterator.position() - 1);
  }


  /**
   * This operation is currently not available in RowCursor.
   */
  @Override
  public void remove() {
    // Methods that mutate the index are not supported,
    throw new UnsupportedOperationException();
  }

  /**
   * Operation not supported in RowCursor.
   */
  @Override
  public void add(SDBRow e) {
    // Methods that mutate the index are not supported,
    throw new UnsupportedOperationException();
  }

  /**
   * Operation not supported in RowCursor.
   */
  @Override
  public void set(SDBRow e) {
    // Methods that mutate the index are not supported,
    throw new UnsupportedOperationException();
  }

}
