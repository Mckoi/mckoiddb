/**
 * com.mckoi.sdb.RowCursor  Jul 9, 2009
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

public class RowCursor implements ListIterator<SDBRow>, SDBTrustedObject {

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
  public boolean hasNext() {
    versionCheck();
    return iterator.hasNext();
  }

  /**
   * Returns true if a call to 'previous' will be able to return the previous
   * value in the list.
   */
  public boolean hasPrevious() {
    versionCheck();
    return iterator.hasPrevious();
  }

  /**
   * Moves the cursor to the next position in the list and returns the SDBRow
   * at the new position.
   */
  public SDBRow next() {
    versionCheck();
    long rowid = iterator.next();
    return new SDBRow(table, rowid);
  }

  /**
   * Returns the position of the next value in the list.
   */
  public int nextIndex() {
    versionCheck();
    return (int) (iterator.position() + 1);
  }

  /**
   * Moves the cursor to the previous position in the list and returns the
   * SDBRow at the new position.
   */
  public SDBRow previous() {
    versionCheck();
    long rowid = iterator.previous();
    return new SDBRow(table, rowid);
  }

  /**
   * Returns the position of the previous value in the list.
   */
  public int previousIndex() {
    versionCheck();
    return (int) (iterator.position() - 1);
  }


  /**
   * This operation is currently not available in RowCursor.
   */
  public void remove() {
    // Methods that mutate the index are not supported,
    throw new UnsupportedOperationException();
  }

  /**
   * Operation not supported in RowCursor.
   */
  public void add(SDBRow e) {
    // Methods that mutate the index are not supported,
    throw new UnsupportedOperationException();
  }

  /**
   * Operation not supported in RowCursor.
   */
  public void set(SDBRow e) {
    // Methods that mutate the index are not supported,
    throw new UnsupportedOperationException();
  }

}
