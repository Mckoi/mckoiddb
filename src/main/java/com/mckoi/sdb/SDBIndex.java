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

import com.mckoi.data.Index64Bit;
import com.mckoi.data.IndexObjectCollator;
import com.mckoi.data.Iterator64Bit;
import java.util.ConcurrentModificationException;

/**
 * An index view of a column of an SDBTable. An index is a view of an ordered
 * set of rows in a table. This object provides various methods for creating
 * cursors for traversing an index, and producing alternative views of an
 * index (such as reverse and subset views).
 *
 * @author Tobias Downer
 */

public class SDBIndex implements Iterable<SDBRow> {

  /**
   * The backed table.
   */
  private final SDBTable table;

  /**
   * Current version of the backed table this index is based on.
   */
  private final long table_version;

  /**
   * The collator for values in this index.
   */
  private final IndexObjectCollator collator;

  /**
   * The columnid of this index in the backed table.
   */
  private final long columnid;

  /**
   * The Index64Bit object, representing the backed index, or view of the
   * index.
   */
  private final Index64Bit index;

  /**
   * The start and end of the index view.
   */
  private final long start, end;


  /**
   * Copy constructor.
   */
  private SDBIndex(SDBIndex copied) {
    this.table = copied.table;
    this.table_version = copied.table_version;
    this.collator = copied.collator;
    this.columnid = copied.columnid;
    this.index = copied.index;
    this.start = copied.start;
    this.end = copied.end;
  }

  /**
   * Constructor.
   */
  SDBIndex(SDBTable table, long table_version, IndexObjectCollator collator,
           long columnid, Index64Bit index, long start, long end) {
    this.table = table;
    this.table_version = table_version;
    this.collator = collator;
    this.columnid = columnid;
    this.index = index;
    if (start <= end) {
      this.start = start;
      this.end = end;
    }
    else {
      this.start = start;
      this.end = start;
    }
  }

  /**
   * Constructor.
   */
  SDBIndex(SDBTable table, long table_version, IndexObjectCollator collator,
           long columnid, Index64Bit index) {
    this(table, table_version, collator, columnid, index, 0, index.size());
  }

  private long startPositionSearch(String e, boolean inclusive) {
    long pos;
    if (!inclusive) {
      // Not inclusive
      pos = index.searchLast(e, collator);
      if (pos < 0) {
        pos = -(pos + 1);
      }
      else {
        pos = pos + 1;
      }
    }
    else {
      // Inclusive
      pos = index.searchFirst(e, collator);
      if (pos < 0) {
        pos = -(pos + 1);
      }
    }

    // If it's found, if the position is out of bounds then cap it,
    if (pos >= 0) {
      if (pos < start) {
        pos = start;
      }
      if (pos > end) {
        pos = end;
      }
    }
    return pos;
  }

  private long endPositionSearch(String e, boolean inclusive) {
    long pos;
    if (!inclusive) {
      // Not inclusive
      pos = index.searchFirst(e, collator);
      if (pos < 0) {
        pos = -(pos + 1);
      }
    }
    else {
      // Inclusive
      pos = index.searchLast(e, collator);
      if (pos < 0) {
        pos = -(pos + 1);
      }
      else {
        pos = pos + 1;
      }
    }

    // If it's found, if the position is out of bounds then cap it,
    if (pos >= 0) {
      if (pos < start) {
        pos = start;
      }
      if (pos > end) {
        pos = end;
      }
    }
    return pos;
  }

  private void versionCheck() {
    if (table_version != table.getCurrentVersion()) {
      throw new ConcurrentModificationException();
    }
  }

  // ----- Implemented methods

  /**
   * Returns the number of rows in this view of the index.
   */
  public long size() {
    versionCheck();
    return end - start;
  }

  /**
   * Returns true if this index is empty, false otherwise.
   */
  public boolean isEmpty() {
    return (size() == 0);
  }

  /**
   * Returns true if this indexed view contains the given value.
   */
  public boolean contains(String e) {
    versionCheck();
    // Find the start and end positions,
    long pos_s = index.searchFirst(e, collator);
    if (pos_s >= 0) {
      if (pos_s >= end) {
        return false;
      }
      if (pos_s >= start) {
        return true;
      }
      // We are here if pos_s < start, so check if the last value is within the
      // bounds,
      long pos_e = index.searchLast(e, collator);
      return pos_e >= start;
//      // If found, check the value is within the bounds,
//      pos_e = index.searchLast(e, collator);
//      return (pos_s < end && pos_e >= start);
    }
    // Not found, return false
    return false;
  }

  /**
   * Returns the first row in this index view, or null if the set is empty.
   */
  public SDBRow first() {
    versionCheck();
    // Return null if empty
    if (start >= end) {
      return null;
    }
    long rowid = index.get(start);
    return new SDBRow(table, rowid);
  }

  /**
   * Returns the last row in this index view, or null if the set is empty.
   */
  public SDBRow last() {
    versionCheck();
    // Return null if empty
    if (start >= end) {
      return null;
    }
    long rowid = index.get(end - 1);
    return new SDBRow(table, rowid);
  }

  /**
   * Returns the first value in this index view, or null if the set is empty.
   */
  public String firstValue() {
    SDBRow first_row = first();
    if (first_row != null) {
      return first_row.getValue(columnid);
    }
    return null;
  }

  /**
   * Returns the last value in this index view, or null if the set is empty.
   */
  public String lastValue() {
    SDBRow last_row = last();
    if (last_row != null) {
      return last_row.getValue(columnid);
    }
    return null;
  }

  /**
   * Returns a cursor for traversing this view of the index from the start
   * position onwards.
   */
  @Override
  public RowCursor iterator() {
    versionCheck();
    return new RowCursor(table, table_version,
                         index.iterator(start, end - 1));
  }

  // ----- Index views

  /**
   * Returns a reverse view of this index.
   */
  public SDBIndex reverse() {
    versionCheck();
    return new ReverseIndex(this);
  }

  /**
   * Returns the head view of this index, between position 0 and the position of
   * the given element in the index. If 'inclusive' is true, includes rows
   * that equal the element string.
   */
  public SDBIndex head(String toElement, boolean inclusive) {
    versionCheck();
    // Find the element position
    long pos = endPositionSearch(toElement, inclusive);
    return new SDBIndex(table, table_version,
                        collator, columnid, index, start, pos);
  }

  /**
   * The tail view of this index, between the position of the given element
   * in the index and the end of the index. If 'inclusive' is true, includes rows
   * that equal the element string.
   */
  public SDBIndex tail(String fromElement, boolean inclusive) {
    versionCheck();
    // Find the element position
    long pos = startPositionSearch(fromElement, inclusive);
    return new SDBIndex(table, table_version,
                        collator, columnid, index, pos, end);
  }

  /**
   * The subset view of rows in this index, between the positions of the given
   * elements in the index. If 'fromInclusive' is true, includes rows that
   * equal the fromElement string. If 'toInclusive' it true, includes rows that
   * equal the toElement string.
   */
  public SDBIndex sub(
          String fromElement, boolean fromInclusive,
          String toElement, boolean toInclusive) {
    versionCheck();

    long nstart = startPositionSearch(fromElement, fromInclusive);
    long nend = endPositionSearch(toElement, toInclusive);

    return new SDBIndex(table, table_version,
                        collator, columnid, index, nstart, nend);
  }

  /**
   * The same as the call 'head(toElement, false)'
   */
  public SDBIndex head(String toElement) {
    return head(toElement, false);
  }

  /**
   * The same as the call 'tail(fromElement, true)'
   */
  public SDBIndex tail(String fromElement) {
    return tail(fromElement, true);
  }

  /**
   * The same as the call 'sub(fromElement, true, toElement, false)'
   */
  public SDBIndex sub(String fromElement, String toElement) {
    return sub(fromElement, true, toElement, false);
  }


  /**
   * A reverse view of an index which maps functions inversely to the original
   * index set.
   */
  private static class ReverseIndex extends SDBIndex {

    private final SDBIndex original;

    ReverseIndex(SDBIndex backed) {
      super(backed);
      this.original = backed;
    }

    @Override
    public SDBIndex reverse() {
      return original;
    }

    @Override
    public SDBRow first() {
      return super.last();
    }

    @Override
    public SDBRow last() {
      return super.first();
    }

    @Override
    public SDBIndex head(String toElement, boolean inclusive) {
      return new ReverseIndex(super.tail(toElement, inclusive));
    }

    @Override
    public SDBIndex tail(String fromElement, boolean inclusive) {
      return new ReverseIndex(super.head(fromElement, inclusive));
    }

    @Override
    public SDBIndex sub(String fromElement, boolean fromInclusive,
                        String toElement, boolean toInclusive) {
      return new ReverseIndex(
                super.sub(toElement, toInclusive, fromElement, fromInclusive));
    }

    @Override
    public RowCursor iterator() {
      return new RowCursor(super.table, super.table_version,
                 new ReverseIterator64Bit(
                            super.index.iterator(super.start, super.end - 1)));
    }

  }

  /**
   * A reverse view of an Iterator64Bit.
   */
  static class ReverseIterator64Bit implements Iterator64Bit {

    private final Iterator64Bit backed;

    ReverseIterator64Bit(Iterator64Bit i) {
      backed = i;
      backed.position(backed.size());
    }

    @Override
    public Iterator64Bit copy() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasNext() {
      return backed.hasPrevious();
    }

    @Override
    public boolean hasPrevious() {
      return backed.hasNext();
    }

    @Override
    public long next() {
      return backed.previous();
    }

    @Override
    public long previous() {
      return backed.next();
    }

    @Override
    public void position(long p) {
      long size = backed.size();
      backed.position(size - (p + 1));
    }

    @Override
    public long position() {
      long size = backed.size();
      return size - (backed.position() + 1);
    }

    @Override
    public long remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long size() {
      return backed.size();
    }

  }

}
