/**
 * com.mckoi.sdb.SDBRow  Jul 7, 2009
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

/**
 * The storage component for a row of data in an SDBTable. The content of
 * the row is accessed by using the 'getValue' method.
 *
 * @author Tobias Downer
 */

public class SDBRow {

  /**
   * The backed SDBTable that this row belongs in.
   */
  private final SDBTable table;

  /**
   * The unique rowid of this row data. This value should not be exposed to
   * user code.
   */
  private final long rowid;

  /**
   * Constructs the row.
   */
  SDBRow(SDBTable table, long rowid) {
    this.table = table;
    this.rowid = rowid;
  }

  // ----- Package protected

  /**
   * Returns the row id value of this row. This is a unique identifier of the
   * location in the storage system where the row information can be found.
   * The value given to a row may change between versions of the table, so
   * this rowid should not be used to identify a row globally.
   */
  long getRowIdValue() {
    return rowid;
  }

  /**
   * Returns the String value of the given column in this row. This is
   * package protected, 'columnid' should not be exposed to user code.
   */
  String getValue(long columnid) {
    return table.getCellValue(rowid, columnid);
  }

  // ----- Public methods,

  /**
   * Returns the value of the given column of this row.
   */
  public String getValue(String column_name) {
    return getValue(table.getColumnId(column_name));
  }

  /**
   * Provides a hint to the engine that there is a high likelihood of this
   * value being accessed shortly. This hint is used to optimize access
   * characteristics in high latency environments (the client may choose to
   * preemptively download this record next time there is network
   * interaction).
   */
  public void prefetchValueHint(String column_name) {
    table.prefetchValueHint(rowid, -1);
  }

}
