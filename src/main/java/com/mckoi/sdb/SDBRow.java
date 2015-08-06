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
