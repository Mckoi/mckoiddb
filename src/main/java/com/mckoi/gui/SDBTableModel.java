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

package com.mckoi.gui;

import com.mckoi.sdb.RowCursor;
import com.mckoi.sdb.SDBRow;
import com.mckoi.sdb.SDBTable;
import java.util.ArrayList;
import javax.swing.table.AbstractTableModel;

/**
 * A Swing TableModel for modelling the contents of an SDBTable in an
 * interactive graphical interface. This implementation only remains valid
 * while the backed transaction of the SDBTable remains open.
 *
 * @author Tobias Downer
 */

public class SDBTableModel extends AbstractTableModel {

  /**
   * The backed SDBTable object.
   */
  private final SDBTable table;

  /**
   * The cursor for retrieving the information from the backed table.
   */
  private final RowCursor row_iterator;

  /**
   * Fetch cache.
   */
  private final ArrayList<Integer> page_read = new ArrayList();
  private final int PAGE_SIZE = 40;



  /**
   * Constructor.
   */
  public SDBTableModel(SDBTable table, RowCursor row_iterator) {
    this.table = table;
    this.row_iterator = row_iterator;
  }

  @Override
  public Class<?> getColumnClass(int columnIndex) {
    // All SDBTable cells are strings
    return String.class;
  }

  @Override
  public String getColumnName(int column) {
    String[] cols = table.getColumnList();
    return cols[column];
  }

  @Override
  public boolean isCellEditable(int rowIndex, int columnIndex) {
    return false;
  }

  public int getColumnCount() {
    return (int) table.getColumnCount();
  }

  public int getRowCount() {
    return (int) row_iterator.size();
  }

  public Object getValueAt(int rowIndex, int columnIndex) {

    // Did we fetch the page?
    int page_no = (rowIndex / PAGE_SIZE);
    if (!page_read.contains(page_no)) {
      page_read.add(page_no);
      // Don't let 'page_read' get too large
      if (page_read.size() > 64) {
        page_read.remove(0);
      }
      // Prefetch the page,
      int pagep = (page_no * PAGE_SIZE);
      row_iterator.position(pagep - 1);
      for (int i = 0; i < PAGE_SIZE && row_iterator.hasNext(); ++i) {
        SDBRow row = row_iterator.next();
        row.prefetchValueHint(null);
      }
    }

    row_iterator.position(rowIndex - 1);
    SDBRow row = row_iterator.next();
    return row.getValue(getColumnName(columnIndex));
  }

}
