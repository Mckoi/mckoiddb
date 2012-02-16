/**
 * com.mckoi.sdb.gui.SDBTableModel  Jul 12, 2009
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
