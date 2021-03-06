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

import com.mckoi.data.DataFile;
import com.mckoi.sdb.RowCursor;
import com.mckoi.sdb.SDBIndex;
import com.mckoi.sdb.SDBSession;
import com.mckoi.sdb.SDBTable;
import com.mckoi.sdb.SDBTransaction;
import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.util.ArrayList;
import javax.swing.BorderFactory;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.border.EmptyBorder;
import javax.swing.tree.MutableTreeNode;
import javax.swing.tree.TreePath;

/**
 * 
 *
 * @author Tobias Downer
 */

public class SDBViewer implements PathViewer {

  /**
   * The SDBSession object.
   */
  private SDBSession session;

  /**
   * The current transaction.
   */
  private SDBTransaction transaction;

  /**
   * The path tree.
   */
  private SDBPathNode path_tree;



  public SDBViewer(String path_name,
                   SDBSession session, SDBTransaction transaction) {
    this.session = session;
    this.transaction = transaction;

    path_tree = new SDBPathNode(path_name);
    path_tree.refresh(transaction);
  }



  // ----- Implemented -----

  public JComponent getObjectView(TreePath selected_path) {

    if (selected_path.getPathCount() == 4) {

      String ob_type = selected_path.getPathComponent(2).toString();
      String ob_name = selected_path.getPathComponent(3).toString();

      if (ob_type.equals("Tables")) {

        String table_name = ob_name;

        SDBTable table = transaction.getTable(table_name);
        RowCursor i = table.iterator();
        SDBTableModel table_model = new SDBTableModel(table, i);

        JPanel panel = new JPanel();
        panel.setLayout(new BorderLayout());

        final String tname = ob_name;

        final JTable table_view = new JTable(table_model);
        JScrollPane scrolly_table_contents = new JScrollPane(table_view);

        // The index list,
        String[] indexed_cols = table.getIndexedColumnList();
        ArrayList<String> index_list = new ArrayList();
        index_list.add("(Insert Order)");
        for (String index_name : indexed_cols) {
          index_list.add(index_name);
        }

        JComboBox index_combobox = new JComboBox();
        index_combobox.setModel(new DefaultComboBoxModel(index_list.toArray()));

        index_combobox.addItemListener(new ItemListener() {
          public void itemStateChanged(ItemEvent evt) {
            if (evt.getStateChange() == ItemEvent.SELECTED) {
              SDBTable table_ds = transaction.getTable(tname);
              String index_name = (String) evt.getItem();

              RowCursor sort_order;
              if (index_name.equals("(Insert Order)")) {
                sort_order = table_ds.iterator();
              }
              else {
                SDBIndex i = table_ds.getIndex(index_name);
                sort_order = i.iterator();
              }
              SDBTableModel model = new SDBTableModel(table_ds, sort_order);
              table_view.setModel(model);
            }
          }
        });

        JPanel top_panel = new JPanel();
        top_panel.setLayout(new FlowLayout());
        top_panel.add(new JLabel("Index Order: "));
        top_panel.add(index_combobox);


        JLabel status_label = new JLabel("Row Count: " + table.getRowCount());
        status_label.setBorder(new EmptyBorder(1, 4, 1, 4));

        panel.add(scrolly_table_contents, BorderLayout.CENTER);
        panel.add(status_label, BorderLayout.SOUTH);
        panel.add(top_panel, BorderLayout.NORTH);

        return panel;

      }
      else if (ob_type.equals("Files")) {

        final String file_name = ob_name;

        HexViewer filev_hex_viewer = new HexViewer();
        JScrollPane scrolly_hex_view = new JScrollPane(filev_hex_viewer);

        JLabel filev_status_panel = new JLabel("Status");
        filev_status_panel.setBorder(BorderFactory.createEmptyBorder(2,8,2,8));

        // Lay out these components on a panel
        JPanel panel = new JPanel();
        panel.setBorder(new EmptyBorder(4, 4, 4, 4));
        panel.setLayout(new BorderLayout());
        panel.add(scrolly_hex_view, BorderLayout.CENTER);
        panel.add(filev_status_panel, BorderLayout.SOUTH);

        DataFile df = transaction.getFile(file_name);
        filev_hex_viewer.setDataFile(df);

        StringBuilder status_str = new StringBuilder();
        status_str.append("<html>");
        status_str.append("Size: ");
        status_str.append(df.size());
        status_str.append(" bytes.");
        status_str.append("</html>");
        filev_status_panel.setText(status_str.toString());

        return panel;

      }

    }

    return null;
  }

  public MutableTreeNode getPathTree() {
    return path_tree;
  }

  public JComponent queryComponent(String dialog_name) {
    return new JLabel("No query component for Simple Database yet.", JLabel.CENTER);
  }

  public void refresh() {
    // Create a new transaction,
    transaction = session.createTransaction();

    // Refresh the path tree,
    path_tree.refresh(transaction);

    

  }

}
