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

import com.mckoi.odb.ODBClass;
import com.mckoi.odb.ODBObject;
import com.mckoi.odb.ODBSession;
import com.mckoi.odb.ODBTransaction;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.tree.MutableTreeNode;
import javax.swing.tree.TreePath;

/**
 * 
 *
 * @author Tobias Downer
 */

public class ODBViewer implements PathViewer {

  /**
   * The ODBSession object.
   */
  private ODBSession session;

  /**
   * The current transaction.
   */
  private ODBTransaction transaction;

  /**
   * The path tree.
   */
  private ODBPathNode path_tree;

  /**
   * The browser.
   */
  private ODBBrowser browser;



  public ODBViewer(String path_name,
                   ODBSession session, ODBTransaction transaction) {
    this.session = session;
    this.transaction = transaction;

    path_tree = new ODBPathNode(path_name);
    path_tree.refresh(transaction);
    browser = new ODBBrowser(transaction);
  }





  @Override
  public JComponent getObjectView(TreePath selected_path) {

    if (selected_path.getPathCount() == 4) {

      String ob_type = selected_path.getPathComponent(2).toString();
      String ob_name = selected_path.getPathComponent(3).toString();

      if (ob_type.equals("Classes")) {

        ODBClass odb_class = transaction.findClass(ob_name);

        browser.goLocation("class:" + odb_class.getInstanceName());

      }
      else if (ob_type.equals("Named Items")) {

        ODBObject named_instance = transaction.getNamedItem(ob_name);

        browser.goLocation(
                "instance:" + named_instance.getODBClass().getInstanceName() +
                ":" + named_instance.getReference().toString());

      }

    }

    return browser;

  }

  @Override
  public MutableTreeNode getPathTree() {
    return path_tree;
  }

  @Override
  public JComponent queryComponent(String dialog_name) {
    return new JLabel("No query component for Object Database yet.", JLabel.CENTER);
  }

  @Override
  public void refresh() {
    // Create a new transaction,
    transaction = session.createTransaction();

    // Refresh the path tree,
    path_tree.refresh(transaction);
    browser.refresh(transaction);
  }


}
