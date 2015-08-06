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

import javax.swing.JComponent;
import javax.swing.tree.MutableTreeNode;
import javax.swing.tree.TreePath;

/**
 * A GUI interface for a database path that provides a tree model node that
 * lists all the interesting objects stored in the path, and a panel element
 * for viewing and optionally editing the selected items.
 *
 * @author Tobias Downer
 */

public interface PathViewer {

  /**
   * Refreshes any information by creating a new transation and updating the
   * information returned by the query methods. This performs an in situation
   * replacement of information in the returned objects.
   */
  void refresh();

  /**
   * Returns a MutableTreeNode object containing textual description of all
   * the interesting objects stored in this path in contextual order. There
   * may be any number of sub-categories.
   */
  MutableTreeNode getPathTree();

  /**
   * Returns a JComponent that displays details of the selected item.
   */
  JComponent getObjectView(TreePath selected_path);

  /**
   * Returns a gui component the presents an interface for querying this path.
   */
  JComponent queryComponent(String dialog_name);

}
