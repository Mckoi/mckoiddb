/**
 * com.mckoi.gui.PathViewer  Apr 9, 2010
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
