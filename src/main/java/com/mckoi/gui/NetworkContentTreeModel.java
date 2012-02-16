/**
 * com.mckoi.sdb.gui.NetworkContentTreeModel  Jul 12, 2009
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

import com.mckoi.sdb.SDBTransaction;
import javax.swing.tree.*;

/**
 * Models the meta-data content of a Simple Database (the set of files and
 * tables) as a TreeModel for use in a graphical user interface. Note that
 * although this implementation uses MutableTableNode to represent the nodes
 * in the tree, the model is not mutable.
 *
 * @author Tobias Downer
 */

public class NetworkContentTreeModel extends DefaultTreeModel {

  /**
   * The root node.
   */
  private final DefaultMutableTreeNode root_node;


  /**
   * Constructor.
   */
  public NetworkContentTreeModel(String root_title) {
    super(null);

    root_node = new DefaultMutableTreeNode(root_title) {
      public boolean isLeaf() {
        return false;
      }
    };



    setRoot(root_node);
  }

  public NetworkContentTreeModel(SDBTransaction t) {
    this("Network");
  }

  /**
   * Adds a generic node object to the tree.
   */
  public void addPath(MutableTreeNode node) {
    root_node.add(node);
  }

  /**
   * Adds a SimpleDatabase path into the tree model.
   */
  public void addSimpleDatabasePath(SDBPathNode path) {
    addPath(path);
  }

  public void refresh(MutableTreeNode node) {
    this.nodeStructureChanged(node);
  }

}
