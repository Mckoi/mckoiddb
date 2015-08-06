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
