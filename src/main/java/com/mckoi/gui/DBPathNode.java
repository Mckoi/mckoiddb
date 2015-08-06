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

import java.util.Enumeration;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.TreeNode;

/**
 * 
 *
 * @author Tobias Downer
 */

public class DBPathNode extends DefaultMutableTreeNode {

  public DBPathNode(String name) {
    super(name);
  }

  public String getPathType() {
    return "";
  }

  @Override
  public int getIndex(TreeNode node) {

    Enumeration<TreeNode> child = children();
    int i = 0;
    while (child.hasMoreElements()) {
      TreeNode n = child.nextElement();
      if (n.toString().equals(node.toString())) {
        return i;
      }
      ++i;
    }

    return -1;
  }

}
