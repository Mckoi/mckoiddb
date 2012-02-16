/**
 * com.mckoi.gui.DBPathNode  May 1, 2010
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
