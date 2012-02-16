/**
 * com.mckoi.gui.ODBPathNode  Feb 4, 2011
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

import com.mckoi.odb.ODBTransaction;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import javax.swing.tree.MutableTreeNode;
import javax.swing.tree.TreeNode;

/**
 * 
 *
 * @author Tobias Downer
 */

public class ODBPathNode extends DBPathNode {

  /**
   * The current transaction.
   */
  private ODBTransaction current_transaction;


  /**
   * Constructs the Object Database path node.
   */
  public ODBPathNode(String path_name) {
    super(path_name);
  }

  @Override
  public boolean isLeaf() {
    return false;
  }

  public void refresh(ODBTransaction new_transaction) {
    removeAllChildren();

    current_transaction = new_transaction;

    NamedItemsTreeNode classes_node =
               new NamedItemsTreeNode("Classes",
                                      current_transaction.getClassNamesList());
    NamedItemsTreeNode named_items_node =
               new NamedItemsTreeNode("Named Items",
                                      current_transaction.getNamedItemsList());

    add(classes_node);
    add(named_items_node);

  }

  public ODBTransaction getCurrentTransaction() {
    return current_transaction;
  }

  @Override
  public String getPathType() {
    return "Object DB";
  }


  private static class ItemEnumeration implements Enumeration {

    private final TreeNode parent;
    private final Iterator<String> i;
    private int cur_pos = 0;

    ItemEnumeration(TreeNode parent, Iterator<String> i) {
      this.parent = parent;
      this.i = i;
    }

    public boolean hasMoreElements() {
      ++cur_pos;
      return i.hasNext();
    }

    public Object nextElement() {
      return new ItemNode(parent, i.next(), cur_pos);
    }

  }

  private static class ItemNode implements TreeNode {

    private final String title;
    private final TreeNode parent;
    private final int index;

    ItemNode(TreeNode parent, String title, int index) {
      this.parent = parent;
      this.title = title;
      this.index = index;
    }

    public Enumeration children() {
      throw new UnsupportedOperationException();
    }

    public boolean getAllowsChildren() {
      return true;
    }

    public TreeNode getChildAt(int childIndex) {
      throw new UnsupportedOperationException();
    }

    public int getChildCount() {
      return 0;
    }

    public int getIndex(TreeNode node) {
      return 0;
    }

    public TreeNode getParent() {
      return parent;
    }

    public boolean isLeaf() {
      return true;
    }

    int getIndex() {
      return index;
    }

    public String toString() {
      return title;
    }

  }



  private static class NamedItemsTreeNode implements MutableTreeNode {

    private final String title;
    private MutableTreeNode parent;
    private final List<String> items;

    NamedItemsTreeNode(String title, List<String> items) {
      this.title = title;
      this.items = items;
    }


    public Enumeration children() {
      return new ItemEnumeration(this, items.iterator());
    }

    public boolean getAllowsChildren() {
      return true;
    }

    public TreeNode getChildAt(int childIndex) {
      return new ItemNode(this, items.get(childIndex), childIndex);
    }

    public int getChildCount() {
      return items.size();
    }

    public int getIndex(TreeNode node) {
      ItemNode item_node = (ItemNode) node;
      if (item_node.getParent() == this) {
        return item_node.getIndex();
      }
      else {
        int i = item_node.getIndex();
        if (i >= 0 && i < getChildCount()) {
          if (getChildAt(i).toString().equals(node.toString())) {
            return i;
          }
        }
        return -1;
      }
    }

    public TreeNode getParent() {
      return parent;
    }

    public boolean isLeaf() {
      return false;
    }

    public void insert(MutableTreeNode child, int index) {
      throw new UnsupportedOperationException();
    }

    public void remove(int index) {
      throw new UnsupportedOperationException();
    }

    public void remove(MutableTreeNode node) {
      throw new UnsupportedOperationException();
    }

    public void removeFromParent() {
      throw new UnsupportedOperationException();
    }

    public void setParent(MutableTreeNode newParent) {
      parent = newParent;
    }

    public void setUserObject(Object object) {
      throw new UnsupportedOperationException();
    }

    public String toString() {
      return title;
    }

  }

}
