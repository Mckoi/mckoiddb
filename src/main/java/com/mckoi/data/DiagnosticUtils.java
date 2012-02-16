/**
 * com.mckoi.treestore.DiagnosticUtils  Dec 13, 2007
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

package com.mckoi.data;

import java.io.PrintStream;
import java.io.IOException;

/**
 * Various diagnostic, reporting and analysis utilities for TreeSystem objects.
 *
 * @author Tobias Downer
 */

public class DiagnosticUtils {

  /**
   * Outputs diagnostic information about a KeyObjectTransaction.
   */
  public static void printTransactionStatistics(
                        KeyObjectTransaction transaction) throws IOException {
    if (transaction instanceof TreeSystemTransaction) {
      ((TreeSystemTransaction) transaction).printStatistics();
    }
    else {
      System.out.println("Can't print transaction statistics, unknown type: " +
                         transaction.getClass());
    }
  }

  /**
   * Outputs the area map of a TreeSystem to a PrintOutput object.
   */
  public static void printGraph(PrintStream out,
                                TreeReportNode node) throws IOException {
    recursivePrintTS(out, node, 0);
  }

  private static void recursivePrintTS(
         PrintStream out, TreeReportNode node, int space) throws IOException {
    // Output the tabs
    for (int i = 0; i < space; ++i) {
      out.print(' ');
    }
    // Output the name and reference
    String name = node.getProperty("name");
    out.print(name);
    String key = node.getProperty("key");
    if (key != null) {
      out.print('#');
      out.print(key);
    }
    out.print('#');
    out.print(node.getProperty("ref"));
    if (name.equals("leaf")) {
      out.print(" ref_count=");
      out.print(node.getProperty("reference_count"));
      out.print(" size=");
      out.print(node.getProperty("leaf_size"));
    }
    else if (name.equals("child_meta")) {
      out.print(" extent=");
      out.print(node.getProperty("extent"));
    }
    out.println();
    // Recurse on the children
    int sz = node.getChildCount();
    for (int i = 0; i < sz; ++i) {
      recursivePrintTS(out, node.getChildAt(i), space + 2);
    }
    
  }

//  /**
//   * Given a list of all 64 bit area references (as a LongList) sorted from
//   * lowest pointer reference to highest, discovers any areas that are live but
//   * not in the tree store as represented by a TreeReportNode.  These areas
//   * are dead but not freed.
//   */
//  public static LongList discoverDeadAreas(LongList refs, TreeReportNode node)
//                                                           throws IOException {
//    // A BTree node heap
//    BTreeNodeHeap node_heap = new BTreeNodeHeap(513);
//
//    // Create a long list of all references in the tree
//    HeapBTree tree_areas = new HeapBTree(node_heap);
//    tree_areas.init();
//    // Walk the tree and add the nodes
//    addAllRefsToList(node, tree_areas);
//    
//    // Now find all the entries in 'refs' not in tree_areas
//    LongList not_in = new LongList();
//    int sz = refs.size();
//    for (int i = 0; i < sz; ++i) {
//      long v = refs.get(i);
//      // If the ref isn't in the tree, add it
//      if (!tree_areas.containsSortKey(v)) {
//        not_in.add(v);
//      }
//    }
//    // Return the list
//    return not_in;
//  }
//
//  /**
//   * Adds all the references from the node into the given list.
//   */
//  private static void addAllRefsToList(TreeReportNode node, HeapBTree list)
//                                                           throws IOException {
//    String str = node.getProperty("ref");
//    long v = Long.parseLong(str);
//    
//    // Does the list contain the sort key?
//    boolean found = list.containsSortKey(v);
//    if (found) {
//      // Don't add it to the list
//    }
//    else {
//      // Otherwise not found, so insert it
//      list.insertSortKey(v);
//    }
//    
//    // Recurse to children
//    for (int i = 0; i < node.getChildCount(); ++i) {
//      addAllRefsToList(node.getChildAt(i), list);
//    }
//
//  }

}
