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
