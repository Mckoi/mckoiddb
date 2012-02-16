/**
 * com.mckoi.treestore.TreeReportNode  Dec 13, 2007
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

import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * A tree node that represents a graph of all areas stored in the tree, used
 * for diagnostic purposes.
 *
 * @author Tobias Downer
 */

public class TreeReportNode {

  /**
   * The properties hashmap.
   */
  private HashMap<String, String> properties;
  
  /**
   * The children of this node.
   */
  private ArrayList<TreeReportNode> children;
  
  /**
   * Constructor.
   */
  public TreeReportNode() {
    properties = new HashMap<String, String>(4);
    children = new ArrayList<TreeReportNode>(12);
  }

  public TreeReportNode(String node_name, NodeReference area_ref) {
    this();
    init(node_name, area_ref);
  }

  public TreeReportNode(String node_name, long area_ref) {
    this();
    init(node_name, area_ref);
  }

  // ----- Property methods -----
  
  /**
   * Sets the name and area reference properties.
   */
  public void init(String node_name, NodeReference area_ref) {
    setProperty("name", node_name);
    setProperty("ref", area_ref.toString());
  }

  public void init(String node_name, long area_ref) {
    setProperty("name", node_name);
    setProperty("ref", area_ref);
  }

  /**
   * Sets a property.
   */
  public void setProperty(String key, String value) {
    properties.put(key, value);
  }

  public void setProperty(String key, long value) {
    properties.put(key, new Long(value).toString());
  } 
  /**
   * Gets a property.
   */
  public String getProperty(String key) {
    return properties.get(key);
  }
  
  // ----- Tree methods -----

  /**
   * Adds a child to this node.
   */
  public void addChild(TreeReportNode node) {
    children.add(node);
  }

  /**
   * Returns the number of children to this node.
   */
  public int getChildCount() {
    return children.size();
  }
  
  /**
   * Returns the child TreeReportNode at the given index.
   */
  public TreeReportNode getChildAt(int i) {
    return children.get(i);
  }
  
  /**
   * Returns an Enumeration of the children.
   */
  public Iterator<TreeReportNode> children() {
    return children.iterator();
  }
  
  // ----- Output -----
  
  public String toString() {
    return getProperty("name") + "[" + getProperty("ref") + "]";
  }

}
