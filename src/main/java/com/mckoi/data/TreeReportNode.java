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
    properties = new HashMap<>(4);
    children = new ArrayList<>(12);
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
    properties.put(key, Long.toString(value));
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
