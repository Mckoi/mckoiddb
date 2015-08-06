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

import java.io.IOException;
import com.mckoi.store.AreaWriter;

/**
 * A leaf node of a tree in a TreeStore.  A leaf node is some serialization
 * of data stored in the tree.  A leaf may represent data inside one or more
 * key elements.
 * 
 * @author Tobias Downer
 */

public abstract class TreeLeaf implements TreeNode {

  public TreeLeaf() {
  }

  /**
   * Returns the address of this node.  If the address is less than 0 then the
   * node is located on the mutable node heap and this object is mutable.  If
   * the address is greater or equal to 0 then the node is immutable and in the
   * store.
   */
  public abstract NodeReference getReference();

  /**
   * Returns the size of the node.  The size is the number of bytes stored in
   * the node.
   */
  public abstract int getSize();

  /**
   * Returns the capacity of this node in bytes.  This is only useful when
   * the leaf is a mutable leaf on the heap.
   */
  public abstract int getCapacity();

  /**
   * Fully retrieves a section of the data content of this leaf node and copies
   * it to the byte array.
   */
  public abstract void get(int position, byte[] buf, int off, int len)
                                                            throws IOException;

  /**
   * Reads a single byte from this leaf node from the given position.
   */
  public abstract byte get(int position) throws IOException;

  /**
   * Writes the data part of this leaf to the given Area.
   */
  public abstract void writeDataTo(AreaWriter area) throws IOException;

  /**
   * Shifts all the data from the given position in the leaf forward by the
   * given amount and changes the size accordingly.  If the new size exceeds
   * the capacity then an exception is generated.
   * <p>
   * This method is only applicable for heap nodes.
   */
  public abstract void shift(int position, int offset) throws IOException;

  /**
   * Writes data into this leaf at the given position and changes the size of
   * leaf as necessary.  If the new size exceeds the capacity then an exception
   * is generated.  If there is data already in the leaf at the given position
   * then it is overwritten.
   * <p>
   * This method is only applicable for heap nodes.
   */
  public abstract void put(int position, byte[] buf, int off, int len)
                                                            throws IOException;

  /**
   * Sets the size of this leaf node.  If the size set is smaller than the
   * current size then the node, and in effect the data composed of the nodes,
   * is reduced in size.  If the size is increased then this node and the data
   * composed of the nodes increases in size.  The size can not be set higher
   * than the capacity.
   * <p>
   * This method is only applicable for heap nodes.
   */
  public abstract void setSize(int size) throws IOException;

}

