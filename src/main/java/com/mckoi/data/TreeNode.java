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

/**
 * Represents a node in the tree.
 * 
 * @author Tobias Downer
 */

public interface TreeNode {

  /**
   * Returns the address of this node.  If the address is less than 0 then the
   * node is located on the mutable node heap and this object is mutable.  If
   * the address is greater or equal to 0 then the node is immutable and in the
   * store.
   */
  NodeReference getReference();

  /**
   * Returns a heap size estimate for the consumption of this tree node on
   * the Java Heap. This is used to estimate how much memory a cache of tree
   * nodes consumes. The calculation of this value should be fairly accurate,
   * being an overestimate if unsure.
   */
  int getHeapSizeEstimate();

}


