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

package com.mckoi.network;

import com.mckoi.data.NodeReference;
import java.io.InputStream;

/**
 * Accessor for a binary node item.
 *
 * @author Tobias Downer
 */

public interface NodeItemBinary {

  /**
   * Returns the nodeid value of the item.
   */
  NodeReference getNodeId();

  /**
   * Returns an input stream that allows iteration through the item. Only one
   * input stream can be made per binary item. The input stream needs to be
   * read to completion if 'asBinary' returns null.
   */
  InputStream getInputStream();

  /**
   * Returns the item as a byte[] array. This returns null if the node item
   * is compressed and the current item is unknown.
   */
  byte[] asBinary();

}
