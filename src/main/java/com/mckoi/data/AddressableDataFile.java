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
 * A DataFile that is addressable in a KeyObjectDatabase.
 *
 * @author Tobias Downer
 */

public interface AddressableDataFile extends DataFile {

  /**
   * Returns an object that describes a fully qualified addressable location
   * of the data representing this DataFile from the start position
   * (inclusive) to the end position (exclusive). The object returned is
   * entirely implementation specific, and is used by the 'copyFrom' and
   * 'replicateFrom' methods to optimize the block replication methods.
   * Calling 'df.getBlockLocationMeta(0, df.size())' will return a meta object
   * describing the complete file content.
   * <p>
   * This method may return null, which signifies that the content of the
   * file is not eligible to be replicated using a block replication method.
   * For example, a DataFile that has content entirely on the Java Heap
   * should return null for this method.
   * <p>
   * For security reasons, the object returned should not expose information
   * about the data being addressed.
   */
  Object getBlockLocationMeta(long start_position, long end_position);

}
