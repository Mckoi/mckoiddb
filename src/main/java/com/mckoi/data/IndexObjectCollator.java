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
 * A comparator used in a search on an index that compares an Object with a
 * 64-bit index reference and determines if the Object value is larger, smaller
 * or the same as the referenced value. This typically is used as a device to
 * dereference a 64-bit key value into a collatable object in another
 * structure to build and query an index.
 *
 * @author Tobias Downer
 */

public interface IndexObjectCollator {

  /**
   * Compares a value at some specified reference and an Object value, and
   * returns a value &gt; 0 if the referenced value is greater, &lt; 0 if the
   * referenced value is less than, or == 0 if the referenced value is the same.
   */
  int compare(long ref, Object val);

}

