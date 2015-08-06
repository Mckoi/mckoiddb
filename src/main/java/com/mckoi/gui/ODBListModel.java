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

package com.mckoi.gui;

import com.mckoi.odb.ODBClass;
import com.mckoi.odb.ODBObject;
import com.mckoi.odb.Reference;

/**
 * A simple model for a list of ODB objects.
 *
 * @author Tobias Downer
 */

public interface ODBListModel {

  /**
   * The number of elements in the list. The returned value may be truncated
   * to Integer.MAX_VALUE.
   */
  int size();

  /**
   * Returns the ODBClass that represents the elements of the list.
   */
  ODBClass getElementClass();

  /**
   * Returns the Reference to the nth element in the list.
   */
  Reference getElementReference(int n);

  /**
   * Returns the ODBObject of the nth element in the list.
   */
  ODBObject getElement(int n);

}
