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
 * An implementation of ODBListModel for a list with a single element which
 * is the given object.
 *
 * @author Tobias Downer
 */

public class ODBInstanceListModel implements ODBListModel {

  /**
   * The object.
   */
  private final ODBObject obj;

  /**
   * Constructor.
   */
  public ODBInstanceListModel(ODBObject obj) {
    this.obj = obj;
  }

  // -----

  public ODBObject getElement(int n) {
    if (n != 0) {
      throw new RuntimeException("Element out of range.");
    }
    return obj;
  }

  public ODBClass getElementClass() {
    return obj.getODBClass();
  }

  public Reference getElementReference(int n) {
    return getElement(n).getReference();
  }

  public int size() {
    return 1;
  }

}
