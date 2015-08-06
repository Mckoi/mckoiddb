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

package com.mckoi.odb;

/**
 * Encapsulates a transaction event that changed the content of a list. Used
 * by ObjectLog.
 *
 * @author Tobias Downer
 */

class ListChangeEvent {

  /**
   * The reference to the list that changed.
   */
  private Reference list_reference;

  ListChangeEvent(Reference list_reference) {
    this.list_reference = list_reference;
  }

  public Reference getListReference() {
    return list_reference;
  }

}
