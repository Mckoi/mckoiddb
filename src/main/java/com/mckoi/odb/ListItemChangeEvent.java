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
 * 
 *
 * @author Tobias Downer
 */

class ListItemChangeEvent {

  /**
   * The reference to the list object.
   */
  private final Reference list_ref;

  /**
   * The reference to the object instance.
   */
  private final Reference object_ref;

  /**
   * The reference to the list class.
   */
  private final Reference class_ref;


  public ListItemChangeEvent(Reference list_ref, Reference object_ref,
                             Reference class_ref) {
    this.list_ref = list_ref;
    this.object_ref = object_ref;
    this.class_ref = class_ref;
  }

  public Reference getListClassReference() {
    return class_ref;
  }

  public Reference getListReference() {
    return list_ref;
  }

  public Reference getObjectReference() {
    return object_ref;
  }

}
