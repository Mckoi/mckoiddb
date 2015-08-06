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
 * Encapsulates a transaction event that changed an object. Used by ObjectLog.
 *
 * @author Tobias Downer
 */

class ObjectChangeEvent {

  /**
   * The reference to the class of the object.
   */
  private final Reference class_ref;

  /**
   * The reference to the object instance itself.
   */
  private final Reference object_ref;

  /**
   * Constructor.
   */
  ObjectChangeEvent(Reference class_ref, Reference object_ref) {
    this.class_ref = class_ref;
    this.object_ref = object_ref;
  }

  Reference getClassReference() {
    return class_ref;
  }

  Reference getObjectReference() {
    return object_ref;
  }

  @Override
  public String toString() {
    return object_ref.toString();
  }

}
