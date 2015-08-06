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

import com.mckoi.data.Key;

/**
 * An object that represents a Key that has been allocated to a Reference.
 *
 * @author Tobias Downer
 */

class KeyAllocation {

  private Key key;
  private Reference ref;

  public KeyAllocation(Key key, Reference ref) {
    this.key = key;
    this.ref = ref;
  }

  public Key getKey() {
    return key;
  }

  public Reference getRef() {
    return ref;
  }

  @Override
  public String toString() {
    return key.toString() + "->" + ref.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final KeyAllocation other = (KeyAllocation) obj;
    if (this.key != other.key && (this.key == null || !this.key.equals(other.key))) {
      return false;
    }
    if (this.ref != other.ref && (this.ref == null || !this.ref.equals(other.ref))) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 31 * hash + (this.key != null ? this.key.hashCode() : 0);
    hash = 31 * hash + (this.ref != null ? this.ref.hashCode() : 0);
    return hash;
  }

}
