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
 * An object that describes the ordering specification of items stored in an
 * OrderedReferenceList.
 *
 * @author Tobias Downer
 */

class OrderSpec {

  /**
   * A string that describes the collation order of the keys, or null if
   * this specifies order by reference value.
   */
  private final String key_collation_description;

  /**
   * The class of objects that are referenced by this spec.
   */
  private final ODBClass object_class;

  /**
   * The field number in the referenced object where the order key can be
   * found. This field must point to an inlined primitive immutable string
   * in the referenced object.
   */
  private final int key_field;

  /**
   * True if the spec allows duplicates.
   */
  private final boolean allows_duplicates;

  /**
   * Constructor for an OrderSpec that orders by reference value.
   */
  OrderSpec(boolean allows_duplicates, ODBClass object_class) {
    this.object_class = object_class;
    this.key_field = 0;
    this.key_collation_description = null;
    this.allows_duplicates = allows_duplicates;
  }

  /**
   * Constructor for an order specification on the given field index in the
   * referenced object with the given key collation.
   */
  OrderSpec(boolean allows_duplicates,
            ODBClass object_class, int key_field_index,
            String key_collation_description) {
    this.object_class = object_class;
    this.key_field = key_field_index;
    this.key_collation_description = key_collation_description;
    this.allows_duplicates = allows_duplicates;
  }

  /**
   * Return true if the collation is by reference value. This ordering type
   * requires no external lookup when values are inserted into the list.
   */
  boolean orderedByReferenceValue() {
    return (key_collation_description == null);
  }

  /**
   * Returns the description of the collation used on the inline string key
   * in the referenced object to determine ordering. This is only defined if
   * 'orderedByReferenceValue' is false.
   */
  String getKeyCollationDescription() {
    return key_collation_description;
  }

  /**
   * The object class.
   */
  ODBClass getODBClass() {
    return object_class;
  }

  /**
   * The field number in the referenced object of the inline string to order
   * against. This is only defined if 'orderedByReferenceValue' is false.
   */
  int getKeyFieldIndex() {
    return key_field;
  }

  /**
   * Returns true if duplicates are allowed by this spec.
   */
  boolean allowsDuplicates() {
    return allows_duplicates;
  }

}
