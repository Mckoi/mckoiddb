/**
 * com.mckoi.odb.OrderSpec  Aug 3, 2010
 *
 * Mckoi Database Software ( http://www.mckoi.com/ )
 * Copyright (C) 2000 - 2012  Diehl and Associates, Inc.
 *
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 3 as published by
 * the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License version 3
 * along with this program.  If not, see ( http://www.gnu.org/licenses/ ) or
 * write to the Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA  02111-1307, USA.
 *
 * Change Log:
 *
 *
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
