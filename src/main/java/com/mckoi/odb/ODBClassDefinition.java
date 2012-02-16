/**
 * com.mckoi.odb.ODBClassDefinition  Aug 3, 2010
 *
 * Mckoi Database Software ( http://www.mckoi.com/ )
 * Copyright (C) 2000 - 2010  Diehl and Associates, Inc.
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

import java.util.ArrayList;
import java.util.List;

/**
 * An object that describes the fields of an instance of a named class.
 *
 * @author Tobias Downer
 */

public final class ODBClassDefinition {

  /**
   * The name of the class.
   */
  private final String class_name;

  /**
   * The list of fields in this definition.
   */
  private ArrayList<FieldInfo> fields;

  /**
   * Constructor.
   */
  ODBClassDefinition(String class_name) {
    this.class_name = class_name;
    fields = new ArrayList(8);
  }

  /**
   * Throws an exception if this object is invalidated.
   */
  void invalidationCheck() {
    if (fields == null) {
      throw new RuntimeException("Class definition has been invalidated");
    }
  }

  /**
   * Throws an exception if the field name already defined.
   */
  private void checkNonDuplicateFieldName(String field_name) {
    for (FieldInfo finfo : fields) {
      if (finfo.getName().equals(field_name)) {
        throw new RuntimeException("Duplicate field name");
      }
    }
  }

  /**
   * Throws an exception if the class name is invalid.
   */
  private void checkClassNameValid(String class_name) {
    if (class_name.startsWith("[") ||
        class_name.contains("#") || class_name.contains(",") ||
        class_name.contains("<") || class_name.contains(">")) {

      throw new RuntimeException("Invalid class name");

    }
  }

  /**
   * Invalidates this object.
   */
  void invalidate() {
    fields = null;
  }

  /**
   * Returns the class definition.
   */
  public String getClassName() {
    return class_name;
  }

  /**
   * Returns the list of defined field names.
   */
  List<FieldInfo> getFields() {
    return fields;
  }

  // ---------- Field definition functions ----------

  /**
   * Defines a reference to an immutable object of the given class type.
   */
  public ODBClassDefinition defineMember(String field_name, String class_name) {
    return defineMember(field_name, class_name, false);
  }

  /**
   * Defines a reference to an object of the given class type. If 'mutable' is
   * true then the field may be altered in the object instance.
   */
  public ODBClassDefinition defineMember(String field_name, String class_name,
                                         boolean mutable) {
    // Invalidation check
    invalidationCheck();
    checkClassNameValid(class_name);
    checkNonDuplicateFieldName(field_name);

    fields.add(new FieldInfo(field_name, class_name, mutable));
    return this;
  }

  /**
   * Defines an immutable inlined string member on the class.
   */
  public ODBClassDefinition defineString(String field_name) {
    return defineString(field_name, false);
  }

  /**
   * Defines an inlined string member on the class. If 'mutable' is true then
   * the field may be altered in the object instance.
   */
  public ODBClassDefinition defineString(String field_name, boolean mutable) {
    // Invalidation check
    invalidationCheck();
    checkNonDuplicateFieldName(field_name);

    fields.add(new FieldInfo(field_name, "[S", mutable));
    return this;
  }

  /**
   * Defines a data element on the class.
   */
  public ODBClassDefinition defineData(String field_name) {
    // Invalidation check
    invalidationCheck();
    checkNonDuplicateFieldName(field_name);

    fields.add(new FieldInfo(field_name, "[D", false));
    return this;
  }


  /**
   * Adds a reference to a List object field type to the definition. The list
   * may only contains objects with the given 'element_class' name.
   * 'collation_spec' is the specification used for ordering the keys, and if
   * 'allowed_duplicates' is true then duplicate keys are allowed in the list
   * or false for only unique key values.
   */
  public ODBClassDefinition defineList(String field_name, String element_class,
                         ODBOrderSpecification collation_spec,
                         boolean allow_duplicates) {

    // NULL checks
    collation_spec.getClass();
    // Invalidation check
    invalidationCheck();
    // Check the element class is a valid name,
    checkClassNameValid(element_class);
    checkNonDuplicateFieldName(field_name);

    // Create the list type,
    StringBuilder list_type = new StringBuilder();
    list_type.append("[L<");
    list_type.append(element_class);
    list_type.append(">(");
    list_type.append(allow_duplicates ? "duplicates" : "unique");
    list_type.append(",");
    list_type.append(collation_spec.getMemberName());
    list_type.append(",");
    if (collation_spec.isInverse()) {
      list_type.append("-");
    }
    list_type.append(collation_spec.getOrderFunction());
    list_type.append(")");

    fields.add(new FieldInfo(field_name, list_type.toString(), false));

    return this;
  }

  /**
   * Adds a reference to a List object field type that has no order field or
   * collation spec (order is on the reference id value).
   */
  public ODBClassDefinition defineList(String field_name, String element_class,
                         boolean allow_duplicates) {

    // Invalidation check
    invalidationCheck();
    // Check the element class is a valid name,
    checkClassNameValid(element_class);
    checkNonDuplicateFieldName(field_name);

    // Create the list type,
    StringBuilder list_type = new StringBuilder();
    list_type.append("[L<");
    list_type.append(element_class);
    list_type.append(">(");
    list_type.append(allow_duplicates ? "duplicates" : "unique");
    list_type.append(")");

    fields.add(new FieldInfo(field_name, list_type.toString(), false));

    return this;
  }



//  /**
//   * Adds a reference to a Data object field type to the definition.
//   */
//  public void addDataField(String field_name, boolean mutable) {
//    fields.add(new FieldInfo(field_name, "[D", mutable));
//  }
//
//  /**
//   * Adds a reference to a class object field type to the definition.
//   */
//  public void addClassReference(String field_name, boolean mutable) {
//    addObjectReference(field_name, ODBClasses.CLASS, mutable);
//  }
//
//  /**
//   * Adds a reference to a user object field type to the definition. Note that
//   * for this definition to be successfully defined in the transaction, the
//   * referenced user object type must be found in the system class store.
//   */
//  public void addObjectReference(String field_name,
//                                 ODBClass clazz, boolean mutable) {
//    fields.add(new FieldInfo(
//                        field_name, clazz.getInstanceName(), mutable));
//  }

}
