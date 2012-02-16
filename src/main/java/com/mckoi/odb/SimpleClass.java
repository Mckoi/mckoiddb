/**
 * com.mckoi.odb.SimpleClass  Aug 4, 2010
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

import java.util.ArrayList;

/**
 * An implementation of ODBClass that is a Java memory resident (fully
 * materialized) class definition. Used to implement system classes.
 *
 * @author Tobias Downer
 */

class SimpleClass implements ODBClass {

  /**
   * The name of the class.
   */
  private final String class_name;

  /**
   * The class instance name.
   */
  private final String class_instance_name;

  /**
   * The db reference of this class if one exists, or null if not.
   */
  private final Reference reference;

  /**
   * An array of FieldInfo objects describing each field in the class.
   */
  private final ArrayList<FieldInfo> fields;

  private boolean class_mutable = true;


  SimpleClass(String class_name, String class_instance_name,
              Reference reference) {
    this.class_name = class_name;
    this.class_instance_name = class_instance_name;
    this.reference = reference;
    this.fields = new ArrayList(8);
  }

  SimpleClass(String class_name, String class_instance_name) {
    this(class_name, class_instance_name, null);
  }

  SimpleClass(String class_name) {
    this(class_name, "$" + class_name, null);
  }

  void addField(String field_name, String field_type, boolean mutable) {
    if (!class_mutable) {
      throw new RuntimeException("Not mutable");
    }
    fields.add(new FieldInfo(field_name, field_type, mutable));
  }

  void setImmutable() {
    class_mutable = false;
  }


  // -----

  public int getFieldCount() {
    return fields.size();
  }

  public String getFieldName(int n) {
    return fields.get(n).getName();
  }

  public String getFieldType(int n) {
    return fields.get(n).getType();
  }

  public String getInstanceName() {
    return class_instance_name;
  }

  public String getName() {
    return class_name;
  }

  public int indexOfField(String field_name) {
    int sz = fields.size();
    for (int i = 0; i < sz; ++i) {
      if (fields.get(i).getName().equals(field_name)) {
        return i;
      }
    }
    return -1;
  }

  public boolean isFieldMutable(int n) {
    return fields.get(n).isMutable();
  }

  public ODBClass getODBClass() {
    return ODBClasses.CLASS;
  }

  public Reference getReference() {
    return reference;
  }


  public String toString() {
    return getInstanceName();
  }

}
