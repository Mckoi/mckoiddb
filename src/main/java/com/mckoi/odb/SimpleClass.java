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
