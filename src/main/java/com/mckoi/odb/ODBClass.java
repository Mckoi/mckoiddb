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
 * A class in the Mckoi Object database data model. The class object 
 * describes either a system primitive type, or a used defined class
 * specification. A class specification is a number of named fields and their
 * corresponding type that make up the fields in an object instance.
 * <p>
 * A class object is immutable and has a corresponding reference to the
 * class object in the storage medium.
 *
 * @author Tobias Downer
 */

public interface ODBClass extends ODBReferenced {

  /**
   * Returns the name of this class.
   */
  String getName();

  /**
   * Returns the instance name of this class, which is a globally unique
   * identifier used to resolve the class details against the object
   * database.
   */
  String getInstanceName();

  /**
   * Returns the number of fields defined for this class.
   */
  int getFieldCount();

  /**
   * Returns the name of field n.
   */
  String getFieldName(int n);

  /**
   * Returns a string describing the type of field n, which is either;
   * "[S", "$Data", "$String", "$List", "$Class",
   * "[Class Name]#[UID]". All of these identifiers represent references
   * except for "[S" that indicates an inline string. For
   * "[Class Name]#[UID]" types, the "[Class Name]#[UID]" part
   * describes the name of the user class and the reference of the object
   * stored.
   */
  String getFieldType(int n);

  /**
   * Returns true if the field is mutable, false if immutable. A mutable
   * field may be changed in an instance of the class. However a
   * mutable field may not be indexed in an object collection class (only
   * immutable values may be used in an index).
   */
  boolean isFieldMutable(int n);

  /**
   * Returns the index of the field with the given name.
   */
  int indexOfField(String field_name);

}
