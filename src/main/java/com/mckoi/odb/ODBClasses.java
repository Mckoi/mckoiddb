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

final class ODBClasses {

//  /**
//   * The primitive class for a Reference.
//   */
//  public static final ODBClass REFERENCE;

//  /**
//   * The primitive class for a ODBString
//   */
//  public static final ODBClass STRING;

//  /**
//   * The primitive class for a ODBData
//   */
//  public static final ODBClass DATA;

  /**
   * The primitive class for a ODBClass
   */
  static final ODBClass CLASS;

  /**
   * The primitive class for named items
   */
  static final ODBClass NAMER;

  static {
    SimpleClass c;

//    // Reference instances can not be created so have no fields (the class
//    // object for Reference is a marker).
//    c = new SimpleClass("System Reference", "$Reference");
//    c.setImmutable();
//    REFERENCE = c;

//    // A string object contains a single inline string primitive
//    c = new SimpleClass("System String", "$String");
//    c.addField("val", "[S", false);
//    c.setImmutable();
//    STRING = c;

//    // Data objects have a system key field only
//    c = new SimpleClass("System Data", "$Data");
//    c.addField("Mresource", "[S", false);
//    c.setImmutable();
//    DATA = c;

    // Class objects have a name and specification field describing the class
    c = new SimpleClass("System Class", "$Class",
                        ODBTransactionImpl.SYS_CLASS_REFERENCE);
    c.addField("name", "[S", false);
    c.addField("serialization", "[S", false);
    c.setImmutable();
    CLASS = c;

    // Class objects have a name and specification field describing the class
    c = new SimpleClass("System Class", "$Namer",
                        ODBTransactionImpl.SYS_NAMER_REFERENCE);
    c.addField("name", "[S", false);
    c.addField("class_ref", "[S", false);
    c.addField("ref", "[S", false);
    c.setImmutable();
    NAMER = c;

  }



//  /**
//   * Creates and returns a class that describes a list.
//   */
//  static ODBClass createListClass(String resolved_classname) {
//
//    // Create the instance name.
//    // Below is an example of a list instance,
//    //    "$List<Person#001593vira>"
//    // This describes a list of people.
//    StringBuilder b = new StringBuilder();
//    b.append("$List<");
//    b.append(resolved_classname);
//    b.append(">");
//
//    SimpleClass list_class = new SimpleClass("System List", b.toString());
//
//    // The com.mckoi.data.Key object for this list.
//    list_class.addField("Mresource", "[D", false);
//    // The key in the list class being ordered against, or null if ordered by
//    // reference.
//    list_class.addField("key_field", "[S", false);
//    // The specification of the key collation, or null if ordered by reference.
//    list_class.addField("collation_spec", "[S", false);
//    // 'true' if duplicates allowed in list, 'false' otherwise.
//    list_class.addField("duplicates", "[S", false);
//    list_class.setImmutable();
//
//    return list_class;
//  }
//
//  /**
//   * Creates and returns a class that describes a user defined object.
//   */
//  static ODBClass createUserObject(String name, String instance_name) {
//    return new SimpleClass(name, instance_name);
//  }

}
