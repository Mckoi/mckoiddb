/**
 * com.mckoi.odb.ODBClassCreator  Oct 29, 2010
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

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Creates and prepares a set of classes to be added to a Mckoi Object
 * Database. Before completing the operation, the classes defined must be
 * validated to ensure member references are consistent. For example, a
 * member may not reference a class that doesn't exist.
 *
 * @author Tobias Downer
 */

public class ODBClassCreator {

  /**
   * The backed transaction.
   */
  private final ODBTransactionImpl transaction;

  /**
   * The list of classes created by this object.
   */
  private List<ODBClassDefinition> classes;

  /**
   * Constructor.
   */
  ODBClassCreator(ODBTransactionImpl backed_transaction) {
    this.transaction = backed_transaction;
    classes = new ArrayList(8);
  }

  /**
   * Generate an exception if this creator object has been invalidated.
   */
  private void checkValid() {
    if (classes == null) {
      throw new RuntimeException("Creator object is invalidated");
    }
  }

  /**
   * Creates a new class with the given class name. Note that this will not
   * immediately create a class that can be referenced inside a transaction.
   * Instead, the set of classes describing the object schema must first be
   * created, and then 'validateAndComplete' is called to perform the database
   * operations. This ensures that member references in a class must be
   * concrete.
   */
  public ODBClassDefinition createClass(String class_name) {
    checkValid();

    // Make sure the class name hasn't already been defined,
    for (ODBClassDefinition c : classes) {
      if (c.getClassName().equals(class_name)) {
        throw new RuntimeException(MessageFormat.format(
                "Class ''{0}'' already been defined in this creator session",
                class_name));
      }
    }

    ODBClassDefinition def = new ODBClassDefinition(class_name);
    classes.add(def);
    return def;
  }

  /**
   * Returns true if this creator has created a class with the given class
   * name.
   */
  public boolean hasCreatedClass(String class_name) {
    checkValid();
    for (ODBClassDefinition def : classes) {
      if (def.getClassName().equals(class_name)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Given a class name, fully qualifies the name. eg. Turns 'File' into
   * 'File#9a91138...(etc)'. The qualification is based first on classes
   * locally created in this creator, and then on existing classes in the
   * database.
   */
  private String resolveClassName(HashMap<String, Reference> class_name_map,
                                  String class_name) {
    Reference ref = class_name_map.get(class_name);
    if (ref == null) {
      ref = transaction.findClass(class_name).getReference();
    }
    return class_name + "#" + ref.toString();
  }

  /**
   * Validates all class definitions created by this object and if the schema
   * is valid, updates the database appropriately. After this method finishes,
   * this object and any objects created by this object are invalid. An
   * exception is generated if the validation process fails.
   */
  public void validateAndComplete() throws ClassValidationException {

    ArrayList<String> list_types = new ArrayList(4);

    try {

      // The operation takes 2 steps.
      // 1) Member types are checked to ensure they reference either classes
      //    in the current schema or created by this object,
      // 2) Class data is populated in the database,
      // 3) Member types are resolved,

      for (ODBClassDefinition def : classes) {
        List<FieldInfo> fields = def.getFields();
        for (FieldInfo field : fields) {
          String field_type = field.getType();
          if (!field_type.startsWith("[")) {
            // Check the user type references a valid class,
            if (transaction.findClass(field_type) == null &&
                !hasCreatedClass(field_type)) {
              throw new ClassValidationException(MessageFormat.format(
                                 "Member type ''{0}'' not found", field_type));
            }
          }
          else if (field_type.startsWith("[L<")) {
            String list_class_element =
                              field_type.substring(3, field_type.indexOf('>'));
            // Check the list type references a valid class,
            if (transaction.findClass(list_class_element) == null &&
                !hasCreatedClass(list_class_element)) {
              throw new ClassValidationException(MessageFormat.format(
                    "List member type ''{0}'' not found", list_class_element));
            }

          }
        }
      }

      // Create a map of class name to resolved class name for the created
      // class.
      HashMap<String, Reference> class_name_map = new HashMap();

      // Create resolved class names for all the inserted classes,
      for (ODBClassDefinition def : classes) {
        String class_name = def.getClassName();
        // Create a unique identifier for this class name,
        class_name_map.put(class_name,
                           transaction.createUniqueReference(ODBClasses.CLASS));
      }

      // Now, resolve any fields to the internal type name,
      for (ODBClassDefinition def : classes) {
        List<FieldInfo> fields = def.getFields();
        for (FieldInfo field : fields) {
          String field_type = field.getType();
          // List elements,
          if (field_type.startsWith("[L<")) {
            int delim = field_type.indexOf('>');
            String class_elem = field_type.substring(3, delim);
            class_elem = resolveClassName(class_name_map, class_elem);
            String new_field_code =
                              "[L<" + class_elem + field_type.substring(delim);
            field.setType(new_field_code);

            // Add this list to the dictionary if it's not found,
            if (!list_types.contains(new_field_code)) {
              list_types.add(new_field_code);
            }

          }
          else if (!field_type.startsWith("[")) {
            String class_elem = field_type;
            class_elem = resolveClassName(class_name_map, class_elem);
            String new_field_code = class_elem;
            field.setType(new_field_code);
          }
        }
      }

      // Define all the classes,

      // Insert the list types into the class dictionary,
      for (String list_type : list_types) {
        if (!transaction.hasClassDirectoryEntry(list_type)) {
          transaction.addToClassDictionary(list_type,
                          transaction.createUniqueReference(ODBClasses.CLASS));
        }
      }

      // Insert the class data into the db,
      for (ODBClassDefinition def : classes) {
        // We define the class at the reference previously computed,
        transaction.defineClass(def, class_name_map.get(def.getClassName()));
      }

    }
    finally {
      classes = null;
    }

  }

}
