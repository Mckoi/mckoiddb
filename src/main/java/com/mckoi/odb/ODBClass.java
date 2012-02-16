/**
 * com.mckoi.odb.ODBClass  Aug 2, 2010
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
