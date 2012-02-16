/**
 * com.mckoi.sdb.ODBObject  Aug 2, 2010
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

/**
 * An object within the Mckoi Object Database API. An
 * ODBObject contains field values based on the object's class specification
 * (ODBClass). An object's field value is accessed via a call to 'getxxx'. If
 * the field of an object is mutable, it may be changed via a call to a
 * 'setxxx' method.
 *
 * @author Tobias Downer
 */

public interface ODBObject extends ODBReferenced {

  /**
   * The number of fields stored in this object. This number matches with the
   * object's class (eg. 'getODBClass().getFieldCount()')
   */
  public int size();

  // ----- Getters -----

  /**
   * Returns the string at field position 'i' in this object.
   */
  public String getString(int i);

  /**
   * Returns the string with the given field name in this object.
   */
  public String getString(String field_name);

//  public Reference getReference(int i);
//  public Reference getReference(String field_name);

  /**
   * Returns the ODBObject at field position 'i' in this object.
   */
  public ODBObject getObject(int i);

  /**
   * Returns the ODBObject with the given field name in this object.
   */
  public ODBObject getObject(String field_name);

  /**
   * Returns the ODBList at field position 'i' in this object.
   */
  public ODBList getList(int i);

  /**
   * Returns the ODBList with the given field name in this object.
   */
  public ODBList getList(String field_name);

  /**
   * Returns the ODBData at field position 'i' in this object.
   */
  public ODBData getData(int i);

  /**
   * Returns the ODBData with the given field name in this object.
   */
  public ODBData getData(String field_name);

  // ----- Setters -----

  /**
   * Sets the string at field position 'i' in this object.
   */
  public void setString(int i, String str);

  /**
   * Sets the value of the given field name in this object to the given
   * string.
   */
  public void setString(String field_name, String str);

//  public void setReference(int i, Reference ref);
//  public void setReference(String field_name, Reference ref);

  /**
   * Sets the object at field position 'i' in this object.
   */
  public void setObject(int i, ODBObject obj);

  /**
   * Sets the value of the given field name in this object to the given
   * ODBObject.
   */
  public void setObject(String field_name, ODBObject obj);

//  public void setList(int i, ODBList list);
//  public void setList(String field_name, ODBList list);
//
//  public void setData(int i, ODBData data);
//  public void setData(String field_name, ODBData data);

}
