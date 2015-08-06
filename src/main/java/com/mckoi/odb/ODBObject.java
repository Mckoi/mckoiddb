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
