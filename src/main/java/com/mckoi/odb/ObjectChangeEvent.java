/**
 * com.mckoi.odb.ObjectChangeEvent  Nov 9, 2010
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
 * Encapsulates a transaction event that changed an object. Used by ObjectLog.
 *
 * @author Tobias Downer
 */

class ObjectChangeEvent {

  /**
   * The reference to the class of the object.
   */
  private final Reference class_ref;

  /**
   * The reference to the object instance itself.
   */
  private final Reference object_ref;

  /**
   * Constructor.
   */
  ObjectChangeEvent(Reference class_ref, Reference object_ref) {
    this.class_ref = class_ref;
    this.object_ref = object_ref;
  }

  Reference getClassReference() {
    return class_ref;
  }

  Reference getObjectReference() {
    return object_ref;
  }

  @Override
  public String toString() {
    return object_ref.toString();
  }

}
