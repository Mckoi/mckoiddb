/**
 * com.mckoi.odb.ListItemChangeEvent  Jan 1, 2011
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
 * 
 *
 * @author Tobias Downer
 */

class ListItemChangeEvent {

  /**
   * The reference to the list object.
   */
  private final Reference list_ref;

  /**
   * The reference to the object instance.
   */
  private final Reference object_ref;

  /**
   * The reference to the list class.
   */
  private final Reference class_ref;


  public ListItemChangeEvent(Reference list_ref, Reference object_ref,
                             Reference class_ref) {
    this.list_ref = list_ref;
    this.object_ref = object_ref;
    this.class_ref = class_ref;
  }

  public Reference getListClassReference() {
    return class_ref;
  }

  public Reference getListReference() {
    return list_ref;
  }

  public Reference getObjectReference() {
    return object_ref;
  }

}
