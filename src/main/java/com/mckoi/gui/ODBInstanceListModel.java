/**
 * com.mckoi.gui.ODBInstanceListModel  Feb 6, 2011
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

package com.mckoi.gui;

import com.mckoi.odb.ODBClass;
import com.mckoi.odb.ODBObject;
import com.mckoi.odb.Reference;

/**
 * An implementation of ODBListModel for a list with a single element which
 * is the given object.
 *
 * @author Tobias Downer
 */

public class ODBInstanceListModel implements ODBListModel {

  /**
   * The object.
   */
  private final ODBObject obj;

  /**
   * Constructor.
   */
  public ODBInstanceListModel(ODBObject obj) {
    this.obj = obj;
  }

  // -----

  public ODBObject getElement(int n) {
    if (n != 0) {
      throw new RuntimeException("Element out of range.");
    }
    return obj;
  }

  public ODBClass getElementClass() {
    return obj.getODBClass();
  }

  public Reference getElementReference(int n) {
    return getElement(n).getReference();
  }

  public int size() {
    return 1;
  }

}
