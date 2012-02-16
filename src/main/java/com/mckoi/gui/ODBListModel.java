/**
 * com.mckoi.gui.ODBListModel  Feb 6, 2011
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
 * A simple model for a list of ODB objects.
 *
 * @author Tobias Downer
 */

public interface ODBListModel {

  /**
   * The number of elements in the list. The returned value may be truncated
   * to Integer.MAX_VALUE.
   */
  int size();

  /**
   * Returns the ODBClass that represents the elements of the list.
   */
  ODBClass getElementClass();

  /**
   * Returns the Reference to the nth element in the list.
   */
  Reference getElementReference(int n);

  /**
   * Returns the ODBObject of the nth element in the list.
   */
  ODBObject getElement(int n);

}
