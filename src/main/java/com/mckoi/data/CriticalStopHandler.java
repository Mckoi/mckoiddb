/**
 * com.mckoi.treestore.CriticalStopHandler  01 Nov 2004
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

package com.mckoi.data;

/**
 * An object that handles critical stop conditions in an application.  For
 * example, if it was necessary to stop all IO operations when an IOException
 * is encountered then it may be handled by this object.
 * 
 * @author Tobias Downer
 * @deprecated this feature is no longer user defined.
 */

public interface CriticalStopHandler {

  /**
   * Throws an Error exception when a critical IO error occurs, and performs
   * any operations necessary to shut down operations on the database.
   */
  Error errorIO(java.io.IOException e) throws Error;

  /**
   * Throws an Error exception when a critical virtual machine error occurs,
   * and performs any operations necessary to shut down operations on the
   * database.  Most typically this is caused by an out of memory error.
   */
  Error errorVirtualMachine(java.lang.VirtualMachineError e) throws Error;
  
}
