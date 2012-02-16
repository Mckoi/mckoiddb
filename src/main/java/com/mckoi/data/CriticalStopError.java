/**
 * com.mckoi.data.CriticalStopError  Nov 14, 2008
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
 * An error caused by a critical stop state of the database, such as a
 * faulty disk/full disk or running out of memory in the VM.  Any condition
 * that may detriment the state of any existing persistent state will cause a
 * database implementation to generate this exception on all access until the
 * VM running the system is shut down, the problem fixed, and the system
 * restarted.
 *
 * @author Tobias Downer
 */

public class CriticalStopError extends Error {

  /**
   * The Error constructor.
   */
  public CriticalStopError(String message, Throwable parent) {
    super(message, parent);
  }

}
