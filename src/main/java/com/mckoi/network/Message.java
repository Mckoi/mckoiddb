/**
 * com.mckoi.network.Message  Nov 30, 2008
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

package com.mckoi.network;

/**
 * An immutable message object.
 *
 * @author Tobias Downer
 */

public interface Message {

  /**
   * The message name.
   */
  String getName();

  /**
   * The number of arguments in the message.
   */
  int count();

  /**
   * Returns argument n from the parameters list.
   */
  Object param(int n);

  /**
   * Returns true if this message represents an error reply.
   */
  boolean isError();
  
  /**
   * Returns the error message if this is an error reply.
   */
  String getErrorMessage();

  /**
   * Returns the error as an ExternalThrowable object.
   */
  ExternalThrowable getExternalThrowable();

}
