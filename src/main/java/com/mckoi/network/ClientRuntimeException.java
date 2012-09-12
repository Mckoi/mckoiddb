/**
 * com.mckoi.network.ClientRuntimeException  Sep 11, 2012
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
 * A client-side runtime exception that may have resulted from an external
 * throwable error (an exception that happened on a remote server).
 *
 * @author Tobias Downer
 */

public class ClientRuntimeException extends RuntimeException {

  /**
   * The external throwable (may be null).
   */
  private ExternalThrowable external_throwable = null;
  
  /**
   * Constructors.
   */
  public ClientRuntimeException(Throwable cause) {
    super(cause);
  }

  public ClientRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }

  public ClientRuntimeException(String message) {
    super(message);
  }

  public ClientRuntimeException() {
  }

  /**
   * Sets the external throwable.
   */
  void setExternalThrowable(ExternalThrowable et) {
    external_throwable = et;
  }

  /**
   * The ExternalThrowable if it has been set.
   */
  public ExternalThrowable getExternalThrowable() {
    return external_throwable;
  }

}
