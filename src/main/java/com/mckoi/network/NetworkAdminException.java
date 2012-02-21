/**
 * com.mckoi.network.NetworkAdminException  Jul 4, 2009
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

import java.io.PrintStream;
import java.io.PrintWriter;

/**
 * An exception generated when an administration command fails (in
 * com.mckoi.network.NetworkProfile).
 *
 * @author Tobias Downer
 */

public class NetworkAdminException extends Exception {

  private ExternalThrowable external_throwable;
  
  public NetworkAdminException(Message m) {
    super(m.getErrorMessage());
    external_throwable = m.getExternalThrowable();
  }

  public NetworkAdminException(String msg) {
    super(msg);
    external_throwable = null;
  }

  /**
   * Returns an ExternalThrowable if this error was generated by a remote
   * exception, or returns null if this exception is the source.
   */
  public ExternalThrowable getExternalThrowable() {
    return external_throwable;
  }

  /**
   * Prints the error string + stack trace to the print stream.
   */
  public void printAdminError(PrintStream out) {
    printStackTrace(out);
    if (external_throwable != null) {
      out.println("Caused from server exception;");
      out.println(external_throwable.getStackTrace());
    }
  }

  /**
   * Prints the error string + stack trace to the print stream.
   */
  public void printAdminError(PrintWriter out) {
    printStackTrace(out);
    if (external_throwable != null) {
      out.println("Caused from server exception;");
      out.println(external_throwable.getStackTrace());
    }
  }

}
