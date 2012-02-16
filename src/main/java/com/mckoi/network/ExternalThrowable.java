/**
 * com.mckoi.network.ExternalThrowable  Nov 30, 2008
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

package com.mckoi.network;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * An exception that happened in an external service (eg. an error that
 * occurred during the process of a query on a server).
 *
 * @author Tobias Downer
 */

public class ExternalThrowable {

  /**
   * The class of the thrown exception.
   */
  private String class_name;
  
  /**
   * The message string of the exception.
   */
  private String message;
  
  /**
   * The stack trace of the exception as reported by 'printStackTrace'.
   */
  private String stack_trace;
  
  /**
   * Constructs the external throwable.
   */
  ExternalThrowable(Throwable t) {
    this.class_name = t.getClass().getName();
    this.message = t.getMessage();
    StringWriter w = new StringWriter();
    PrintWriter pw = new PrintWriter(w);
    t.printStackTrace(pw);
    pw.flush();
    this.stack_trace = w.toString();
  }

  /**
   * Full argument constructor.
   */
  ExternalThrowable(String class_name, String message, String stack_trace) {
    this.class_name = class_name;
    this.message = message;
    this.stack_trace = stack_trace;
  }
  
  /**
   * Returns the class of the external exception.
   */
  public String getClassName() {
    return class_name;
  }
  
  /**
   * Returns the external exception message.
   */
  public String getMessage() {
    return message;
  }
  
  /**
   * Returns the external exception stack trace.
   */
  public String getStackTrace() {
    return stack_trace;
  }

  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("[External Throwable: ");
    b.append(getClassName());
    b.append(" message: '");
    b.append(getMessage());
    b.append("']");
    return b.toString();
  }

}
