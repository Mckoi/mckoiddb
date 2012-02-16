/**
 * com.mckoi.network.CommitFaultException  Jun 23, 2009
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

import java.text.MessageFormat;

/**
 * An exception that indicates changes made by concurrent transactions are in
 * conflict and a consistent model of a proposed change can not be published.
 * For example, two transactions deleting the same row in a table would cause
 * a CommitFaultException to be thrown.
 *
 * @author Tobias Downer
 */

public class CommitFaultException extends Exception {

  /**
   * Constructs the commit fault exception with a message. The message should
   * describe the reason for the fault in a way a human could understand.
   */
  public CommitFaultException(String msg) {
    super(msg);
  }

  /**
   * Constructs the commit fault exception with a formatted message and
   * arguments. The message should describe the reason for the fault in a way
   * a human could understand.
   */
  public CommitFaultException(String msg, Object ... args) {
    this(MessageFormat.format(msg, args));
  }

//  private static String format(String msg, Object[] args) {
//
//
//    for (int i = 0; i < args.length; ++i) {
//      msg = msg.replace("%" + i, args[i].toString());
//    }
//    return msg;
//  }

}
