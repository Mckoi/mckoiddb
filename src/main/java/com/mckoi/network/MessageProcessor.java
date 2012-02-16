/**
 * com.mckoi.network.MessageProcessor  Nov 30, 2008
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

/**
 * An object that processes a sequential message stream in order, and returns a
 * stream that contains the replies of the processed operations.
 *
 * @author Tobias Downer
 */

public interface MessageProcessor {

  /**
   * Processes the given message string, and returns a MessageStream that
   * contains the replies of the operations in the same order as the received.
   */
  ProcessResult process(MessageStream message_stream);

}
