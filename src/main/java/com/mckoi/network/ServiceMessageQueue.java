/**
 * com.mckoi.network.ServiceMessageQueue  Jun 22, 2010
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

import java.util.ArrayList;

/**
 * A queue for messages sent to services.
 *
 * @author Tobias Downer
 */

public abstract class ServiceMessageQueue {

  /**
   * The message queue.
   */
  protected final ArrayList<ServiceAddress> service_addresses;
  protected final ArrayList<MessageStream> messages;
  protected final ArrayList<String> types;


  protected ServiceMessageQueue() {

    service_addresses = new ArrayList(4);
    messages = new ArrayList(4);
    types = new ArrayList(4);

  }

  /**
   * Adds a message stream and type to the queue.
   */
  public void addMessageStream(ServiceAddress service_address,
                               MessageStream message_stream,
                               String message_type) {

    service_addresses.add(service_address);
    messages.add(message_stream);
    types.add(message_type);

  }

  

  /**
   * Enqueues messages that failed to be processed.
   */
  public abstract void enqueue();

}
