/*
 * Mckoi Software ( http://www.mckoi.com/ )
 * Copyright (C) 2000 - 2015  Diehl and Associates, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
