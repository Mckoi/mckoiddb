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

/**
 * An object used by clients to connect to services in the Mckoi network.
 *
 * @author Tobias Downer
 */

public interface NetworkConnector {

  /**
   * Stops this connector, invalidating it and putting any resources it
   * used up for GC.
   */
  public void stop();
  
  /**
   * Connects to the instance administration component of the given address.
   */
  public MessageProcessor connectInstanceAdmin(ServiceAddress address);
  
  /**
   * Connects to a block server at the given address.
   */
  public MessageProcessor connectBlockServer(ServiceAddress address);

  /**
   * Connects to a manager server at the given address.
   */
  public MessageProcessor connectManagerServer(ServiceAddress address);

  /**
   * Connects to a root server at the given address.
   */
  public MessageProcessor connectRootServer(ServiceAddress address);
  
}
