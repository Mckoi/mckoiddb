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

import java.net.NetworkInterface;

/**
 * Property values used to initialize a TCP connector.
 * 
 * @author Tobias Downer
 */
public class TCPConnectorValues {

  private final String network_password;
  private final NetworkInterface output_net_interface;

  public TCPConnectorValues(
            String network_password, NetworkInterface output_net_interface) {
    this.network_password = network_password;
    this.output_net_interface = output_net_interface;
  }

  String getNetworkPassword() {
    return network_password;
  }

  NetworkInterface getOutputNetworkInterface() {
    return output_net_interface;
  }
  
}
