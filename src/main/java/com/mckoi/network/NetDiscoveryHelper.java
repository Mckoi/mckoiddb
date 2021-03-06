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

import java.io.IOException;
import java.io.PrintWriter;
import java.net.NetworkInterface;
import java.util.Enumeration;

/**
 * Convenience classes for network discovery.
 *
 * @author Tobias Downer
 */

public class NetDiscoveryHelper {

  /**
   * Prints out all the NetworkInterface names available on this machine as
   * a list.
   * 
   * @param pout
   */
  public static void printValidInterfaces(PrintWriter pout) throws IOException {

    Enumeration<NetworkInterface> network_interfaces =
                                      NetworkInterface.getNetworkInterfaces();
    while (network_interfaces.hasMoreElements()) {
      NetworkInterface net_if = network_interfaces.nextElement();
      pout.println("  " + net_if.getName());
    }
    
  }
  
}
