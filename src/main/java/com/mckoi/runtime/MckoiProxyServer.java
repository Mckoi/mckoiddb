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

package com.mckoi.runtime;

import com.mckoi.network.NetDiscoveryHelper;
import com.mckoi.network.TCPInstanceProxyServer;
import com.mckoi.util.CommandLine;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;

/**
 * A runtime tool for starting a proxy server on the local machine.
 *
 * @author Tobias Downer
 */

public class MckoiProxyServer {

  public static void main(String[] args) {
    try {

      // Output standard info
      System.out.println(MckoiDDBVerInfo.displayVersionString());
      System.out.println(MckoiDDBVerInfo.license_message);

      String host_arg = null;
      String port_arg = null;
      String if_arg = null;

      StringWriter wout = new StringWriter();
      PrintWriter pout = new PrintWriter(wout);

      CommandLine command_line = new CommandLine(args);
      boolean failed = false;
      try {
        host_arg = command_line.switchArgument("-host");
        port_arg = command_line.switchArgument("-port");
        if_arg = command_line.switchArgument("-if");
      }
      catch (Throwable e) {
        pout.println("Error parsing arguments.");
        failed = true;
      }
      // Check arguments that can be null,
      if (port_arg == null) {
        pout.println("Error, no port address given.");
        failed = true;
      }
      if (if_arg == null) {
        pout.println("Error, no network interface given.");
        pout.println("Valid interface names on this machine:");
        NetDiscoveryHelper.printValidInterfaces(pout);
        failed = true;
      }

      NetworkInterface out_net_if = null;
      if (!failed) {
        // Validate interface argument
        out_net_if = NetworkInterface.getByName(if_arg);
        if (out_net_if == null) {
          pout.println("Error, network interface not found: " + if_arg);
          pout.println("Valid interface names on this machine:");
          NetDiscoveryHelper.printValidInterfaces(pout);
          failed = true;
        }
      }
      
      pout.flush();

      // If failed,
      if (failed) {
        System.out.println("com.mckoi.runtime.MckoiProxyServer command line arguments");
        System.out.println();
        System.out.println("-host [bind address]");
        System.out.println("  The interface address to bind the socket on the local");
        System.out.println("  machine (optional - if not given binds to localhost)");
        System.out.println("-port [port]");
        System.out.println("  The port to bind the socket.");
        System.out.println("-if [interface name]");
        System.out.println("  The interface name (eg. 'eth0') used to connect to the");
        System.out.println("  DDB network.");
        System.out.println();
        System.out.println(wout.toString());
      }
      else {

        InetAddress host;
        if (host_arg != null) {
          host = InetAddress.getByName(host_arg);
        }
        else {
          host = null;
        }

        int port;
        try {
          port = Integer.parseInt(port_arg);
        }
        catch (Throwable e) {
          System.out.println("Error: couldn't parse port argument: " + port_arg);
          return;
        }
        

        System.out.println("Proxy service started " + host + " port: " + port);
        TCPInstanceProxyServer proxy_server =
                            new TCPInstanceProxyServer(host, port, out_net_if);
        proxy_server.run();
      }

    }
    catch (UnknownHostException e) {
      e.printStackTrace();
    }
    catch (IOException e) {
      e.printStackTrace();
    }

  }

}
