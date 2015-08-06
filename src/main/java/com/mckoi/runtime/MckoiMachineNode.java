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

import com.mckoi.network.NetworkConfigResource;
import com.mckoi.network.TCPInstanceAdminServer;
import com.mckoi.util.CommandLine;
import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

/**
 * The command-line invocation class that starts a Mckoi Distributed node.
 *
 * @author Tobias Downer
 */

public class MckoiMachineNode {

  public static void main(String[] args) {
    try {

      // Output standard info
      System.out.println(MckoiDDBVerInfo.displayVersionString());
      System.out.println(MckoiDDBVerInfo.license_message);

      String node_config = null;
      String net_config = null;
      String host_arg = null;
      String port_arg = null;

      StringWriter wout = new StringWriter();
      PrintWriter pout = new PrintWriter(wout);

      CommandLine command_line = new CommandLine(args);
      boolean failed = false;
      try {

        // Fetch the location of the 'network.conf' file, either by reading
        // it from the 'netconfig' switch or dereferencing it from the
        // 'netconfinfo' location.
        String network_config_val = command_line.switchArgument("-netconfig");
        String netconf_info_val = command_line.switchArgument("-netconfinfo");
        if (netconf_info_val != null) {
          Properties nci = new Properties();
          FileReader r = new FileReader(new File(netconf_info_val));
          nci.load(r);
          net_config = nci.getProperty("netconf_location");
          r.close();
        }
        else if (network_config_val != null) {
          net_config = network_config_val;
        }
        else {
          net_config = "network.conf";
        }
        
        node_config = command_line.switchArgument("-nodeconfig", "node.conf");
        host_arg = command_line.switchArgument("-host");
        port_arg = command_line.switchArgument("-port");
      }
      catch (Throwable e) {
        pout.println("Error parsing arguments.");
        failed = true;
      }
      // Check arguments that can be null,
      if (net_config == null) {
        pout.println("Error, no network configuration given.");
        failed = true;
      }
      else if (node_config == null) {
        pout.println("Error, no node configuration file given.");
        failed = true;
      }
      if (host_arg == null) {
        pout.println("Error, no host (bind address) given.");
        failed = true;
      }
      if (port_arg == null) {
        pout.println("Error, no port address given.");
        failed = true;
      }

      pout.flush();

      // If failed,
      if (failed) {
        System.out.println("com.mckoi.runtime.MckoiMachineNode command line arguments");
        System.out.println();
        System.out.println("-nodeconfig [filename]");
        System.out.println("  The node configuration file (default: node.conf).");
        System.out.println("-netconfig [filename or URL]");
        System.out.println("  The network configuration file (default: network.conf).");
        System.out.println("-host [host]");
        System.out.println("  The interface address to bind the socket on the local");
        System.out.println("  machine.");
        System.out.println("-port [port]");
        System.out.println("  The port to bind the socket.");
        System.out.println();
        System.out.println(wout.toString());
      }
      else {
        // Get the node configuration file,
        Properties node_config_resource = new Properties();
        FileInputStream fin = new FileInputStream(new File(node_config));
        node_config_resource.load(new BufferedInputStream(fin));
        fin.close();

        // Parse the network configuration string,
        NetworkConfigResource net_config_resource =
                                       NetworkConfigResource.parse(net_config);

        // The base path,
        InetAddress host;
        if (host_arg == null) throw new NullPointerException();
        host = InetAddress.getByName(host_arg);

        int port;
        try {
          port = Integer.parseInt(port_arg);
        }
        catch (Throwable e) {
          System.out.println("Error: couldn't parse port argument: " + port_arg);
          return;
        }

        host_arg = host_arg + " ";
        System.out.println("Machine Node, " + host_arg +
                           "port: " + port_arg);
        TCPInstanceAdminServer inst =
              new TCPInstanceAdminServer(net_config_resource,
                                         host, port, node_config_resource);
        inst.run();
      }

    }
    catch (UnknownHostException e) {
      e.printStackTrace(System.err);
    }
    catch (IOException e) {
      e.printStackTrace(System.err);
    }

  }

}
