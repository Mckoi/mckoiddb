/**
 * com.mckoi.runtime.MckoiMachineNode  Dec 20, 2008
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

package com.mckoi.runtime;

import com.mckoi.network.NetworkConfigResource;
import com.mckoi.network.TCPInstanceAdminServer;
import com.mckoi.util.CommandLine;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
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
        node_config = command_line.switchArgument("-nodeconfig", "node.conf");
        net_config = command_line.switchArgument("-netconfig", "network.conf");
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
        System.out.println("  machine (optional - if not given binds to all interfaces)");
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

        if (host_arg != null) {
          host_arg = host_arg + " ";
        }
        System.out.println("Machine Node, " + (host_arg != null ? host_arg : "") +
                            "port: " + port_arg);
        TCPInstanceAdminServer inst =
              new TCPInstanceAdminServer(net_config_resource,
                                         host, port, node_config_resource);
        inst.run();
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
