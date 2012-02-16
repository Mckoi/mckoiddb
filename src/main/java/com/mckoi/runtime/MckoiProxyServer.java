/**
 * com.mckoi.runtime.MckoiProxyServer  Jul 19, 2009
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

import com.mckoi.network.TCPInstanceProxyServer;
import com.mckoi.util.CommandLine;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
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

      StringWriter wout = new StringWriter();
      PrintWriter pout = new PrintWriter(wout);

      CommandLine command_line = new CommandLine(args);
      boolean failed = false;
      try {
        host_arg = command_line.switchArgument("-host");
        port_arg = command_line.switchArgument("-port");
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
                                       new TCPInstanceProxyServer(host, port);
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
