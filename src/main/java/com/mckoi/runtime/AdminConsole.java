/**
 * com.mckoi.runtime.AdminConsole  Jul 4, 2009
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

package com.mckoi.runtime;

import com.mckoi.network.AdminInterpreter;
import com.mckoi.network.NetworkConfigResource;
import com.mckoi.network.NetworkProfile;
import com.mckoi.util.CommandLine;
import java.io.*;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An administration console for a MckoiDDB network.
 *
 * @author Tobias Downer
 */

public class AdminConsole {


  public static void main(String[] args) {

    // Output standard info
    System.out.println(MckoiDDBVerInfo.displayVersionString());
    System.out.println(MckoiDDBVerInfo.license_message);

    // Turn logging off for the console app,
    Logger logger = Logger.getLogger("com.mckoi.network.Log");
    logger.setLevel(Level.OFF);

    String network_conf_arg = null;
    String network_pass_arg = null;
    boolean no_console = false;

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
        network_conf_arg = nci.getProperty("netconf_location");
        r.close();
      }
      else if (network_config_val != null) {
        network_conf_arg = network_config_val;
      }
      else {
        network_conf_arg = "network.conf";
      }
      
      network_pass_arg = command_line.switchArgument("-netpassword");
      no_console = command_line.containsSwitch("-noconsole");
    }
    catch (Throwable e) {
      pout.println("Error parsing arguments.");
      failed = true;
    }
    // Check arguments that can be null,
    if (network_conf_arg == null) {
      pout.println("Error, no network configuration file/url given.");
      failed = true;
    }
    if (network_pass_arg == null) {
      pout.println("Error, no network password given.");
      failed = true;
    }

    if (failed) {

      System.out.println("com.mckoi.runtime.AdminConsole command line arguments");
      System.out.println();
      System.out.println("-netconfig [local file or URL]");
      System.out.println("  Either a path or URL of the location of the network ");
      System.out.println("  configuration file (default: network.conf).");
      System.out.println("-netconfinfo [local file]");
      System.out.println("  A file that contains a 'netconf_location' property that is ");
      System.out.println("  either a URL or File location of the network.conf resource.");
      System.out.println("    (The command line arguments must have either a -netconfig");
      System.out.println("     or -netconfinfo argument)");
      System.out.println("-netpassword [password]");
      System.out.println("  The challenge password used in all connection handshaking");
      System.out.println("  throughout the Mckoi network. All machines must have the");
      System.out.println("  same net password.");
      System.out.println();
      System.out.println(wout.toString());

    }
    else {

      System.out.println();

      Reader r;
      Writer w;
      Console console = System.console();
      boolean display_prompt;
      if (!no_console && console != null) {
        r = console.reader();
        w = console.writer();
        display_prompt = true;
      }
      else {
        r = new InputStreamReader(System.in);
        w = new OutputStreamWriter(System.out);
        display_prompt = false;
      }

      try {
        NetworkConfigResource network_conf_resource =
                                NetworkConfigResource.parse(network_conf_arg);

        NetworkProfile network_profile =
                                   NetworkProfile.tcpConnect(network_pass_arg);
        network_profile.setNetworkConfiguration(network_conf_resource);

        AdminInterpreter interpreter =
                     new AdminInterpreter(r, w, network_profile,
                                          display_prompt);
        interpreter.process();

      }
      catch (IOException e) {
        e.printStackTrace(System.err);
      }
    }

  }

}
