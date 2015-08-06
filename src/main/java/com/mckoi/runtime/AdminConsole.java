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

import com.mckoi.network.AdminInterpreter;
import com.mckoi.network.NetDiscoveryHelper;
import com.mckoi.network.NetworkConfigResource;
import com.mckoi.network.NetworkProfile;
import com.mckoi.network.TCPConnectorValues;
import com.mckoi.util.CommandLine;
import java.io.*;
import java.net.NetworkInterface;
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
    String if_arg = null;
    boolean no_console = false;

    NetworkInterface out_net_if = null;

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
      if_arg = command_line.switchArgument("-if");

      no_console = command_line.containsSwitch("-noconsole");

      // Check arguments that can be null,
      if (network_conf_arg == null) {
        pout.println("Error, no network configuration file/url given.");
        failed = true;
      }
      if (network_pass_arg == null) {
        pout.println("Error, no network password given.");
        failed = true;
      }
//      if (if_arg == null) {
//        pout.println("Error, no network interface given.");
//        pout.println("Valid interface names on this machine:");
//        NetDiscoveryHelper.printValidInterfaces(pout);
//        failed = true;
//      }

      // Validation,
      if (!failed) {
        if (if_arg != null) {
          // Validate interface argument
          out_net_if = NetworkInterface.getByName(if_arg);
          if (out_net_if == null) {
            pout.println("Error, network interface not found: " + if_arg);
            pout.println("Valid interface names on this machine:");
            NetDiscoveryHelper.printValidInterfaces(pout);
            failed = true;
          }
        }
      }

    }
    catch (IOException e) {
      pout.println("IOException: " + e.getMessage());
      e.printStackTrace(pout);
      failed = true;
    }
    catch (Throwable e) {
      pout.println("Error parsing arguments: " + e.getMessage());
      e.printStackTrace(pout);
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
      System.out.println("-if [network interface name]");
      System.out.println("  The network interface name used to talk to the DDB");
      System.out.println("  network, necessary for routing on IPv6 networks running");
      System.out.println("  link local addresses. (eg. 'eth0')");
      System.out.println("  Optional, but necessary for IPv6 link local networks.");
      System.out.println();
      pout.flush();
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

        TCPConnectorValues properties =
                          new TCPConnectorValues(network_pass_arg, out_net_if);
        NetworkProfile network_profile =
                                   NetworkProfile.tcpConnect(properties);
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
