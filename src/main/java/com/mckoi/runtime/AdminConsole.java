/**
 * com.mckoi.runtime.AdminConsole  Jul 4, 2009
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

import com.mckoi.network.AdminInterpreter;
import com.mckoi.network.NetworkConfigResource;
import com.mckoi.network.NetworkProfile;
import com.mckoi.util.CommandLine;
import java.io.Console;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
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

    StringWriter wout = new StringWriter();
    PrintWriter pout = new PrintWriter(wout);

    CommandLine command_line = new CommandLine(args);
    boolean failed = false;
    try {
      network_conf_arg = command_line.switchArgument("-netconfig", "network.conf");
      network_pass_arg = command_line.switchArgument("-netpassword");
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
      if (console != null) {
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
//        File schema_dir = new File(base_path_arg, "admin");
//        if (!schema_dir.exists()) {
//          schema_dir.mkdir();
//        }
//        else if (!schema_dir.isDirectory()) {
//          throw new RuntimeException(
//                       "Can't make network schema directory: " + schema_dir);
//        }
//
//        File schema_file = new File(schema_dir, "network_schema");
//        // If the file doesn't exist, create it
//        if (!schema_file.exists()) {
//          schema_file.createNewFile();
//        }

        NetworkProfile network_profile =
                                   NetworkProfile.tcpConnect(network_pass_arg);
        network_profile.setNetworkConfiguration(network_conf_resource);

//        FileReader schema_in = new FileReader(schema_file);
//        network_profile.readNetworkSchema(schema_in);
//        schema_in.close();
        AdminInterpreter interpreter =
                     new AdminInterpreter(r, w, network_profile,
                                          display_prompt);
        interpreter.process();

      }
      catch (IOException e) {
        e.printStackTrace();
      }
    }

  }

}
