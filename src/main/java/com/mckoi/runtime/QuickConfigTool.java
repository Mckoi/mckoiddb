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

import com.mckoi.network.*;
import java.io.*;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This tool is a convenience that installs a single node installation of
 * MckoiDDB in a directory. The installation starts a manager, root and block
 * server, and creates some default paths.
 *
 * @author Tobias Downer
 */

public class QuickConfigTool {

  /**
   * Completes the given template and outputs it to the file.
   */
  private static void writeTemplate(
                  String template_name, Map<String, String> properties,
                  PrintWriter file_out) throws IOException {

    InputStream template_in =
                      QuickConfigTool.class.getResourceAsStream(template_name);
    BufferedReader in =
               new BufferedReader(new InputStreamReader(template_in, "UTF-8"));

    Pattern VAR_PATTERN = Pattern.compile("\\$\\{(\\S+)\\}");
    
    String line = in.readLine();
    while (line != null) {
      // Make any variable substitutions,
      while (true) {
        Matcher matcher = VAR_PATTERN.matcher(line);
        if (matcher.find()) {
          String var = matcher.group(1);
          String val = properties.get(var);
          if (val == null) {
            throw new RuntimeException("Variable " + var + " not found.");
          }
          line = line.replace("${" + var + "}", val);
        }
        else {
          break;
        }
      }
      // Output the line,
      file_out.println(line);

      line = in.readLine();
    }

  }

  /**
   * Installs the network.conf, client.conf and node.conf files in the given
   * directory location.
   */
  public static void installConfigFiles(
                                  File path, Map<String, String> properties) {

    // The config files,
    File node_conf = new File(path, "node.conf");
    File client_conf = new File(path, "client.conf");
    File network_conf = new File(path, "network.conf");

    // Check none of the files exist,
    if (node_conf.exists() || client_conf.exists() || network_conf.exists()) {
      throw new RuntimeException("Configuration file already exists.");
    }

    try {
      PrintWriter file_out;
      // Write out the node configuration,
      file_out = new PrintWriter(new FileOutputStream(node_conf));
      writeTemplate("template_node_conf.txt", properties, file_out);
      file_out.close();
      // Write out the network configuration,
      file_out = new PrintWriter(new FileOutputStream(network_conf));
      writeTemplate("template_network_conf.txt", properties, file_out);
      file_out.close();
      // Write out the client configuration,
      file_out = new PrintWriter(new FileOutputStream(client_conf));
      writeTemplate("template_client_conf.txt", properties, file_out);
      file_out.close();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    // Done!

  }

  
  /**
   * Create the machine node from the configuration files at the given path.
   */
  public static TCPInstanceAdminServer createMachineNode(
                              File path, String this_host_string, int port)
                                                           throws IOException {

    // Create the file objects,
    File node_config_file = new File(path, "node.conf");
    File network_config_file = new File(path, "network.conf");

    // Get the node configuration file,
    Properties node_config_properties = new Properties();
    FileInputStream fin = new FileInputStream(node_config_file);
    node_config_properties.load(new BufferedInputStream(fin));
    fin.close();

    // Parse the network configuration string,
    NetworkConfigResource net_config_resource =
                      NetworkConfigResource.getNetConfig(network_config_file);

    // The base path,
    InetAddress host;
    if (this_host_string != null) {
      host = InetAddress.getByName(this_host_string);
    }
    else {
      host = null;
    }

//    String host_arg;
//    if (this_host_string != null) {
//      host_arg = this_host_string + " ";
//    }
//    else {
//      host_arg = "";
//    }
//    System.out.println("Machine Node, " + host_arg +
//                        "port: " + port);

    // Disable logging to system.err
    node_config_properties.setProperty("log_use_parent_handlers", "no");

    return new TCPInstanceAdminServer(net_config_resource,
                                      host, port, node_config_properties);

  }
  
  /**
   * Installs the configuration files in the given directory.
   */
  public static void quickConfigOperation(PrintWriter out,
          File path, String net_password,
          int tcp_port, boolean create_default_paths) throws IOException {

    // The node address
    String this_node_address = "127.0.0.1:" + tcp_port;
    
    // Fill out the properties,
    HashMap<String, String> properties = new HashMap();

    File node_directory = new File(path, "base");
    File log_directory = new File(path, "log");

    String ndir_str = node_directory.getCanonicalPath();
    String ldir_str = log_directory.getCanonicalPath();
    if (!ndir_str.endsWith(File.separator)) {
      ndir_str = ndir_str + File.separator;
    }
    if (!ldir_str.endsWith(File.separator)) {
      ldir_str = ldir_str + File.separator;
    }

    // Escape windows separators
    ndir_str = ndir_str.replace("\\", "\\\\");
    ldir_str = ldir_str.replace("\\", "\\\\");

    // --- node.conf ---
    properties.put("NETWORK_PASSWORD", net_password);
    properties.put("NODE_DIRECTORY", ndir_str);
    properties.put("LOG_DIRECTORY", ldir_str);
    properties.put("LOG_LEVEL", "info");
    properties.put("ROOT_SERVER_TRANSACTION_CACHE", "default");

    // --- network.conf ---
    // Allowed IP addresses
    properties.put("CONNECT_WHITELIST", "127.0.0.1");
    // Allowed nodes (server address + ":" + port)
    properties.put("NETWORK_NODELIST", this_node_address);
    properties.put("CONFIGCHECK_TIMEOUT", "120");

    // --- client.conf ---
    properties.put("MANAGER_ADDRESS", this_node_address);
    properties.put("NETWORK_PASSWORD", net_password);
    properties.put("TRANSACTION_CACHE_SIZE", "14MB");
    properties.put("GLOBAL_CACHE_SIZE", "64MB");

    out.println();

    // Install the configuration files,
    installConfigFiles(path, properties);
    out.println("DONE: Wrote configuration files.");

    // Start a machine node,
    TCPInstanceAdminServer inst =
                               createMachineNode(path, "127.0.0.1", tcp_port);
    // Start the node on its own thread,
    new Thread(inst).start();
    out.println("DONE: Starting machine node.");

    try {
      // Wait until the instance is started,
      inst.waitUntilStarted();

      File client_conf_file = new File(path, "client.conf");
      File network_conf_file = new File(path, "network.conf");

      // Connect to the machine node and setup as appropriate,
      MckoiDDBClient client = MckoiDDBClientUtils.connectTCP(client_conf_file);
      NetworkProfile net_profile = client.getNetworkProfile(null);

      // Set the network configuration from file,
      NetworkConfigResource net_config_resource =
                         NetworkConfigResource.getNetConfig(network_conf_file);
      net_profile.setNetworkConfiguration(net_config_resource);

      // The localhost service address,
      ServiceAddress this_service_addr =
                                ServiceAddress.parseString(this_node_address);
      // Register manager, block and root server,
      out.println("-- STARTING MANAGER ROLE --");
      net_profile.startManager(this_service_addr);
      net_profile.registerManager(this_service_addr);
      out.println("-- STARTING ROOT ROLE --");
      net_profile.startRoot(this_service_addr);
      net_profile.registerRoot(this_service_addr);
      out.println("-- STARTING BLOCK ROLE --");
      net_profile.startBlock(this_service_addr);
      net_profile.registerBlock(this_service_addr);

      // Do we add the default paths?
      if (create_default_paths) {
        out.println("-- ADDING DEFAULT PATHS --");
        out.println("-- ADDING PATH simpledb01 (SimpleDatabase) --");
        net_profile.addPathToNetwork(
                "simpledb01", "com.mckoi.sdb.SimpleDatabase",
                this_service_addr, new ServiceAddress[] { this_service_addr });
        out.println("-- ADDING PATH objectdb01 (ObjectDatabase) --");
        net_profile.addPathToNetwork(
                "objectdb01", "com.mckoi.odb.ObjectDatabase",
                this_service_addr, new ServiceAddress[] { this_service_addr });
      }

    }
    catch (NetworkAdminException e) {
      throw new RuntimeException(e);
    }
    finally {
      // Close the server,
      inst.close();
      inst.waitUntilStopped();
    }

  }


  
  // ----- Command line stuff -----
  
  private static String prompt(Scanner scanner, String prompt_string) {
    System.out.print(prompt_string);
    System.out.print(": ");
    return scanner.nextLine();
  }

  private static String prompt(Scanner scanner,
                               String prompt_string, String default_val) {
    System.out.print(prompt_string);
    System.out.print(" [default = ");
    System.out.print(default_val);
    System.out.print("] : ");
    String val = scanner.nextLine();
    if (val.length() == 0) {
      val = default_val;
    }
    return val;
  }

  public static void main(String[] args) {

    // Output standard info
    System.out.println(MckoiDDBVerInfo.displayVersionString());
    System.out.println(MckoiDDBVerInfo.license_message);

    System.out.println();

    try {

      boolean failed = false;
      if (args != null && args.length == 1) {

        String path = args[0];

        File path_f = new File(path);
        if (!path_f.exists()) {
          path_f.mkdirs();
        }

        if (!path_f.isDirectory()) {
          System.out.println("ERROR: directory doesn't exist.");
          System.out.println();
          return;
        }

        Scanner scanner = new Scanner(System.in);

        // Read from the console input,
        System.out.println("Welcome to the MckoiDDB Quick Configuration Tool.");
        System.out.println("This tool will install and configure a single node installation in the given");
        System.out.println("directory. First off, please enter a network password to use.");
        System.out.println();

        String net_pass;
        do {
          net_pass = prompt(scanner, " Enter network password");
        }
        while (net_pass.length() == 0);

        System.out.println();
        System.out.println("Now I need to know the localhost TCP port the server will be run on.");
        System.out.println();

        String tcp_port;
        do {
          tcp_port = prompt(scanner, " TCP port", "3500");
          try {
            int port_val = Integer.parseInt(tcp_port);
            if (port_val < 100 || port_val >= 65535) {
              tcp_port = "";
            }
          }
          catch (NumberFormatException e) {
            tcp_port = "";
          }
        }
        while (tcp_port.length() == 0);
        System.out.println(" Using port : " + tcp_port);

        System.out.println();
        System.out.println("Do you want to create default paths? If yes, the paths created are;");
        System.out.println(" 1. com.mckoi.sdb.SimpleDatabase simpledb01");
        System.out.println(" 2. com.mckoi.sdb.ObjectDatabase objectdb01");
        System.out.println();

        String create_default_paths;
        do {
          create_default_paths = prompt(scanner, " Create default paths? (yes/no)", "yes");
          if (create_default_paths.equals("y")) {
            create_default_paths = "yes";
          }
          if (create_default_paths.equals("n")) {
            create_default_paths = "no";
          }
          if (!create_default_paths.equals("yes") &&
              !create_default_paths.equals("no")) {
            create_default_paths = "";
          }
        }
        while (create_default_paths.length() == 0);
        if (create_default_paths.equals("yes")) {
          System.out.println(" OK, I'll create some default paths.");
        }

        PrintWriter sys_out = new PrintWriter(System.out, true);

        // Ok, good to go,
        quickConfigOperation(sys_out,
                            path_f, net_pass,
                            Integer.parseInt(tcp_port),
                            create_default_paths.equals("yes"));

        sys_out.println();
        sys_out.println("DONE: Installing single node MckoiDDB instance.");
        sys_out.println();
        sys_out.println("----");
        sys_out.println();
        sys_out.println("The following are some tools you can now use from the installed directory.");
        sys_out.println("NOTE: You must start the Mckoi machine node before being able to use the");
        sys_out.println("  installed database.");
        sys_out.println();
        sys_out.println("To start the MckoiDDB machine node, use the command;");
        sys_out.println(" java -cp [location of MckoiDDB jar] com.mckoi.runtime.MckoiMachineNode \\");
        sys_out.println("                  -host 127.0.0.1 -port " + tcp_port);
        sys_out.println();
        sys_out.println("For the MckoiDDB console, use the command;");
        sys_out.println(" java -cp [location of MckoiDDB jar] com.mckoi.runtime.AdminConsole \\");
        sys_out.println("                  -netpassword " + net_pass);
        sys_out.println();
        sys_out.println("For the database viewer, use the command;");
        sys_out.println(" java -cp [location of MckoiDDB jar] com.mckoi.runtime.DBViewer \\");
        sys_out.println("                  client.conf");


      }
      else {
        failed = true;
      }

      if (failed) {
        System.out.println("Syntax: QuickConfigTool [path]");
        System.out.println();
        System.out.println(
          " Creates a default single node installation at the given path together with");
        System.out.println(
          " any relevant configuration files.");
      }

    }
    catch (IOException e) {
      e.printStackTrace();
    }

  }



}
