/**
 * com.mckoi.runtime.MckoiNetworkSimulation  May 15, 2009
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

import com.mckoi.network.TCPInstanceAdminServer;
import com.mckoi.network.NetworkConfigResource;
import com.mckoi.network.ServiceAddress;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Properties;
import java.io.File;
import java.io.IOException;
import java.io.FileReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.UIManager;
import javax.swing.UIManager.LookAndFeelInfo;
import javax.swing.UnsupportedLookAndFeelException;

/**
 * A runtime class that starts a MckoiDDB simluation of a Mckoi distributed
 * network with the machine nodes running on different ports of the local
 * machine.
 *
 * @author Tobias Downer
 */

public class MckoiNetworkSimulation {

  /**
   * The main method takes the location of the configuration file as the
   * argument.
   */
  public static void main(String[] args) {

    // Output standard info
    System.out.println(MckoiDDBVerInfo.displayVersionString());
    System.out.println(MckoiDDBVerInfo.license_message);

    boolean failure = false;
    String fail_msg = "";
    File prop_file = null;
    // If the number of arguments is not correct,
    if (args.length != 1) {
      failure = true;
      fail_msg = "Incorrect number of arguments.";
    }
    else {
      // Check the property file exists, etc.
      prop_file = new File(args[0]);
      if (!prop_file.exists() || prop_file.isDirectory()) {
        failure = true;
        fail_msg = "Property file not found: " + prop_file.getAbsolutePath();
      }
      else {
        // File exists,
        System.out.println("Loading property file: " + prop_file.getAbsolutePath());
        System.out.println();
        Properties p = new Properties();
        try {
          p.load(new FileReader(prop_file));
          process(p);
        }
        catch (IOException e) {
          System.out.println("IOException: " + e.getMessage());
        }
        catch (PError e) {
          System.out.println(e.getMessage());
        }
      }
    }

    // If we failed,
    if (failure) {
      System.out.println(
         "com.mckoi.runtime.MckoiNetworkSimulation [configuration file name]");
      System.out.println();
      System.out.println("  Reads the configuration properties from [configuration file name] and");
      System.out.println("  sets up simulation of a Mckoi network on the local machine. See the");
      System.out.println("  documentation for details of the configuration file.");
      System.out.println();
      System.out.println(fail_msg);
    }
  }

  /**
   * Fetch an integer value from a property.
   */
  private static int getIntegerValue(Properties p, String property_key) {
    String key_v = p.getProperty(property_key);
    if (key_v == null) {
      throw new PError("Error: '" + property_key + "' integer property not found.");
    }
    try {
      return Integer.parseInt(key_v);
    }
    catch (NumberFormatException e) {
      throw new PError("Error: '" + key_v + "' integer parse error.");
    }
  }

  /**
   * Fetch a string value that's a password from the properties file.
   */
  private static String getPasswordValue(Properties p, String property_key) {
    String key_v = p.getProperty(property_key);
    if (key_v == null) {
      throw new PError("Error: '" + property_key + "' password property not found.");
    }
    // Sanity check on the password,
    if (key_v.length() > 2 && key_v.length() < 256) {
      return key_v;
    }
    throw new PError("Error: '" + key_v + "' password parse error.");
  }

  /**
   * Fetch the local host address from the properties file.
   */
  private static InetAddress getLocalHostValue(Properties p, String property_key) {
    String key_v = p.getProperty(property_key);
    try {
      // If the key value isn't found, we default to the local host,
      if (key_v == null) {
        // Loopback address if no value defined,
        return InetAddress.getByName("127.0.0.1");
      }
      // Turn the string into a local host and check it,
      InetAddress inet = InetAddress.getByName(key_v);
      return inet;
    }
    catch (UnknownHostException e) {
      throw new PError("Error: Unknown host: '" + key_v + "'.");
    }
  }

  /**
   * Fetch the path for a node from the properties file.
   */
  private static File getPathValue(Properties p, String property_key) {
    String key_v = p.getProperty(property_key);

    // If the key value isn't found, we report the error
    if (key_v == null) {
      throw new PError("Error: '" + property_key + "' path not found.");
    }
    // Turn it into a File object,
    File f = new File(key_v);
    // If it's a file, then report the error,
    if (f.exists() && f.isFile()) {
      throw new PError("Error: '" + key_v +
                       "' (property '" + property_key + "') " +
                       "is a file and should be a directory path.");
    }
    // If it doesn't exist, check it can be easily created,
    if (!f.exists()) {
      File parent = f.getParentFile();
      if (parent == null) {
        // This means file is the root, which is an error,
        throw new PError("Error: '" + key_v + "' is the root directory.");
      }
      // Check the parent directory exists,
      if (!parent.exists() || !parent.isDirectory()) {
        throw new PError("Error: parent path of '" + key_v +
                         "' doesn't exist or is not a directory.");
      }
    }
    // Otherwise, it exists as a directory or it can be easily created,
    return f;
  }

  /**
   * Process the properties file and start the nodes.
   */
  private static void process(Properties p) throws IOException {

//    boolean http_admin = false;
//    // The http admin information
//    InetAddress http_admin_host = null;
//    int http_admin_port = 0;
//    File http_admin_path = null;
//    // The http admin browser username and password,
//    String http_admin_username = null;
//    String http_admin_password = null;

    // Whether we start the gui,
    String gui_mode = p.getProperty("gui_mode", "false");

    // The number of nodes,
    int node_count = getIntegerValue(p, "node_count");
    // The network password,
    String net_pass = getPasswordValue(p, "net_password");
    // The local interface,
    InetAddress ihost = getLocalHostValue(p, "inet_address");

    // Sanity check on the node count,
    if (node_count < 0 || node_count > 5000) {
      throw new PError("Node count error, the range of nodes supported by this utility is 0 to 5000");
    }

//    // Do we start the http admin server?
//    String http_admin_value = p.getProperty("http_admin");
//    if (http_admin_value != null && http_admin_value.equalsIgnoreCase("true")) {
//      // Get the admin host address,
//      http_admin_host = getLocalHostValue(p, "http_admin_inet_address");
//      // The port
//      http_admin_port = getIntegerValue(p, "http_admin_inet_port");
//      // The path,
//      http_admin_path = getPathValue(p, "http_admin_path");
//      // The username and password,
//      http_admin_username = getPasswordValue(p, "http_admin_username");
//      http_admin_password = getPasswordValue(p, "http_admin_password");
//      // Sanity checks,
//      if (http_admin_port < 0 || http_admin_port > 65535) {
//        throw new PError("Error: Illegal port value for 'http_admin_inet_port'");
//      }
//      http_admin = true;
//    }

    // The array of nodes,
    NodeInfo[] nodes = new NodeInfo[node_count];

    // For each node, fetch the various properties for the node,
    for (int i = 0; i < node_count; ++i) {
      String node_property_key = "node." + i + ".";
      // Fetch the path in the local system where the node data is stored,
      File path = getPathValue(p, node_property_key + "path");
      // Fetch the local port we are binding to,
      int node_port = getIntegerValue(p, node_property_key + "port");
      if (node_port < 0 || node_port > 65535) {
        throw new PError("Error: Illegal port value for '" + node_property_key + "port'");
      }

      NodeInfo n = new NodeInfo();
      n.inet_address = ihost;
      n.inet_port = node_port;
      n.local_path = path;

      nodes[i] = n;
    }

    // Checks,
    // Make sure a path is not duplicated, and a port is not duplicated,
    for (int i = 0; i < node_count - 1; ++i) {
      File p1 = nodes[i].local_path;
      int prt1 = nodes[i].inet_port;
      for (int n = i + 1; n < node_count; ++n) {
        File p2 = nodes[n].local_path;
        int prt2 = nodes[n].inet_port;
        // Check for duplicated paths
        if (p1.equals(p2)) {
          throw new PError("Error: duplicate node paths discovered (" +
                  "node " + i + " and node " + n + ")");
        }
        // Check for duplicated port values,
        if (prt1 == prt2) {
          throw new PError("Error: duplicate node port values discovered (" +
                  "node " + i + " and node " + n + ")");
        }
      }
    }

    // Create any paths that need to be created,
    for (int i = 0; i < node_count; ++i) {
      File p1 = nodes[i].local_path;
      // If is doesn't exist, try and create it,
      if (!p1.exists()) {
        System.out.println("Creating Directory: " + p1.getAbsolutePath());
        // Make directory,
        boolean b = p1.mkdir();
        if (!b) {
          throw new PError("Error: unable to create directory: " + p1.getAbsolutePath());
        }
      }
    }

    // The IP whitelist.
    String ip_whitelist = "127.0.0.1";
//    ip_whitelist = ip_whitelist + "," + http_admin_host.getHostAddress();
    ip_whitelist = ip_whitelist + "," + ihost.getHostAddress();

    // The node list
    String nodelist = null;
    for (NodeInfo node : nodes) {
      ServiceAddress saddr =
                        new ServiceAddress(node.inet_address, node.inet_port);
      if (nodelist == null) {
        nodelist = saddr.formatString();
      }
      else {
        nodelist = nodelist + "," + saddr.formatString();
      }
    }

    // Make the properties object,
    final Properties config_properties = new Properties();
    config_properties.setProperty("connect_whitelist", ip_whitelist);
    config_properties.setProperty("network_nodelist", nodelist);

    // Create a config resource for this simulation,
    NetworkConfigResource config_resource = new NetworkConfigResource() {
      protected long getLastModifiedTime() throws IOException {
        // This config resource never changes,
        return 90000;
      }
      protected void loadResource() throws IOException {
        // Nothing to load, we have the info we need
      }
      protected Properties refreshNodeProperties() throws IOException {
        return config_properties;
      }
    };

    NodeInstance[] node_instances = new NodeInstance[node_count];

    // Start up the node invocations,
    for (int i = 0; i < node_count; ++i) {
      InetAddress addr = nodes[i].inet_address;
      int port = nodes[i].inet_port;
      File path = nodes[i].local_path;

      NodeInstance ni = new NodeInstance(i, config_resource,
                                         addr, port, net_pass, path);
      node_instances[i] = ni;

      System.out.println("Starting Node " + i);
      System.out.print("  Address: " + addr.toString() + " ");
      System.out.println("port: " + port);
      System.out.println("  Path: " + path.getAbsolutePath());

      ni.start();

    }

//    // Start the http administration server if desired,
//    System.out.println();
//    if (http_admin) {
//      // Create the path if it doesn't already exist,
//      if (!http_admin_path.exists()) {
//        System.out.println("Creating Directory: " + http_admin_path.getAbsolutePath());
//        // Make directory,
//        boolean b = http_admin_path.mkdir();
//        if (!b) {
//          throw new PError("Error: unable to create directory: " + http_admin_path.getAbsolutePath());
//        }
//      }
//
//      System.out.println("Starting HTTP Administration server");
//      System.out.print("  Address: " + http_admin_host.toString() + " ");
//      System.out.println("port: " + http_admin_port);
//      System.out.println("  Path: " + http_admin_path.getAbsolutePath());
//
//      // Start the HTTP administration server
//      HTTPAdminServer http_admin_server =
//              new HTTPAdminServer(http_admin_path,
//                                  http_admin_host, http_admin_port,
//                                  http_admin_username, http_admin_password);
//      http_admin_server.setNetworkPassword(net_pass);
//      http_admin_server.start();
//    }

    System.out.println();
    // If no nodes,
    if (node_count == 0) {
      System.out.println("No nodes to start (node_count = 0).");
    }
    else {
      // otherwise,
      System.out.println("Simulation is now running.");
    }

    // If GUI,
    if (!gui_mode.equals("false")) {
      // Generate the GUI

      String look_and_feel = UIManager.getSystemLookAndFeelClassName();
      LookAndFeelInfo[] lnf_set = UIManager.getInstalledLookAndFeels();
      for (LookAndFeelInfo i : lnf_set) {
        if (i.getClassName().endsWith("NimbusLookAndFeel")) {
          look_and_feel = i.getClassName();
        }
      }

      // Install the look and feel
      try {
        UIManager.setLookAndFeel(look_and_feel);
      } catch (InstantiationException e) {
      } catch (ClassNotFoundException e) {
      } catch (UnsupportedLookAndFeelException e) {
      } catch (IllegalAccessException e) {
      }

      JPanel panel = new JPanel();
      panel.setLayout(new GridLayout(node_count, 1));

      for (int i = 0; i < node_count; ++i) {
        NodeInstance ni = node_instances[i];

        JPanel instance_panel = ni.createInstancePanel();
        panel.add(instance_panel);

      }

      JFrame frame = new JFrame("Simulation Control");
      frame.getContentPane().add(panel);
      frame.pack();
      frame.setVisible(true);

    }

  }

  /**
   * Internal exception.
   */
  private static class PError extends RuntimeException {
    public PError(String s) {
      super(s);
    }
  }

  private static class NodeInfo {
    InetAddress inet_address;
    int inet_port;
    File local_path;
  }

  private static class NodeInstance {

    private final int node_number;
    private final NetworkConfigResource config_resource;
    private final InetAddress inet_address;
    private final int port;
    private final String net_password;
    private final File path;

    private TCPInstanceAdminServer inst_admin_server;

    NodeInstance(int node_number,
                 NetworkConfigResource config_resource,
                 InetAddress inet_address, int port,
                 String net_password, File path) {

      this.node_number = node_number;
      this.config_resource = config_resource;
      this.inet_address = inet_address;
      this.port = port;
      this.net_password = net_password;
      this.path = path;

    }

    private void start() throws IOException {
      inst_admin_server =
           new TCPInstanceAdminServer(config_resource,
                                      inet_address, port, net_password, path);
      // Start the instance on its own thread,
      new Thread(inst_admin_server).start();
    }

    private void stop() {
      inst_admin_server.close();
    }

    private JPanel createInstancePanel() {
      StringBuilder node_string = new StringBuilder();
      node_string.append("Node ");
      node_string.append(inet_address.getHostAddress());
      node_string.append(":");
      node_string.append(port);
      node_string.append(" (");
      node_string.append(path.toString());
      node_string.append(")");

      final JButton start_button = new JButton("Start");
      final JButton stop_button = new JButton("Stop");
      final JLabel node_display = new JLabel(node_string.toString());

      start_button.setEnabled(false);

      start_button.addActionListener(new ActionListener() {
        public void actionPerformed(ActionEvent evt) {
          start_button.setEnabled(false);
          stop_button.setEnabled(true);
          stop_button.requestFocus();

          try {
            start();
          }
          catch (IOException e) {
            System.out.println("IO Error: " + e.getMessage());
          }
        }
      });
      stop_button.addActionListener(new ActionListener() {
        public void actionPerformed(ActionEvent evt) {
          stop_button.setEnabled(false);
          start_button.setEnabled(true);
          start_button.requestFocus();

          stop();
        }
      });

      JPanel instance_panel = new JPanel();
      instance_panel.setLayout(new FlowLayout());
      instance_panel.add(start_button);
      instance_panel.add(stop_button);
      instance_panel.add(node_display);

      return instance_panel;
    }

  }

}
