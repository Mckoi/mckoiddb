/**
 * com.mckoi.network.NetworkProfile  Jul 4, 2009
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

package com.mckoi.network;

import com.mckoi.data.NodeReference;
import com.mckoi.util.StringUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class enables the discovery of machine roles in a network. It is
 * intended to be used for administration functions only. It performs tasks
 * such as querying machine details about nodes on the network, discovering
 * nodes that aren't working, etc.
 *
 * @author Tobias Downer
 */

public class NetworkProfile {

//  /**
//   * The network schema (the list of machines in the network).
//   */
//  private HashMap<ServiceAddress, Object> machine_list;

  /**
   * The NetworkConfigurationResource containing information about the network
   * in general.
   */
  private NetworkConfigResource network_config;

  /**
   * The NetworkConnector for communicating with nodes in the network.
   */
  private final NetworkConnector network_connector;

  /**
   * The network challenge password needed to query a machine node.
   */
  private final String network_password;

  /**
   * The list of machine profiles recently inspected.
   */
  private ArrayList<MachineProfile> machine_profiles;

  /**
   * Constructs this profile object with the given NetworkConnector and network
   * password.
   */
  public NetworkProfile(NetworkConnector connector, String network_password) {
//    machine_list = new HashMap();
    this.network_connector = connector;
    this.network_password = network_password;
  }

  /**
   * Sets the network configuration resource object used to query information
   * about the network schema (the machines on the network).
   */
  public void setNetworkConfiguration(NetworkConfigResource config)
                                                           throws IOException {
    this.network_config = config;
    this.network_config.load();
  }






//  /**
//   * Given a Reader, extracts details of the network schema configuration.
//   * This document describes the service address of every node of the network,
//   * and the name of the server. It does not detail the services each server
//   * runs, which is queried by asking the machine.
//   */
//  public void readNetworkSchema(Reader doc) throws IOException {
//    // The document is simply formatted as a line deliminated list of servers
//    // in the network. Each entry is formatted as '[ip]:[port]'
//    BufferedReader read_in = new BufferedReader(doc);
//    while (true) {
//      String line = read_in.readLine();
//      if (line == null) {
//        return;
//      }
//      // Parse the service address
//      ServiceAddress saddr = ServiceAddress.parseString(line);
//
//      // Add the server into the server map
//      machine_list.put(saddr, Boolean.TRUE);
//    }
//  }
//
//  /**
//   * Writes the details of the network schema configuration out to the given
//   * Writer object in the same format 'readNetworkSchema' understands.
//   */
//  public void writeNetworkSchema(Writer doc) throws IOException {
//    BufferedWriter write_out = new BufferedWriter(doc);
//    PrintWriter out = new PrintWriter(write_out);
//
//    // Make a list and sort it,
//    Set<ServiceAddress> server_addresses = machine_list.keySet();
//    ArrayList<ServiceAddress> slist = new ArrayList();
//    for (ServiceAddress addr : server_addresses) {
//      slist.add(addr);
//    }
//
//    // Sort the list of service addresses
//    Collections.sort(slist);
//
//    // And output the sorted list to the writer,
//    for (ServiceAddress addr : slist) {
//      out.println(addr.formatString());
////      InetAddress inet_addr = addr.asInetAddress();
////      out.print(inet_addr.getCanonicalHostName());
////      out.print(":");
////      out.println(addr.getPort());
//    }
//
//    // Flush the buffer
//    out.flush();
//  }
//
//  /**
//   * Adds a machine node to the network schema (does nothing if the machine
//   * already added).
//   */
//  public void addMachineNode(ServiceAddress machine) {
//    machine_list.put(machine, Boolean.TRUE);
//    refresh();
//  }
//
//  /**
//   * Removes a machine node from the network schema (does nothing if the
//   * machine node not in the schema).
//   */
//  public void removeMachineNode(ServiceAddress machine) {
//    machine_list.remove(machine);
//    refresh();
//  }

  // -----

  /**
   * Returns true if the error message is a connection failure message.
   */
  public static boolean isConnectionFailure(Message m) {
    ExternalThrowable et = m.getExternalThrowable();
    // If it's a connect exception,
    String ex_type = et.getClassName();
    if (ex_type.equals("java.net.ConnectException")) {
      return true;
    }
    else if (ex_type.equals("com.mckoi.network.ServiceNotConnectedException")) {
      return true;
    }
    return false;
  }

  /**
   * Queries the machine at the given ServiceAddress and returns true if the
   * machine is a valid Mckoi machine node.
   */
  public boolean isValidMckoiNode(ServiceAddress machine) {
    // Request a report from the administration role on the machine,
    MessageProcessor mp = network_connector.connectInstanceAdmin(machine);
    MessageStream msg_out = new MessageStream(16);
    msg_out.addMessage("report");
    msg_out.closeMessage();
    ProcessResult msg_in = mp.process(msg_out);
    Message last_m = null;

    for (Message m : msg_in) {
      last_m = m;
    }
    if (last_m.isError()) {
      // Not a valid node,
      // Should we break this error down to smaller questions. Such as, is the
      // password incorrect, etc?
      return false;
    }

    return true;
  }

  /**
   * Returns a machine list of all nodes in the network sorted by the ip/port
   * address.
   */
  public ArrayList<ServiceAddress> sortedServerList() {

    String node_list = network_config.getNetworkNodelist();
    if (node_list == null || node_list.length() == 0) {
      return new ArrayList(0);
    }

    ArrayList<ServiceAddress> slist = new ArrayList();
    try {
      List<String> nodes = StringUtil.explode(node_list, ",");
      for (String node : nodes) {
        slist.add(ServiceAddress.parseString(node.trim()));
      }
    }
    catch (RuntimeException e) {
      throw new RuntimeException("Unable to parse network configuration node list.", e);
    }
    catch (IOException e) {
      throw new RuntimeException("IO Error parsing network configuration node list.", e);
    }

    // Sort the list of service addresses (the list is probably already sorted)
    Collections.sort(slist);

    return slist;
//
//
//    // Make a list of servers and sort it,
//    Set<ServiceAddress> server_addresses = machine_list.keySet();
//    ArrayList<ServiceAddress> slist = new ArrayList();
//    for (ServiceAddress addr : server_addresses) {
//      slist.add(addr);
//    }
//
//    // Sort the list of service addresses (the list is probably already sorted)
//    Collections.sort(slist);
//
//    return slist;
  }


  /**
   * Perform a network inspection, which queries each machine in the schema
   * and discovers the function it performs. This method will populate
   * internal structures in this object so that queries to the accessor
   * methods will return information about the network.
   */
  private ArrayList<MachineProfile> inspectNetwork() {
    // If cached,
    if (machine_profiles != null) {
      return machine_profiles;
    }

    // The sorted list of all servers in the schema,
    ArrayList<ServiceAddress> slist = sortedServerList();

    // The list of machine profiles,
    ArrayList<MachineProfile> machines = new ArrayList();

    // For each machine in the network,
    for (ServiceAddress server : slist) {

      MachineProfile machine_profile = new MachineProfile(server);

      // Request a report from the administration role on the machine,
      MessageProcessor mp = network_connector.connectInstanceAdmin(server);
      MessageStream msg_out = new MessageStream(16);
      msg_out.addMessage("report");
      msg_out.closeMessage();
      ProcessResult msg_in = mp.process(msg_out);
      Message last_m = null;

      for (Message m : msg_in) {
        last_m = m;
      }
      if (last_m.isError()) {
        machine_profile.setProblemMessage(last_m.getErrorMessage());
      }
      else {
        // Get the message replies,
        String b = (String) last_m.param(0);
        boolean is_block = !b.equals("block_server=no");
        String m = (String) last_m.param(1);
        boolean is_manager = !m.equals("manager_server=no");
        String r = (String) last_m.param(2);
        boolean is_root = !r.equals("root_server=no");

        long used_mem = (Long) last_m.param(3);
        long total_mem = (Long) last_m.param(4);
        long used_disk = (Long) last_m.param(5);
        long total_disk = (Long) last_m.param(6);

        // Populate the lists,
        machine_profile.setIsBlock(is_block);
        machine_profile.setIsRoot(is_root);
        machine_profile.setIsManager(is_manager);

        machine_profile.setHeapUsed(used_mem);
        machine_profile.setHeapTotal(total_mem);
        machine_profile.setStorageUsed(used_disk);
        machine_profile.setStorageTotal(total_disk);

      }

      // Add the machine profile to the list
      machines.add(machine_profile);

    }

    machine_profiles = machines;
    return machine_profiles;
  }

  /**
   * Refreshes this profile by inspecting the network and discovering any
   * changes to state.
   */
  public void refresh() {
    machine_profiles = null;
    inspectNetwork();
  }

  /**
   * Refresh the network configuration resource.
   */
  public void refreshNetworkConfig() throws IOException {
    network_config.load();
  }

  /**
   * Returns true if the given ServiceAddress is a machine node that is part of
   * the network.
   */
  public boolean isMachineInNetwork(ServiceAddress machine_addr) {
    inspectNetwork();

    for (MachineProfile machine : machine_profiles) {
      if (machine.getServiceAddress().equals(machine_addr)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns a MachineProfile object of the machine in the network with the
   * given ServiceAddress, or null if there is no machine in the schema with
   * the given address.
   */
  public MachineProfile getMachineProfile(ServiceAddress address) {
    inspectNetwork();
    for (MachineProfile p : machine_profiles) {
      if (p.getServiceAddress().equals(address)) {
        return p;
      }
    }
    return null;
  }

  /**
   * Returns the current manager servers on the network, from the profile, or
   * null if there's current no manager assigned.
   */
  public MachineProfile[] getManagerServers() {
    inspectNetwork();

    ArrayList<MachineProfile> list = new ArrayList();
    for (MachineProfile machine : machine_profiles) {
      if (machine.isManager()) {
        list.add(machine);
      }
    }
    return list.toArray(new MachineProfile[list.size()]);
  }

  /**
   * Returns the set of all root servers in the network, from the profile,
   * or an empty array if no root servers discovered.
   */
  public MachineProfile[] getRootServers() {
    inspectNetwork();

    ArrayList<MachineProfile> list = new ArrayList();
    for (MachineProfile machine : machine_profiles) {
      if (machine.isRoot()) {
        list.add(machine);
      }
    }

    return list.toArray(new MachineProfile[list.size()]);
  }

  /**
   * Returns the set of all block servers in the network, from the profile,
   * or an empty array if no block servers discovered.
   */
  public MachineProfile[] getBlockServers() {
    inspectNetwork();

    ArrayList<MachineProfile> list = new ArrayList();
    for (MachineProfile machine : machine_profiles) {
      if (machine.isBlock()) {
        list.add(machine);
      }
    }

    return list.toArray(new MachineProfile[list.size()]);
  }

  /**
   * Returns a list of all machine profiles discovered on the network.
   */
  public MachineProfile[] getAllMachineProfiles() {
    inspectNetwork();

    return machine_profiles.toArray(
            new MachineProfile[machine_profiles.size()]);
  }


  // ----- Network commands -----


  private Message commandAdmin(ServiceAddress machine, MessageStream msg_out) {
    MessageProcessor proc = network_connector.connectInstanceAdmin(machine);
    ProcessResult msg_in = proc.process(msg_out);
    Message last_m = null;
    for (Message m : msg_in) {
      last_m = m;
    }
    return last_m;
  }
  private Message commandRoot(ServiceAddress machine, MessageStream msg_out) {
    MessageProcessor proc = network_connector.connectRootServer(machine);
    ProcessResult msg_in = proc.process(msg_out);
    Message last_m = null;
    for (Message m : msg_in) {
      last_m = m;
    }
    return last_m;
  }
  private Message commandBlock(ServiceAddress machine, MessageStream msg_out) {
    MessageProcessor proc = network_connector.connectBlockServer(machine);
    ProcessResult msg_in = proc.process(msg_out);
    Message last_m = null;
    for (Message m : msg_in) {
      last_m = m;
    }
    return last_m;
  }
  private Message commandManager(ServiceAddress machine, MessageStream msg_out) {
    MessageProcessor proc = network_connector.connectManagerServer(machine);
    ProcessResult msg_in = proc.process(msg_out);
    Message last_m = null;
    for (Message m : msg_in) {
      last_m = m;
    }
    return last_m;
  }

  /**
   * Sends a command to the manager cluster. If none of the manager's are
   * available or accept the command, the exception is rethrown.
   */
  private void sendManagerCommand(String function_name, Object... args)
                                                throws NetworkAdminException {

    // Send the add path command to the first available manager server.
    MachineProfile[] manager_servers = getManagerServers();

    MessageStream msg_out = new MessageStream(12);
    MessageCommunicator.createMessage(msg_out, function_name, args);

    // The first manager that takes the command,
    boolean success = false;
    Message last_error = null;
    for (int i = 0; i < manager_servers.length && success == false; ++i) {
      ServiceAddress manager_server = manager_servers[i].getServiceAddress();
      Message m = commandManager(manager_server, msg_out);
      if (m.isError()) {
        if (!isConnectionFailure(m)) {
          throw new NetworkAdminException(m);
        }
        last_error = m;
      }
      else {
        success = true;
      }
    }
    // All managers failed,
    if (!success) {
      throw new NetworkAdminException(last_error);
    }

  }

  /**
   * Performs a function on the first node that is available in the manager
   * cluster.
   */
  private Object sendManagerFunction(String function_name, Object... args)
                                                throws NetworkAdminException {

    // Send the add path command to the first available manager server.
    MachineProfile[] manager_servers = getManagerServers();

    MessageStream msg_out = new MessageStream(12);
    MessageCommunicator.createMessage(msg_out, function_name, args);

    // The first manager that takes the command,
    Object result = null;
    Message last_error = null;
    for (int i = 0; i < manager_servers.length && result == null; ++i) {
      ServiceAddress manager_server = manager_servers[i].getServiceAddress();
      Message m = commandManager(manager_server, msg_out);
      if (m.isError()) {
        if (!isConnectionFailure(m)) {
          throw new NetworkAdminException(m);
        }
        last_error = m;
      }
      else {
        return m.param(0);
      }
    }

    // All managers failed,
    throw new NetworkAdminException(last_error);

  }

  /**
   * Sends a command to all the root servers in the given list. If any of the
   * messages fails, throws an exception.
   */
  private void sendAllRootServers(ServiceAddress[] roots,
          String function_name, Object... args) throws NetworkAdminException {

    MessageStream msg_out = new MessageStream(12);
    MessageCommunicator.createMessage(msg_out, function_name, args);

    // Send the command to all the root servers,
    Message last_error = null;
    ProcessResult[] msg_ins = new ProcessResult[roots.length];

    for (int i = 0; i < roots.length; ++i) {
      ServiceAddress root_server = roots[i];
      MessageProcessor proc = network_connector.connectRootServer(root_server);
      msg_ins[i] = proc.process(msg_out);
    }

    int success_count = 0;
    for (ProcessResult msg_in : msg_ins) {
      for (Message m : msg_in) {
        if (m.isError()) {
          if (!isConnectionFailure(m)) {
            throw new NetworkAdminException(m);
          }
          last_error = m;
        }
        else {
          ++success_count;
        }
      }
    }

    // Any one root failed,
    if (success_count != roots.length) {
      throw new NetworkAdminException(last_error);
    }
  }

  /**
   * Sends a command to the root server. If the server fails on the message,
   * throws an exception.
   */
  private void sendRootServer(ServiceAddress root,
          String function_name, Object... args) throws NetworkAdminException {

    MessageStream msg_out = new MessageStream(12);
    MessageCommunicator.createMessage(msg_out, function_name, args);

    // Send the command to all the root servers,
    Message last_error = null;
    ProcessResult msg_in;

    MessageProcessor proc = network_connector.connectRootServer(root);
    msg_in = proc.process(msg_out);

    int success_count = 0;
    for (Message m : msg_in) {
      if (m.isError()) {
        if (!isConnectionFailure(m)) {
          throw new NetworkAdminException(m);
        }
        last_error = m;
      }
      else {
        ++success_count;
      }
    }

    // Any one root failed,
    if (success_count != 1) {
      throw new NetworkAdminException(last_error);
    }
  }



  private MachineProfile checkMachineInNetwork(ServiceAddress machine)
                                                throws NetworkAdminException {
    inspectNetwork();

    for (MachineProfile m : machine_profiles) {
      if (m.getServiceAddress().equals(machine)) {
        return m;
      }
    }
    throw new NetworkAdminException(
              "Machine '" + machine.displayString() +
              "' is not in the network schema");
  }


  /**
   * Assign a machine to a role.
   * <p>
   * Note that this does not update service registration of nodes in the
   * network.
   */
  private void changeRole(MachineProfile machine,
                          String status, String role_type)
                                                throws NetworkAdminException {

    MessageStream msg_out = new MessageStream(7);
    msg_out.addMessage(status);
    if (role_type.equals("manager")) {
      msg_out.addString("manager_server");
    }
    else if (role_type.equals("root")) {
      msg_out.addString("root_server");
    }
    else if (role_type.equals("block")) {
      msg_out.addString("block_server");
    }
    else {
      throw new RuntimeException("Unknown role type: " + role_type);
    }
    msg_out.closeMessage();

    Message m = commandAdmin(machine.getServiceAddress(), msg_out);
    if (m.isError()) {
//      System.out.println(m.getExternalThrowable().getStackTrace());
      throw new NetworkAdminException(m);
    }
    // Success,

    // Update the network profile,
    if (role_type.equals("manager")) {
      machine.setIsManager(status.equals("start"));
    }
    else if (role_type.equals("root")) {
      machine.setIsRoot(status.equals("start"));
    }
    else if (role_type.equals("block")) {
      machine.setIsBlock(status.equals("start"));
    }

  }






  /**
   * Assign a machine to a manager server role. Generates an exception if
   * there is already a manager server.
   * <p>
   * Note that this does not update service registration of nodes in the
   * network.
   */
  public void startManager(ServiceAddress machine)
                                                throws NetworkAdminException {
    inspectNetwork();
    // Check machine is in the schema,
    MachineProfile machine_p = checkMachineInNetwork(machine);
    if (!machine_p.isManager()) {
      // No current manager, so go ahead and assign,
      changeRole(machine_p, "start", "manager");
    }
    else {
      throw new NetworkAdminException(
                             "Manager already assigned on machine " + machine);
    }
  }

  /**
   * Relieve a machine from the manager server role. Generates an exception if
   * the machine is not assigned as manager server.
   * <p>
   * Note that this does not update service registration of nodes in the
   * network.
   */
  public void stopManager(ServiceAddress machine)
                                                throws NetworkAdminException {

    inspectNetwork();
    // Check machine is in the schema,
    MachineProfile machine_p = checkMachineInNetwork(machine);
    if (machine_p.isManager()) {
      // The current manager matches, so we can stop
      changeRole(machine_p, "stop", "manager");
    }
    else {
      throw new NetworkAdminException(
                     "Manager not assigned to machine " + machine);
    }
  }

  /**
   * Assign a machine to a root server role. Generates an exception if
   * the machine is already assigned to be a root.
   * <p>
   * Note that this does not update service registration of nodes in the
   * network.
   */
  public void startRoot(ServiceAddress machine) throws NetworkAdminException {
    inspectNetwork();
    // Check machine is in the schema,
    MachineProfile machine_p = checkMachineInNetwork(machine);
    if (machine_p.isRoot()) {
      throw new NetworkAdminException(
                           "Root already assigned on machine " + machine);
    }
    // Go ahead and change the role of the machine
    changeRole(machine_p, "start", "root");
  }

  /**
   * Relieve a machine from a root server role. Generates an exception if
   * the machine is not assigned as a root server.
   * <p>
   * Note that this does not update service registration of nodes in the
   * network.
   */
  public void stopRoot(ServiceAddress machine) throws NetworkAdminException {

    inspectNetwork();
    // Check machine is in the schema,
    MachineProfile machine_p = checkMachineInNetwork(machine);
    if (!machine_p.isRoot()) {
      throw new NetworkAdminException(
                             "Root not assigned on machine " + machine);
    }
    // Go ahead and change the role of the machine
    changeRole(machine_p, "stop", "root");
  }

  /**
   * Assign a machine to a block server role. Generates an exception if
   * the machine is already assigned to be a block server.
   * <p>
   * Note that this does not update service registration of nodes in the
   * network.
   */
  public void startBlock(ServiceAddress machine) throws NetworkAdminException {
    inspectNetwork();
    // Check machine is in the schema,
    MachineProfile machine_p = checkMachineInNetwork(machine);
    if (machine_p.isBlock()) {
      throw new NetworkAdminException(
                       "Block server already assigned on machine " + machine);
    }
    // Go ahead and change the role of the machine
    changeRole(machine_p, "start", "block");
  }

  /**
   * Relieve a machine from a block server role. Generates an exception if
   * the machine is not assigned as a block server.
   * <p>
   * Note that this does not update service registration of nodes in the
   * network.
   */
  public void stopBlock(ServiceAddress machine) throws NetworkAdminException {

    inspectNetwork();
    // Check machine is in the schema,
    MachineProfile machine_p = checkMachineInNetwork(machine);
    if (!machine_p.isBlock()) {
      throw new NetworkAdminException(
                           "Block server not assigned on machine " + machine);
    }
    // Go ahead and change the role of the machine
    changeRole(machine_p, "stop", "block");
  }


  /**
   * Registers a manager with the current managers assigned on the network.
   */
  public void registerManager(ServiceAddress manager)
                                                throws NetworkAdminException {
    inspectNetwork();

    // Check machine is in the schema,
    MachineProfile machine_p = checkMachineInNetwork(manager);
    MachineProfile[] current_managers = getManagerServers();

    if (current_managers.length == 0) {
      throw new NetworkAdminException("No manager server found");
    }
    // Check it is a manager server,
    if (!machine_p.isManager()) {
      throw new NetworkAdminException(
                     "Machine '" + manager + "' is not assigned as a manager");
    }

    // The list of manager servers,
    ServiceAddress[] manager_servers =
                                   new ServiceAddress[current_managers.length];
    for (int i = 0; i < current_managers.length; ++i) {
      manager_servers[i] = current_managers[i].getServiceAddress();
    }

    MessageStream msg_out = new MessageStream(7);
    msg_out.addMessage("registerManagerServers");
    msg_out.addServiceAddressArr(manager_servers);
    msg_out.closeMessage();

    // Register the root server with all the managers currently on the network,
    for (int i = 0; i < current_managers.length; ++i) {
      Message m =
              commandManager(current_managers[i].getServiceAddress(), msg_out);
      if (m.isError()) {
        throw new NetworkAdminException(m);
      }
    }
  }

  /**
   * Registers a manager with the current managers assigned on the network.
   */
  public void deregisterManager(ServiceAddress root)
                                                throws NetworkAdminException {
    inspectNetwork();

    // Check machine is in the schema,
    MachineProfile machine_p = checkMachineInNetwork(root);
    MachineProfile[] current_managers = getManagerServers();

    if (current_managers.length == 0) {
      throw new NetworkAdminException("No manager server found");
    }
    // Check it is a manager server,
    if (!machine_p.isManager()) {
      throw new NetworkAdminException(
                        "Machine '" + root + "' is not assigned as a manager");
    }

    MessageStream msg_out = new MessageStream(7);
    msg_out.addMessage("deregisterManagerServer");
    msg_out.addServiceAddress(root);
    msg_out.closeMessage();

    // Register the root server with all the managers currently on the network,
    for (int i = 0; i < current_managers.length; ++i) {
      Message m =
              commandManager(current_managers[i].getServiceAddress(), msg_out);
      if (m.isError()) {
        throw new NetworkAdminException(m);
      }
    }
  }

  /**
   * Contacts a root server and registers it to the current manager server
   * assigned on the network.
   */
  public void registerRoot(ServiceAddress root) throws NetworkAdminException {
    inspectNetwork();

    // Check machine is in the schema,
    MachineProfile machine_p = checkMachineInNetwork(root);
    MachineProfile[] current_managers = getManagerServers();

    if (current_managers.length == 0) {
      throw new NetworkAdminException("No manager server found");
    }
    // Check it is a root server,
    if (!machine_p.isRoot()) {
      throw new NetworkAdminException(
                           "Machine '" + root + "' is not assigned as a root");
    }

    MessageStream msg_out = new MessageStream(7);
    msg_out.addMessage("registerRootServer");
    msg_out.addServiceAddress(root);
    msg_out.closeMessage();

    // Register the root server with all the managers currently on the network,
    for (int i = 0; i < current_managers.length; ++i) {
      Message m =
              commandManager(current_managers[i].getServiceAddress(), msg_out);
      if (m.isError()) {
        throw new NetworkAdminException(m);
      }
    }

  }

  /**
   * Contacts a root server and deregisters it from the current manager server
   * assigned on the network.
   */
  public void deregisterRoot(ServiceAddress root)
                                                throws NetworkAdminException {
    inspectNetwork();

    // Check machine is in the schema,
    MachineProfile machine_p = checkMachineInNetwork(root);
    MachineProfile[] current_managers = getManagerServers();

    if (current_managers.length == 0) {
      throw new NetworkAdminException("No manager server found");
    }
    // Check it is a root server,
    if (!machine_p.isRoot()) {
      throw new NetworkAdminException(
                           "Machine '" + root + "' is not assigned as a root");
    }

    MessageStream msg_out = new MessageStream(7);
    msg_out.addMessage("deregisterRootServer");
    msg_out.addServiceAddress(root);
    msg_out.closeMessage();

    for (int i = 0; i < current_managers.length; ++i) {
      Message m =
              commandManager(current_managers[i].getServiceAddress(), msg_out);
      if (m.isError()) {
        throw new NetworkAdminException(m);
      }
    }

  }

  /**
   * Contacts the current manager server assigned on the network and registers
   * a block server to it.
   */
  public void registerBlock(ServiceAddress block)
                                                throws NetworkAdminException {
    inspectNetwork();

    // Check machine is in the schema,
    MachineProfile machine_p = checkMachineInNetwork(block);
    MachineProfile[] current_managers = getManagerServers();

    if (current_managers.length == 0) {
      throw new NetworkAdminException("No manager server found");
    }
    // Check it is a block role,
    if (!machine_p.isBlock()) {
      throw new NetworkAdminException(
                    "Machine '" + block + "' is not assigned as a block role");
    }

    MessageStream msg_out = new MessageStream(7);
    msg_out.addMessage("registerBlockServer");
    msg_out.addServiceAddress(block);
    msg_out.closeMessage();

    for (int i = 0; i < current_managers.length; ++i) {
      Message m =
              commandManager(current_managers[i].getServiceAddress(), msg_out);
      if (m.isError()) {
        throw new NetworkAdminException(m);
      }
    }
  }

  /**
   * Contacts the current manager server assigned on the network and
   * deregisters the given block server from it.
   */
  public void deregisterBlock(ServiceAddress block)
                                                throws NetworkAdminException {
    inspectNetwork();

    // Check machine is in the schema,
    MachineProfile machine_p = checkMachineInNetwork(block);
    MachineProfile[] current_managers = getManagerServers();

    if (current_managers.length == 0) {
      throw new NetworkAdminException("No manager server found");
    }
    // Check it is a block role,
    if (!machine_p.isBlock()) {
      throw new NetworkAdminException(
                    "Machine '" + block + "' is not assigned as a block role");
    }

    MessageStream msg_out = new MessageStream(7);
    msg_out.addMessage("deregisterBlockServer");
    msg_out.addServiceAddress(block);
    msg_out.closeMessage();

    for (int i = 0; i < current_managers.length; ++i) {
      Message m =
              commandManager(current_managers[i].getServiceAddress(), msg_out);
      if (m.isError()) {
        throw new NetworkAdminException(m);
      }
    }
  }

//  /**
//   * Returns the list of paths and their consensus processors from the given
//   * root server.
//   */
//  public PathInfo[] getPathsFromRoot(ServiceAddress root)
//                                                 throws NetworkAdminException {
//    inspectNetwork();
//
//    // Check machine is in the schema,
//    MachineProfile machine_p = checkMachineInNetwork(root);
//
//    MessageStream msg_out = new MessageStream(7);
//    msg_out.addMessage("consensusProcessorReport");
//    msg_out.closeMessage();
//
//    Message m = commandRoot(root, msg_out);
//    if (m.isError()) {
//      throw new NetworkAdminException(m);
//    }
//    else {
//      String[] paths = (String[]) m.param(0);
//      String[] funs = (String[]) m.param(1);
//
//      PathInfo[] list = new PathInfo[paths.length];
//      for (int i = 0; i < paths.length; ++i) {
//        list[i] = new PathInfo(root, paths[i], funs[i]);
//      }
//
//      return list;
//    }
//  }

  /**
   * Returns the list of all path names from all root servers registered on the
   * network (ordered in no significant way).
   */
  public String[] getAllPathNames() throws NetworkAdminException {
    inspectNetwork();

    // The list of all paths,
    String[] path_list = (String[]) sendManagerFunction("getAllPaths");

    return path_list;
  }

  /**
   * Returns the PathInfo for the given path name, or null if the path is not
   * defined.
   */
  public PathInfo getPathInfoForPath(String path_name)
                                                throws NetworkAdminException {
    // Query the manager cluster for the PathInfo
    return (PathInfo) sendManagerFunction("getPathInfoForPath", path_name);
  }


  /**
   * Adds a path/root server map to the network. This sends the path map to
   * the first available manager server.
   */
  public void addPathToNetwork(String path_name, String consensus_fun,
                  ServiceAddress root_leader, ServiceAddress[] root_servers)
                                                throws NetworkAdminException {
    inspectNetwork();

//    ServiceAddress root_leader = root_server;
//    ServiceAddress[] root_servers = new ServiceAddress[] { root_server };

    // Send the add path command to the first available manager server.
    sendManagerCommand("addPathToNetwork",
                           path_name, consensus_fun,
                           root_leader, root_servers);

    // Fetch the path info from the manager cluster,
    PathInfo path_info =
               (PathInfo) sendManagerFunction("getPathInfoForPath", path_name);

    // Send command to all the root servers,
    sendAllRootServers(root_servers, "internalSetPathInfo", path_name,
                       path_info.getVersionNumber(), path_info);
    sendAllRootServers(root_servers, "loadPathInfo", path_info);

    // Initialize the path on the leader,
    sendRootServer(root_leader,
          "initialize", path_info.getPathName(), path_info.getVersionNumber());

  }

  /**
   * Removes a path/root server map from the network. This sends the path map
   * removal command from the first available manager server.
   */
  public void removePathFromNetwork(String path_name,
                    ServiceAddress root_server) throws NetworkAdminException {
    inspectNetwork();

    // Send the remove path command to the first available manager server.
    sendManagerCommand("removePathFromNetwork", path_name, root_server);

  }


//  /**
//   * Adds a new consensus and path to the given machine running a root service.
//   * In addition, this will tell the manager server about the path_name to
//   * root server association added.
//   */
//  public void addConsensusFunction(ServiceAddress root,
//                                   String path_name, String consensus_fun)
//                                                throws NetworkAdminException {
//    inspectNetwork();
//
//    // Check machine is in the schema,
//    MachineProfile machine_p = checkMachineInNetwork(root);
//    // Check it's root,
//    if (!machine_p.isRoot()) {
//      throw new NetworkAdminException("Machine '" + root + "' is not a root");
//    }
//
//    // Get the current manager server,
//    MachineProfile[] mans = getManagerServers();
//    if (mans.length == 0) {
//      throw new NetworkAdminException("No manager server found");
//    }
//
//    {
//      // Check with the root server that the class instantiates,
//      MessageStream msg_out = new MessageStream(12);
//      msg_out.addMessage("checkConsensusClass");
//      msg_out.addString(consensus_fun);
//      msg_out.closeMessage();
//
//      Message m = commandRoot(root, msg_out);
//      if (m.isError()) {
//        throw new NetworkAdminException("Class '" + consensus_fun + "' doesn't instantiate on the root");
//      }
//    }
//
//    // Use the manager servers,
//    ServiceAddress[] manager_servers = new ServiceAddress[mans.length];
//    for (int i = 0; i < mans.length; ++i) {
//      manager_servers[i] = mans[i].getServiceAddress();
//    }
//
//    // Create a new empty database,
//    MckoiDDBClient db_client =
//              MckoiDDBClientUtils.connectTCP(manager_servers, network_password);
//    DataAddress data_address = db_client.createEmptyDatabase();
//    db_client.disconnect();
//
//    // Perform the command,
//    MessageStream msg_out = new MessageStream(12);
//    msg_out.addMessage("addConsensusProcessor");
//    msg_out.addString(path_name);
//    msg_out.addString(consensus_fun);
//    msg_out.addDataAddress(data_address);
//    msg_out.closeMessage();
//
//    Message m = commandRoot(root, msg_out);
//    if (m.isError()) {
//      throw new NetworkAdminException(m);
//    }
//
//    msg_out = new MessageStream(12);
//    msg_out.addMessage("initialize");
//    msg_out.addString(path_name);
//    msg_out.closeMessage();
//
//    m = commandRoot(root, msg_out);
//    if (m.isError()) {
//      throw new NetworkAdminException(m);
//    }
//    else {
//      // Tell the manager servers about this path,
//      msg_out = new MessageStream(7);
//      msg_out.addMessage("addPathRootMapping");
//      msg_out.addString(path_name);
//      msg_out.addServiceAddress(root);
//      msg_out.closeMessage();
//
//      // Try sending to a manager that doesn't fail. If any fail, the data
//      // will be replicated during maintenance.
//
//      Message last_error = null;
//      boolean success = false;
//      for (int i = 0; i < manager_servers.length && success == false; ++i) {
//        m = commandManager(manager_servers[i], msg_out);
//        if (m.isError()) {
//          last_error = m;
//        }
//        else {
//          success = true;
//        }
//      }
//      // All managers failed,
//      if (!success) {
//        throw new NetworkAdminException(last_error);
//      }
//    }
//
//  }
//
//  /**
//   * Deletes a path name assigned on the given root server.
//   */
//  public void removeConsensusFunction(ServiceAddress root, String path_name)
//                                                throws NetworkAdminException {
//    inspectNetwork();
//
//    // Check machine is in the schema,
//    MachineProfile machine_p = checkMachineInNetwork(root);
//    // Check it's root,
//    if (!machine_p.isRoot()) {
//      throw new NetworkAdminException("Machine '" + root + "' is not a root");
//    }
//
//    // Get the current manager server,
//    MachineProfile[] mans = getManagerServers();
//    if (mans.length == 0) {
//      throw new NetworkAdminException("No manager server found");
//    }
//
//    // Use the manager servers,
//    ServiceAddress[] manager_servers = new ServiceAddress[mans.length];
//    for (int i = 0; i < mans.length; ++i) {
//      manager_servers[i] = mans[i].getServiceAddress();
//    }
//
//    // Perform the command,
//    MessageStream msg_out = new MessageStream(7);
//    msg_out.addMessage("removeConsensusProcessor");
//    msg_out.addString(path_name);
//    msg_out.closeMessage();
//
//    Message m = commandRoot(root, msg_out);
//    if (m.isError()) {
//      throw new NetworkAdminException(m);
//    }
//    else {
//      // Tell the manager servers about this path,
//      msg_out = new MessageStream(7);
//      msg_out.addMessage("removePathRootMapping");
//      msg_out.addString(path_name);
//      msg_out.addServiceAddress(root);
//      msg_out.closeMessage();
//
//      // Try sending to all the managers. If any fail, the data will be
//      // replicated during maintenance. If they all fail, then the data will
//      // be inconsistent.
//      Message last_error = null;
//      boolean success = false;
//      for (int i = 0; i < manager_servers.length && success == false; ++i) {
//        m = commandManager(manager_servers[i], msg_out);
//        if (m.isError()) {
//          last_error = m;
//        }
//        else {
//          success = true;
//        }
//      }
//      // All managers failed,
//      if (!success) {
//        throw new NetworkAdminException(last_error);
//      }
//    }
//
//  }

//  /**
//   * Given a path name, queries the manager server and returns the root server
//   * that has been assigned to manage this path, or null if no root server
//   * association found.
//   */
//  public ServiceAddress getRootFor(String path_name)
//                                                throws NetworkAdminException {
//    inspectNetwork();
//
//    // Get the current manager server,
//    MachineProfile[] mans = getManagerServers();
//    if (mans.length == 0) {
//      throw new NetworkAdminException("No manager server found");
//    }
//    ServiceAddress manager_server = mans[0].getServiceAddress();
//
//    MessageStream msg_out = new MessageStream(7);
//    msg_out.addMessage("getRootLeaderForPath");
//    msg_out.addString(path_name);
//    msg_out.closeMessage();
//
//    Message m = commandManager(manager_server, msg_out);
//    if (m.isError()) {
//      throw new NetworkAdminException(m);
//    }
//
//    // Return the service address for the root server,
//    return (ServiceAddress) m.param(0);
//  }


  /**
   * Returns an array of DataAddress that represent the snapshots stored on the
   * given path at the time of the given timestamp. This will always return
   * DataAddress object regardless of whether any commits happened at the
   * given time or not. The returned DataAddress objects will be snapshots
   * at roughly the time given.
   */
  public DataAddress[] getHistoricalPathRoots(ServiceAddress root, String path_name,
                  long timestamp, int max_count) throws NetworkAdminException {

    inspectNetwork();

    // Check machine is in the schema,
    MachineProfile machine_p = checkMachineInNetwork(root);
    // Check it's root,
    if (!machine_p.isRoot()) {
      throw new NetworkAdminException("Machine '" + root + "' is not a root");
    }

    // Perform the command,
    MessageStream msg_out = new MessageStream(7);
    msg_out.addMessage("getPathHistorical");
    msg_out.addString(path_name);
    msg_out.addLong(timestamp);
    msg_out.addLong(timestamp);
    msg_out.closeMessage();

    Message m = commandRoot(root, msg_out);
    if (m.isError()) {
      throw new NetworkAdminException(m);
    }

    // Return the data address array,
    return (DataAddress[]) m.param(0);
  }

  /**
   * Sets the root for the given path name by issuing a 'publish' command on
   * the root server. Great care should be taken when using this function
   * because it bypasses all commit checks.
   */
  public void setPathRoot(ServiceAddress root, String path_name,
                          DataAddress address) throws NetworkAdminException {

    inspectNetwork();

    // Check machine is in the schema,
    MachineProfile machine_p = checkMachineInNetwork(root);
    // Check it's root,
    if (!machine_p.isRoot()) {
      throw new NetworkAdminException("Machine '" + root + "' is not a root");
    }

    // Perform the command,
    MessageStream msg_out = new MessageStream(7);
    msg_out.addMessage("publishPath");
    msg_out.addString(path_name);
    msg_out.addDataAddress(address);
    msg_out.closeMessage();

    Message m = commandRoot(root, msg_out);
    if (m.isError()) {
      throw new NetworkAdminException(m);
    }

  }

  /**
   * Returns the stats string for the given path name on the given root server.
   */
  public String getPathStats(PathInfo path_info)
                                                throws NetworkAdminException {
    inspectNetwork();

    ServiceAddress root_leader = path_info.getRootLeader();

    // Check machine is in the schema,
    MachineProfile machine_p = checkMachineInNetwork(root_leader);
    // Check it's root,
    if (!machine_p.isRoot()) {
      throw new NetworkAdminException("Machine '" + root_leader + "' is not a root");
    }

    // Perform the command,
    MessageStream msg_out = new MessageStream(7);
    msg_out.addMessage("getPathStats");
    msg_out.addString(path_info.getPathName());
    msg_out.addInteger(path_info.getVersionNumber());
    msg_out.closeMessage();

    Message m = commandRoot(root_leader, msg_out);
    if (m.isError()) {
      throw new NetworkAdminException(m);
    }

    // Return the stats string for this path
    return (String) m.param(0);
  }

  /**
   * Returns the GUID of a block server.
   */
  public long getBlockGUID(ServiceAddress block) throws NetworkAdminException {
    inspectNetwork();

    // Check machine is in the schema,
    MachineProfile machine_p = checkMachineInNetwork(block);
    // Check it's a block server,
    if (!machine_p.isBlock()) {
      throw new NetworkAdminException(
                               "Machine '" + block + "' is not a block role");
    }

    MessageStream msg_out = new MessageStream(7);
    msg_out.addMessage("serverGUID");
    msg_out.addServiceAddress(block);
    msg_out.closeMessage();

    Message m = commandBlock(block, msg_out);
    if (m.isError()) {
      throw new NetworkAdminException(m);
    }

    // Return the GUID
    return (Long) m.param(0);
  }


  /**
   * Given a block id, queries the manager server database and returns the list
   * of all the block servers that contain the block. This is exactly the same
   * function used by the tree system for block lookup.
   */
  public ServiceAddress[] getBlockServerList(BlockId block_id)
                                                 throws NetworkAdminException {

    inspectNetwork();

    // Get the current manager server,
    MachineProfile[] mans = getManagerServers();
    if (mans.length == 0) {
      throw new NetworkAdminException("No manager server found");
    }
    // Query the first one,
    ServiceAddress manager_server = mans[0].getServiceAddress();

    MessageStream msg_out = new MessageStream(7);
    msg_out.addMessage("getServerList");
    msg_out.addBlockId(block_id);
    msg_out.closeMessage();

    Message m = commandManager(manager_server, msg_out);
    if (m.isError()) {
      throw new NetworkAdminException(m);
    }

    int count = (Integer) m.param(0);
    ServiceAddress[] addrs = new ServiceAddress[count];
    for (int i = 0; i < count; ++i) {
      addrs[i] = (ServiceAddress) m.param(1 + (i * 2));
    }

    return addrs;

  }

  /**
   * Tells the block server at the given service address to preserve only
   * the nodes in the 'nodes_to_preserve' list. Any other nodes in the block
   * may safely be removed from the block file to free up system resources.
   */
  public long preserveNodesInBlock(ServiceAddress block_server,
                     BlockId block_id, List<NodeReference> nodes_to_preserve)
                                                 throws NetworkAdminException {

    // Turn the nodes list into a DataAddress array,
    DataAddress[] da_arr = new DataAddress[nodes_to_preserve.size()];
    int i = 0;
    for (NodeReference node : nodes_to_preserve) {
      da_arr[i] = new DataAddress(node);
      ++i;
    }

    // The message,
    MessageStream msg_out = new MessageStream(7);
    msg_out.addMessage("preserveNodesInBlock");
    msg_out.addBlockId(block_id);
    msg_out.addDataAddressArr(da_arr);
    msg_out.closeMessage();
    
    Message m = commandBlock(block_server, msg_out);
    if (m.isError()) {
      throw new NetworkAdminException(m);
    }

    // Return the process_id
    long process_id = (Long) m.param(0);
    return process_id;

  }
  

//  /**
//   * Return the number of block to block server mappings the manager server is
//   * currently managing.
//   */
//  public long getBlockMappingCount() throws NetworkAdminException {
//    inspectNetwork();
//
//    // Get the current manager server,
//    MachineProfile[] mans = getManagerServers();
//    if (mans.length == 0) {
//      throw new NetworkAdminException("No manager server found");
//    }
//    ServiceAddress manager_server = mans[0].getServiceAddress();
//
//    MessageStream msg_out = new MessageStream(7);
//    msg_out.addMessage("getBlockMappingCount");
//    msg_out.closeMessage();
//
//    Message m = commandManager(manager_server, msg_out);
//    if (m.isError()) {
//      throw new NetworkAdminException(m);
//    }
//
//    // Return the service address for the root server,
//    return (Long) m.param(0);
//  }
//
//  /**
//   * Return a range of block to block server mappings between index p1 and
//   * p2 in the list. The returned array is arranged as block_id->server guid
//   * pairs.
//   */
//  public long[] getBlockMappingRange(long p1, long p2)
//                                                throws NetworkAdminException {
//    inspectNetwork();
//
//    // Get the current manager server,
//    MachineProfile[] mans = getManagerServers();
//    if (mans.length == 0) {
//      throw new NetworkAdminException("No manager server found");
//    }
//    ServiceAddress manager_server = mans[0].getServiceAddress();
//
//    MessageStream msg_out = new MessageStream(7);
//    msg_out.addMessage("getBlockMappingRange");
//    msg_out.addLong(p1);
//    msg_out.addLong(p2);
//    msg_out.closeMessage();
//
//    Message m = commandManager(manager_server, msg_out);
//    if (m.isError()) {
//      throw new NetworkAdminException(m);
//    }
//
//    // Return the service address for the root server,
//    long[] map = (long[]) m.param(0);
//    return map;
//  }

  /**
   * Returns the status and address of all registered block servers from the
   * manager. The returned map is the ServiceAddress of a block server
   * associated with its status string. The status being a static from
   * DefaultManagerServer (eg. DefaultManagerServer.STATUS_UP).
   */
  public Map<ServiceAddress, String> getBlocksStatus()
                                                throws NetworkAdminException {
    inspectNetwork();

    // Get the current manager server,
    MachineProfile[] mans = getManagerServers();
    if (mans.length == 0) {
      throw new NetworkAdminException("No manager server found");
    }
    ServiceAddress manager_server = mans[0].getServiceAddress();

    MessageStream msg_out = new MessageStream(7);
    msg_out.addMessage("getRegisteredServerList");
    msg_out.closeMessage();

    Message m = commandManager(manager_server, msg_out);
    if (m.isError()) {
      throw new NetworkAdminException(m);
    }

    // The list of block servers registered with the manager,
    ServiceAddress[] regservers = (ServiceAddress[]) m.param(0);
    String[] regservers_status = (String[]) m.param(1);

    HashMap<ServiceAddress, String> map = new HashMap();
    for (int i = 0; i < regservers.length; ++i) {
      map.put(regservers[i], regservers_status[i]);
    }

    // Return the map,
    return map;
  }

  /**
   * Contacts the current manager server and changes the database to make a
   * block_id -> server_guid association for that block. After this call,
   * the manager server will return the given server as a container for the
   * given block_id.
   */
  public void addBlockAssociation(BlockId block_id, long server_guid)
                                                 throws NetworkAdminException {
    inspectNetwork();

    // Get the current manager server,
    MachineProfile[] mans = getManagerServers();
    if (mans.length == 0) {
      throw new NetworkAdminException("No manager server found");
    }

    ServiceAddress[] manager_servers = new ServiceAddress[mans.length];
    for (int i = 0; i < mans.length; ++i) {
      manager_servers[i] = mans[i].getServiceAddress();
    }

    // NOTE: This command will be propogated through all the other managers on
    //   the network by the manager.
    MessageStream msg_out = new MessageStream(7);
    msg_out.addMessage("internalAddBlockServerMapping");
    msg_out.addBlockId(block_id);
    msg_out.addLongArray(new long[] { server_guid });
    msg_out.closeMessage();

    // Send the command to all the managers, if all fail throw an exception.
    boolean success = false;
    Message last_error = null;
    for (int i = 0; i < manager_servers.length; ++i) {
      Message m = commandManager(manager_servers[i], msg_out);
      if (m.isError()) last_error = m;
      else success = true;
    }
    if (!success) {
      throw new NetworkAdminException(last_error);
    }
  }

  /**
   * Contacts the current manager server and change the database to remove
   * a block_id -> server_guid association. After this call, the manager
   * server will no longer return the given server as a container for the
   * given block_id.
   */
  public void removeBlockAssociation(BlockId block_id, long server_guid)
                                                 throws NetworkAdminException {
    inspectNetwork();

    // Get the current manager server,
    MachineProfile[] mans = getManagerServers();
    if (mans.length == 0) {
      throw new NetworkAdminException("No manager server found");
    }

    ServiceAddress[] manager_servers = new ServiceAddress[mans.length];
    for (int i = 0; i < mans.length; ++i) {
      manager_servers[i] = mans[i].getServiceAddress();
    }

    // NOTE: This command will be propogated through all the other managers on
    //   the network by the manager.
    MessageStream msg_out = new MessageStream(7);
    msg_out.addMessage("internalRemoveBlockServerMapping");
    msg_out.addBlockId(block_id);
    msg_out.addLongArray(new long[] { server_guid });
    msg_out.closeMessage();

    // Send the command to all the managers, if all fail throw an exception.
    boolean success = false;
    Message last_error = null;
    for (int i = 0; i < manager_servers.length; ++i) {
      Message m = commandManager(manager_servers[i], msg_out);
      if (m.isError()) last_error = m;
      else success = true;
    }
    if (!success) {
      throw new NetworkAdminException(last_error);
    }
  }

  /**
   * Returns the list of all blocks stored on a block server, reported by the
   * block server.
   */
  public BlockId[] getBlockList(ServiceAddress block)
                                                throws NetworkAdminException {
    inspectNetwork();

    // Check machine is in the schema,
    MachineProfile machine_p = checkMachineInNetwork(block);
    // Check it's a block server,
    if (!machine_p.isBlock()) {
      throw new NetworkAdminException(
                               "Machine '" + block + "' is not a block role");
    }

    MessageStream msg_out = new MessageStream(7);
    msg_out.addMessage("blockSetReport");
    msg_out.closeMessage();

    Message m = commandBlock(block, msg_out);
    if (m.isError()) {
      throw new NetworkAdminException(m);
    }

    // Return the block list,
    return (BlockId[]) m.param(1);
  }

  /**
   * Returns the analytics history stats for the given server on the network.
   * Note that the stats object could be large (a days worth of analytics
   * at 1 min timeframe is about 64kb of data).
   */
  public long[] getAnalyticsStats(ServiceAddress server)
                                                throws NetworkAdminException {

    MessageStream msg_out = new MessageStream(7);
    msg_out.addMessage("reportStats");
    msg_out.closeMessage();
    Message m = commandAdmin(server, msg_out);
    if (m.isError()) {
      throw new NetworkAdminException(m);
    }

    long[] stats = (long[]) m.param(0);
    return stats;
  }

  /**
   * Issues a command to the given block server to send the given block_id
   * to the destination block server.
   */
  public void processSendBlock(BlockId block_id,
                               ServiceAddress source_block_server,
                               ServiceAddress dest_block_server,
                               long dest_server_sguid)
                                                throws NetworkAdminException {
    inspectNetwork();

    // Get the current manager server,
    MachineProfile[] mans = getManagerServers();
    if (mans.length == 0) {
      throw new NetworkAdminException("No manager server found");
    }

    // Use the manager servers,
    ServiceAddress[] manager_servers = new ServiceAddress[mans.length];
    for (int i = 0; i < mans.length; ++i) {
      manager_servers[i] = mans[i].getServiceAddress();
    }

//    ServiceAddress manager_server = mans[0].getServiceAddress();

    MessageStream msg_out = new MessageStream(6);
    msg_out.addMessage("sendBlockTo");
    msg_out.addBlockId(block_id);
    msg_out.addServiceAddress(dest_block_server);
    msg_out.addLong(dest_server_sguid);
    msg_out.addServiceAddressArr(manager_servers);
    msg_out.closeMessage();

    Message m = commandBlock(source_block_server, msg_out);
    if (m.isError()) {
      throw new NetworkAdminException(m);
    }

  }


  /**
   * Returns the debug string from the manager.
   */
  public String getManagerDebugString(ServiceAddress manager_server)
                                                throws NetworkAdminException {

    MessageStream msg_out = new MessageStream(3);
    msg_out.addMessage("debugString");
    msg_out.closeMessage();

    Message m = commandManager(manager_server, msg_out);
    if (m.isError()) {
      throw new NetworkAdminException(m);
    }

    return (String) m.param(0);
  }


  // ----- Statics

  /**
   * Creates a NetworkProfile in which requests are connected via a TCP
   * connection for the service.
   */
  public static NetworkProfile tcpConnect(String network_password) {
    NetworkConnector connector = new TCPNetworkConnector(network_password);
    NetworkProfile network_profile =
                               new NetworkProfile(connector, network_password);
    return network_profile;
  }

}
