/**
 * com.mckoi.appcore.NetworkAccess  Sep 29, 2012
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
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * An interface for accessing the network infrastructure. This allows for
 * different implementations of the administration tool to access the network
 * in a secure way.
 *
 * @author Tobias Downer
 */

public interface NetworkAccess {

  /**
   * Queries the machine at the given ServiceAddress and returns true if the
   * machine is a valid Mckoi machine node.
   */
  boolean isValidMckoiNode(ServiceAddress machine);
  
  /**
   * Returns a machine list of all nodes in the network sorted by the ip/port
   * address.
   */
  List<ServiceAddress> sortedServerList();

  /**
   * Refreshes this profile by inspecting the network and discovering any
   * changes to state.
   */
  void refresh();
  
  /**
   * Refresh the network configuration resource.
   */
  void refreshNetworkConfig() throws IOException;
  
  /**
   * Returns true if the given ServiceAddress is a machine node that is part of
   * the network.
   */
  boolean isMachineInNetwork(ServiceAddress machine);
  
  /**
   * Returns a MachineProfile object of the machine in the network with the
   * given ServiceAddress, or null if there is no machine in the schema with
   * the given address.
   */
  MachineProfile getMachineProfile(ServiceAddress address);
  
  /**
   * Returns the current manager servers on the network, from the profile, or
   * null if there's current no manager assigned.
   */
  MachineProfile[] getManagerServers();

  /**
   * Returns the set of all root servers in the network, from the profile,
   * or an empty array if no root servers discovered.
   */
  MachineProfile[] getRootServers();

  /**
   * Returns the set of all block servers in the network, from the profile,
   * or an empty array if no block servers discovered.
   */
  MachineProfile[] getBlockServers();

  /**
   * Returns a list of all machine profiles discovered on the network.
   */
  MachineProfile[] getAllMachineProfiles();

  /**
   * Assign a machine to a manager server role. Generates an exception if
   * there is already a manager server.
   * <p>
   * Note that this does not update service registration of nodes in the
   * network.
   */
  void startManager(ServiceAddress machine) throws NetworkAdminException;
  
  /**
   * Relieve a machine from the manager server role. Generates an exception if
   * the machine is not assigned as manager server.
   * <p>
   * Note that this does not update service registration of nodes in the
   * network.
   */
  void stopManager(ServiceAddress machine) throws NetworkAdminException;

  /**
   * Assign a machine to a root server role. Generates an exception if
   * the machine is already assigned to be a root.
   * <p>
   * Note that this does not update service registration of nodes in the
   * network.
   */
  void startRoot(ServiceAddress machine) throws NetworkAdminException;

  /**
   * Relieve a machine from a root server role. Generates an exception if
   * the machine is not assigned as a root server.
   * <p>
   * Note that this does not update service registration of nodes in the
   * network.
   */
  void stopRoot(ServiceAddress machine) throws NetworkAdminException;
  
  /**
   * Assign a machine to a block server role. Generates an exception if
   * the machine is already assigned to be a block server.
   * <p>
   * Note that this does not update service registration of nodes in the
   * network.
   */
  void startBlock(ServiceAddress machine) throws NetworkAdminException;

  /**
   * Relieve a machine from a block server role. Generates an exception if
   * the machine is not assigned as a block server.
   * <p>
   * Note that this does not update service registration of nodes in the
   * network.
   */
  void stopBlock(ServiceAddress machine) throws NetworkAdminException;

  /**
   * Registers a manager with the current managers assigned on the network.
   */
  void registerManager(ServiceAddress manager) throws NetworkAdminException;

  /**
   * Registers a manager with the current managers assigned on the network.
   */
  void deregisterManager(ServiceAddress root) throws NetworkAdminException;

  /**
   * Contacts a root server and registers it to the current manager server
   * assigned on the network.
   */
  void registerRoot(ServiceAddress root) throws NetworkAdminException;

  /**
   * Contacts a root server and deregisters it from the current manager server
   * assigned on the network.
   */
  void deregisterRoot(ServiceAddress root) throws NetworkAdminException;

  /**
   * Contacts the current manager server assigned on the network and registers
   * a block server to it.
   */
  void registerBlock(ServiceAddress block) throws NetworkAdminException;

  /**
   * Contacts the current manager server assigned on the network and
   * deregisters the given block server from it.
   */
  void deregisterBlock(ServiceAddress block) throws NetworkAdminException;

  /**
   * Returns the list of all path names from all root servers registered on the
   * network (ordered in no significant way).
   */
  String[] getAllPathNames() throws NetworkAdminException;

  /**
   * Returns the PathInfo for the given path name, or null if the path is not
   * defined.
   */
  PathInfo getPathInfoForPath(String path_name) throws NetworkAdminException;

  /**
   * Adds a path/root server map to the network. This sends the path map to
   * the first available manager server.
   */
  void addPathToNetwork(String path_name, String consensus_fun,
                  ServiceAddress root_leader, ServiceAddress[] root_servers)
                                                  throws NetworkAdminException;

  /**
   * Removes a path/root server map from the network. This sends the path map
   * removal command from the first available manager server.
   */
  void removePathFromNetwork(String path_name,
                      ServiceAddress root_server) throws NetworkAdminException;

  /**
   * Returns an array of DataAddress that represent the snapshots stored on the
   * given path at the time of the given timestamp. This will always return
   * DataAddress object regardless of whether any commits happened at the
   * given time or not. The returned DataAddress objects will be snapshots
   * at roughly the time given.
   */
  DataAddress[] getHistoricalPathRoots(ServiceAddress root,
                   PathInfo path_info,
                   long timestamp, int max_count) throws NetworkAdminException;

  /**
   * Sets the root for the given path name by issuing a 'publish' command on
   * the root server. Great care should be taken when using this function
   * because it bypasses all commit checks.
   */
  void setPathRoot(ServiceAddress root, PathInfo path_info,
                   DataAddress address) throws NetworkAdminException;

  /**
   * Returns the stats string for the given path name on the given root server.
   */
  String getPathStats(PathInfo path_info) throws NetworkAdminException;

  /**
   * Returns the GUID of a block server.
   */
  long getBlockGUID(ServiceAddress block) throws NetworkAdminException;

  /**
   * Given a block id, queries the manager server database and returns the list
   * of all the block servers that contain the block. This is exactly the same
   * function used by the tree system for block lookup.
   */
  ServiceAddress[] getBlockServerList(BlockId block_id)
                                                  throws NetworkAdminException;

  /**
   * Tells the block server at the given service address to preserve only
   * the nodes in the 'nodes_to_preserve' list. Any other nodes in the block
   * may safely be removed from the block file to free up system resources.
   */
  long preserveNodesInBlock(ServiceAddress block_server,
                     BlockId block_id, List<NodeReference> nodes_to_preserve)
                                                  throws NetworkAdminException;

  /**
   * Returns the status and address of all registered block servers from the
   * manager. The returned map is the ServiceAddress of a block server
   * associated with its status string. The status being a static from
   * DefaultManagerServer (eg. DefaultManagerServer.STATUS_UP).
   */
  Map<ServiceAddress, String> getBlocksStatus() throws NetworkAdminException;

  /**
   * Contacts the current manager server and changes the database to make a
   * block_id -> server_guid association for that block. After this call,
   * the manager server will return the given server as a container for the
   * given block_id.
   */
  void addBlockAssociation(BlockId block_id, long server_guid)
                                                  throws NetworkAdminException;

  /**
   * Contacts the current manager server and change the database to remove
   * a block_id -> server_guid association. After this call, the manager
   * server will no longer return the given server as a container for the
   * given block_id.
   */
  void removeBlockAssociation(BlockId block_id, long server_guid)
                                                  throws NetworkAdminException;

  /**
   * Returns the list of all blocks stored on a block server, reported by the
   * block server.
   */
  BlockId[] getBlockList(ServiceAddress block) throws NetworkAdminException;

  /**
   * Returns the analytics history stats for the given server on the network.
   * Note that the stats object could be large (a days worth of analytics
   * at 1 min timeframe is about 64kb of data).
   */
  long[] getAnalyticsStats(ServiceAddress server) throws NetworkAdminException;

  /**
   * Issues a command to the given block server to send the given block_id
   * to the destination block server.
   */
  void processSendBlock(BlockId block_id,
                        ServiceAddress source_block_server,
                        ServiceAddress dest_block_server,
                        long dest_server_sguid) throws NetworkAdminException;

  /**
   * Returns the debug string from the manager.
   */
  String getManagerDebugString(ServiceAddress manager_server)
                                                  throws NetworkAdminException;

}
