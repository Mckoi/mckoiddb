/**
 * com.mckoi.network.AdminInterpreter  Jul 4, 2009
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

package com.mckoi.network;

import com.mckoi.util.AnalyticsHistory;
import java.io.*;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An interpreter for performing administration functions, to be used by a
 * console feature. The interpreter takes a sequence of line separated
 * commands and performs the function by interacting with the nodes in the
 * network appropriately.
 *
 * @author Tobias Downer
 */

public class AdminInterpreter {

  /**
   * True if we display the prompt.
   */
  private final boolean display_prompt;

  /**
   * The input reader.
   */
  private final BufferedReader in;

  /**
   * The output writer.
   */
  private final PrintWriter out;

  /**
   * A cached network profile object.
   */
  private final NetworkProfile network_profile;

  /**
   * Constructs the interpreter.
   *
   * @param in the input reader of commands.
   * @param out the output writer to be displayed to the user.
   * @param network_profile the object for describing and interacting with the
   *   network.
   * @param display_prompt true if a command prompt should be displayed. If
   *   false, all input commands are also output to 'out'
   */
  public AdminInterpreter(Reader in, Writer out,
                          NetworkProfile network_profile,
                          boolean display_prompt) {

    this.display_prompt = display_prompt;
    this.network_profile = network_profile;
    this.in = new BufferedReader(in);
    this.out = new PrintWriter(out);
  }



  private String[] args;

  private boolean match(String str, String pattern) {
    Pattern p = Pattern.compile(pattern);
    Matcher m = p.matcher(str);
    boolean matches = m.matches();
    if (matches) {
      // Fetch the arguments,
      int count = m.groupCount();
      args = new String[count];
      for (int i = 0; i < count; ++i) {
        args[i] = m.group(i + 1);
      }
    }
    return matches;
  }

  /**
   * Memory report string.
   */
  private static String memoryReport(long used, long total) {
    StringBuilder b = new StringBuilder();

    String sz;
    double precision;

    if (total >= (1024L * 1024L * 1024L * 1024L)) {
      sz = " TB";
      precision = (1024L * 1024L * 1024L * 1024L);
    }
    else if (total >= (1024L * 1024L * 1024L)) {
      sz = " GB";
      precision = (1024L * 1024L * 1024L);
    }
    else {
      sz = " MB";
      precision = (1024L * 1024L);
    }

    double total_mb = ((double) total) / precision;
    double used_mb = ((double) used) / precision;
    BigDecimal showt =
            BigDecimal.valueOf(total_mb).setScale(1, BigDecimal.ROUND_HALF_UP);
    BigDecimal showu =
            BigDecimal.valueOf(used_mb).setScale(1, BigDecimal.ROUND_HALF_UP);

    b.append(showu.toString());
    b.append("/");
    b.append(showt.toString());
    b.append(sz);

    return b.toString();
  }


  private ServiceAddress parseMachineAddress(String machine) {
    try {
      return ServiceAddress.parseString(machine);
    }
    catch (IOException e) {
      out.println("Error parsing machine address: " + e.getMessage());
      throw new IntErrException();
    }
  }

  /**
   * Parses a comma separated list of service addresses.
   */
  private ServiceAddress[] parseMachineAddressList(String machine_list) {
    String[] machines = machine_list.split(",");

    try {
      ServiceAddress[] services = new ServiceAddress[machines.length];
      for (int i = 0; i < machines.length; ++i) {
        services[i] = ServiceAddress.parseString(machines[i].trim());
      }
      return services;
    }
    catch (IOException e) {
      out.println("Error parsing machine address: " + e.getMessage());
      throw new IntErrException();
    }
  }


//  /**
//   * Returns the NetworkProfile for this administration process.
//   */
//  private NetworkProfile getNetworkProfile() {
//    if (network_profile == null) {
//      NetworkConnector connector = new TCPNetworkConnector(network_password);
//      network_profile = new NetworkProfile(connector, network_password);
//    }
//    return network_profile;
//  }

  /**
   * Don't remove, legacy functions use this.
   *
   * @deprecated use AnalyticsHistory instead.
   */
  public static void printStatItem(PrintWriter out, long[] stats, int item_count) {
    AnalyticsHistory.printStatItem(out, stats, item_count);
  }


  /**
   * Displays an overview of the network.
   */
  public void showNetwork() {

    int manager_count = 0;
    int root_count = 0;
    int block_count = 0;

    out.println(" MRB RC  BC  Server");
    out.println("------------------------------------");
    out.flush();
    network_profile.refresh();

    MachineProfile[] profiles = network_profile.getAllMachineProfiles();

    for (MachineProfile p : profiles) {
      if (p.isError()) {
        out.print(" ??? ?   ?   ");
      }
      else {
        out.print(" ");
        out.print(p.isManager() ? "M" : ".");;
        out.print(p.isRoot() ? "R" : ".");;
        out.print(p.isBlock() ? "B" : ".");;
        out.print("         ");

        manager_count += p.isManager() ? 1 : 0;
        root_count += p.isRoot() ? 1 : 0;
        block_count += p.isBlock() ? 1 : 0;
      }

      out.println(p.getServiceAddress().displayString());

      if (p.isError()) {
        out.println("             " + p.getProblemMessage());
      }

      out.flush();
    }
    out.println();
    out.println(profiles.length + " machines in the network.");
    out.print(manager_count);
    out.print(" manager ");
    out.print(root_count);
    out.print(" root ");
    out.print(block_count);
    out.println(" block machines.");
    out.println();

  }

  /**
   * Displays node analytics.
   */
  public void showAnalytics() throws NetworkAdminException {

    out.flush();
    network_profile.refresh();

    MachineProfile[] profiles = network_profile.getAllMachineProfiles();

    for (MachineProfile p : profiles) {
      out.println(p.getServiceAddress().displayString());
      out.print("  ");
      if (p.isError()) {
        out.print("Error: ");
        out.println(p.getProblemMessage());
      }
      else {
        long[] stats = network_profile.getAnalyticsStats(p.getServiceAddress());
        if (stats.length < 4) {
          out.println("Sorry, no analytics available yet.");
        }
        else {
          AnalyticsHistory.printStatItem(out, stats, 1);
          out.print(" ");
          AnalyticsHistory.printStatItem(out, stats, 5);
          out.print(" ");
          AnalyticsHistory.printStatItem(out, stats, 15);
          out.println();
        }

      }

      out.flush();
    }
    out.println();

  }

  /**
   * Shows debug information of the manager cluster.
   */
  private void showManagerDebug() throws NetworkAdminException {

    out.flush();
    network_profile.refresh();

    MachineProfile[] managers = network_profile.getManagerServers();

    for (int i = 0; i < managers.length; ++i) {
      MachineProfile machine = managers[i];
      out.print("Manager: ");
      out.println(machine.getServiceAddress().displayString());
      out.println();
      if (!machine.isError()) {
        String str = network_profile.getManagerDebugString(
                                                 machine.getServiceAddress());
        out.println(str);
      }
      else {
        out.println("Error: " + machine.getProblemMessage());
      }
      out.flush();
    }

  }


  private void outputPathInfo(PathInfo p) throws NetworkAdminException {

    String path_name = p.getPathName();

    out.print("+Name: ");
    out.print(path_name);
    out.print(" (");
    out.print(p.getConsensusFunction());
    out.println(")");

    out.print(" Srvs: ");
    ServiceAddress leader = p.getRootLeader();
    ServiceAddress[] srvs = p.getRootServers();
    for (ServiceAddress srv : srvs) {
      boolean il = srv.equals(leader);
      if (il) out.print("[");
      out.print(srv.displayString());
      if (il) out.print("*]");
      out.print(" ");
    }
    out.println();

    out.print(" Status: ");
    try {
      String stats = network_profile.getPathStats(p);
      if (stats != null) {
        out.print(stats);
      }
    }
    catch (NetworkAdminException e) {
      out.print("Error retrieving stats: " + e.getMessage());
    }
    out.println();
    out.println();

  }


  /**
   * Displays the lists of paths defined on the system.
   */
  public void showPaths() throws NetworkAdminException {
    MachineProfile[] roots = network_profile.getRootServers();
    if (roots.length == 0) {
      out.println("No root servers available on the network.");
      return;
    }

    out.flush();
    network_profile.refresh();

    // Get all paths from the manager cluster,
    String[] path_names = network_profile.getAllPathNames();

    int count = 0;
    for (String path_name : path_names) {
      PathInfo path_info = network_profile.getPathInfoForPath(path_name);
      outputPathInfo(path_info);
      out.flush();
      ++count;
    }

    out.println();
    out.println("Path Count: " + count);
    out.flush();

  }

  /**
   * Displays an overview of the status of all servers.
   */
  public void showStatus() throws NetworkAdminException {
    out.println(" Status    Server");
    out.println("-----------------------------------------");
    out.flush();
    network_profile.refresh();

    Map<ServiceAddress, String> status_info = null;
    // Manager servers status,
    MachineProfile[] managers = network_profile.getManagerServers();
    if (managers.length > 0) {
      for (int i = 0; i < managers.length; ++i) {
        out.print(" ");
        out.print("UP        ");
        out.print("Manager: ");
        out.println(managers[i].getServiceAddress().displayString());

        try {
          status_info = network_profile.getBlocksStatus();
        }
        catch (NetworkAdminException e) {
          out.println("Error retrieving manager status info: " + e.getMessage());
        }
      }
    }
    else {
      out.println("! Manager server not available");
    }

    // Status of root servers
    MachineProfile[] roots = network_profile.getRootServers();
    if (roots.length == 0) {
      out.println("! Root servers not available");
    }
    for (MachineProfile r : roots) {
      out.print(" ");
      if (r.isError()) {
        out.print("DOWN      ");
      }
      else {
        out.print("UP        ");
      }
      out.print("Root: ");
      out.println(r.getServiceAddress().displayString());
      if (r.isError()) {
        out.print("  ");
        out.println(r.getProblemMessage());
      }

    }

    // The block servers we fetch from the map,
    ArrayList<ServiceAddress> blocks = new ArrayList();
    if (status_info != null) {
      for (ServiceAddress s : status_info.keySet()) {
        blocks.add(s);
      }
    }
    else {
      MachineProfile[] sblocks = network_profile.getBlockServers();
      for (MachineProfile b : sblocks) {
        blocks.add(b.getServiceAddress());
      }
    }
    Collections.sort(blocks);

    if (blocks.size() == 0) {
      out.println("! Block servers not available");
    }
    for (ServiceAddress b : blocks) {
      out.print(" ");
      if (status_info != null) {
        String status_str = status_info.get(b);
        if (status_str.equals(ServiceStatusTracker.STATUS_UP)) {
          // Manager reported up
          out.print(" UP       ");
        }
        else if (status_str.equals(ServiceStatusTracker.STATUS_DOWN_CLIENT_REPORT)) {
          // Manager reported down from client report of error
          out.print(" D-CR     ");
        }
        else if (status_str.equals(ServiceStatusTracker.STATUS_DOWN_HEARTBEAT)) {
          // Manager reported down from heart beat check on the server
          out.print(" D-HB     ");
        }
        else if (status_str.equals(ServiceStatusTracker.STATUS_DOWN_SHUTDOWN)) {
          // Manager reported down from shut down request
          out.print(" D-SD     ");
        }
        else {
          out.print(" ?ERR     ");
        }

      }
      else {
        // Try and get status from machine profile
        MachineProfile r = network_profile.getMachineProfile(b);
        if (r.isError()) {
          out.print("DOWN      ");
        }
        else {
          out.print("UP        ");
        }
      }

      out.print("Block: ");
      out.println(b.displayString());
//      if (r.isError()) {
//        out.print("  ");
//        out.println(r.getProblemMessage());
//      }
    }

    out.println();

  }


  /**
   * Output a block line from the 'showBlocks' report.
   */
  private void outputBlockLine(long block_id,
          ArrayList<Long> block_list, long[] sguid_bit_arr) {

    if (block_list.size() > 0) {
      String bid_str = String.valueOf(block_id);
      int pad = 10 - bid_str.length();
      pad = Math.max(pad, 1);
      StringBuilder padding = new StringBuilder();
      for (int i = 0; i < pad; ++i) {
        padding.append(' ');
      }

      out.print(bid_str);
      out.print(padding);

      for (int i = 0; i < sguid_bit_arr.length; ++i) {
        long sguid = sguid_bit_arr[i];
        if (block_list.contains(sguid)) {
          out.print("* ");
        }
        else {
          out.print("  ");
        }
      }

      out.println();
      out.flush();
    }

  }


  /**
   * Check the status of block_servers stored on the block servers,
   */
  private void blockMapProcess(BlockMapProcess process)
                                                throws NetworkAdminException {
    out.println("Processing...");
    out.flush();

    // Refresh
    network_profile.refresh();

    MachineProfile[] managers = network_profile.getManagerServers();
    if (managers.length == 0) {
      out.println("Error: Manager currently not available.");
      throw new IntErrException();
    }

    // Generate a map of server guid value to MachineProfile for that machine
    // node currently available.
    MachineProfile[] block_servers = network_profile.getBlockServers();
    long[] available_block_guids = new long[block_servers.length];

    HashMap<Long, MachineProfile> sguid_to_address = new HashMap();
    HashMap<ServiceAddress, Long> address_to_sguid = new HashMap();
    
    for (int i = 0; i < block_servers.length; ++i) {
      long server_guid =
            network_profile.getBlockGUID(block_servers[i].getServiceAddress());
      available_block_guids[i] = server_guid;
      sguid_to_address.put(available_block_guids[i], block_servers[i]);
      address_to_sguid.put(
               block_servers[i].getServiceAddress(), available_block_guids[i]);
    }
    Arrays.sort(available_block_guids);

    int block_server_count = block_servers.length;
    out.println("Block servers currently available: " + block_server_count);
    if (block_server_count < 3) {
      out.println(
          "WARNING: There are currently less than 3 block servers available.");
    }
    out.flush();

    // The map of block_id to list of server guids that contain the block,
    HashMap<BlockId, ArrayList<Long>> block_id_map = new HashMap();

    // For each block server,
    Set<Long> server_guids = sguid_to_address.keySet();
    for (long server_guid : server_guids) {
      MachineProfile block = sguid_to_address.get(server_guid);
      // Fetch the list of blocks for the server,
      BlockId[] block_ids =
                       network_profile.getBlockList(block.getServiceAddress());
      for (BlockId block_id : block_ids) {
        // Build the association,
        ArrayList<Long> list = block_id_map.get(block_id);
        if (list == null) {
          list = new ArrayList(5);
          block_id_map.put(block_id, list);
        }
        list.add(server_guid);
      }
    }

    // Now, 'block_id_map' contains the actual map of block id to servers as
    // reported by the block servers. We now need to compare this to the map
    // the manager server has.

    // For each block,
    for (BlockId block_id : block_id_map.keySet()) {

      // The servers the manager server has on record for this block,
      ServiceAddress[] query = network_profile.getBlockServerList(block_id);
      // Convert to SGUID list
      List<Long> servers_queried_for_block = new ArrayList();
      for (ServiceAddress saddr : query) {
        servers_queried_for_block.add(address_to_sguid.get(saddr));
      }

      // The list of servers we know holds the block from the network query,
      List<Long> server_guids_known_for_block = block_id_map.get(block_id);

      // Tell the process function about what we found,
      process.managerProcess(block_id,
                    servers_queried_for_block, server_guids_known_for_block,
                    sguid_to_address);
      
    }


//    // The total number of mappings recorded by the manager,
//    long count = network_profile.getBlockMappingCount();
//    BlockId[] m = network_profile.getBlockMappingRange(0, Long.MAX_VALUE);
//
//    BlockId bl_cur = null;
//    ArrayList<Long> list = new ArrayList();
//
//    for (int i = 0; i < m.length; i += 2) {
//      BlockId bl = m[i];      // the block id
//      long sg = m[i + 1];     // the server guid
//
//      if (bl_cur == null || bl != bl_cur) {
//        if (list.size() > 0) {
//          // Check this block,
//          ArrayList<Long> in_list = block_id_map.get(bl_cur);
//          process.managerProcess(bl_cur, list, in_list, sguid_to_address);
//        }
//        list.clear();
//        bl_cur = bl;
//      }
//      list.add(sg);
//    }

    // For each block,
    Set<BlockId> block_ids = block_id_map.keySet();
    int min_block_threshold = Math.min(3, Math.max(block_server_count, 1));
    for (BlockId block_id : block_ids) {
      ArrayList<Long> block_id_on_sguids = block_id_map.get(block_id);
      ArrayList<Long> available_sguids = new ArrayList();
      for (long block_sguid : block_id_on_sguids) {
        if (Arrays.binarySearch(available_block_guids, block_sguid) >= 0) {
          available_sguids.add(block_sguid);
        }
      }

      process.process(block_id, available_sguids,
                      available_block_guids, min_block_threshold,
                      sguid_to_address);

    }

  }

  /**
   * Produces a summary of block_servers on the network.
   */
  void checkBlockStatus() throws NetworkAdminException {
    blockMapProcess(new BlockMapProcess() {

      @Override
      public void managerProcess(BlockId block_id,
                                 List<Long> manager_queried_servers,
                                 List<Long> actual_known_sguids,
                                 Map<Long, MachineProfile> sguid_to_address)
                                                 throws NetworkAdminException {

        if (actual_known_sguids == null) {
          // This means this block_id is referenced by the manager but there
          // are no blocks available that currently store it.

          // ISSUE: This is fairly normal and will happen when the manager
          //   server is restarted.
//          out.println("Manager has association for block " + block_id);
//          out.println("But there are no actual block servers that report they hold the block.");
        }
        else {
          for (long sguids : manager_queried_servers) {
            if (!actual_known_sguids.contains(sguids)) {
              out.print("Manager has a block_id association for ");
              out.println(block_id + " -> " +
                        sguid_to_address.get(sguids).getServiceAddress().displayString());
              out.println("But, the block server says it doesn't hold this block.");
            }
          }
          for (long sguids : actual_known_sguids) {
            if (!manager_queried_servers.contains(sguids)) {
              out.print("Server ");
              out.print(sguid_to_address.get(sguids).getServiceAddress().displayString());
              out.print(" says it holds block ");
              out.println(block_id);
              out.println("But, the manager server doesn't have this association.");
            }
          }
        }
      }

      @Override
      public void process(BlockId block_id,
                          List<Long> available_sguids_containing_block_id,
                          long[] available_block_servers,
                          long min_threshold,
                          Map<Long, MachineProfile> sguid_to_address)
                                                throws NetworkAdminException {

        int block_availability = available_sguids_containing_block_id.size();
        // If a block has 0 availability,
        if (block_availability == 0) {
          out.println("ERROR: Block " + block_id + " currently has 0 availability!");
          out.println("  A block server containing a copy of this block must be added on the network.");
        }
        else if (block_availability < min_threshold) {
          out.println("Block " + block_id +
                      " available on less than " +
                      min_threshold + " servers (availability = " +
                      block_availability + ")");
        }
      }

    });
  }

  /**
   * Sends commands to block servers to attempt to fix block availability
   * issues.
   */
  void fixBlockAvailability() throws NetworkAdminException {
    final Random r = new Random();

    blockMapProcess(new BlockMapProcess() {

      @Override
      public void managerProcess(BlockId block_id,
                                 List<Long> manager_queried_servers,
                                 List<Long> actual_known_sguids,
                                 Map<Long, MachineProfile> sguid_to_address)
                                                 throws NetworkAdminException {
        if (actual_known_sguids == null) {
          // This means this block_id is referenced by the manager but there
          // are no blocks available that currently store it.

          // ISSUE: This is fairly normal and will happen when the manager
          //   server is restarted.

        }
        else {
          for (long sguid : manager_queried_servers) {
            if (!actual_known_sguids.contains(sguid)) {
              // Manager has a block_id association to a block server that
              // doesn't hold the record. The association that should be
              // removed is 'block_id -> sguid'

              out.println("Removing block association: " + block_id + " -> " +
                      sguid_to_address.get(sguid).getServiceAddress().displayString());
              network_profile.removeBlockAssociation(block_id, sguid);

            }
          }
          for (long sguid : actual_known_sguids) {
            if (!manager_queried_servers.contains(sguid)) {
              // Manager doesn't have a block_id association that it should
              // have. The association made is 'block_id -> sguid'

              out.println("Adding block association: " + block_id + " -> " +
                      sguid_to_address.get(sguid).getServiceAddress().displayString());
              network_profile.addBlockAssociation(block_id, sguid);
            }
          }
        }
      }

      @Override
      public void process(BlockId block_id,
                          List<Long> available_sguids_containing_block_id,
                          long[] available_block_servers,
                          long min_threshold,
                          Map<Long, MachineProfile> sguid_to_address)
                                                throws NetworkAdminException {
        int block_availability = available_sguids_containing_block_id.size();
        // If a block has 0 availability,
        if (block_availability == 0) {
          out.println("ERROR: Block " + block_id + " currently has 0 availability!");
          out.println("  I can't fix this - A block server containing a copy of this block needs to be added on the network.");
        }
        else if (block_availability < min_threshold) {
          // The set of all block servers we can copy the block to,
          ArrayList<Long> dest_block_servers =
                                 new ArrayList(available_block_servers.length);
          // Of all the block servers, find the list of servers that don't
          // contain the block_id
          for (long available_server : available_block_servers) {
            boolean use_server = true;
            for (long server : available_sguids_containing_block_id) {
              if (server == available_server) {
                use_server = false;
                break;
              }
            }
            if (use_server) {
              dest_block_servers.add(available_server);
            }
          }

          long source_sguid = available_sguids_containing_block_id.get(0);
          MachineProfile source_server = sguid_to_address.get(source_sguid);

          // Pick servers to bring the availability of the block to the ideal
          // value of 3.

          int ideal_count = (int) (min_threshold - block_availability);
          for (int i = 0; i < ideal_count; ++i) {
            int lid = r.nextInt(dest_block_servers.size());
            long dest_server_sguid = dest_block_servers.get(lid);
            dest_block_servers.remove(lid);
            MachineProfile dest_server = sguid_to_address.get(dest_server_sguid);

            out.print("Copying ");
            out.print(block_id);
            out.print(" from ");
            out.print(source_server.getServiceAddress());
            out.print(" to ");
            out.print(dest_server.getServiceAddress());
            out.println(".");

            // Send the command to copy,
            network_profile.processSendBlock(block_id,
                                          source_server.getServiceAddress(),
                                          dest_server.getServiceAddress(),
                                          dest_server_sguid);
          }

          out.println("block " + block_id + " can be copied to: " +
                      dest_block_servers);
        }
      }
    });
  }

//  /**
//   * Diagramatically show all the block_servers stored over all the block servers.
//   */
//  public void showBlocks(long limit) throws NetworkAdminException {
//    // Refresh
//    network_profile.refresh();
//
//    MachineProfile[] managers = network_profile.getManagerServers();
//    if (managers.length == 0) {
//      out.println("Error: Manager currently not available.");
//      throw new IntErrException();
//    }
//
//    // Generate a map of server guid value to MachineProfile for that machine
//    // node currently available.
//    MachineProfile[] blocks = network_profile.getBlockServers();
//    long[] bit_arr = new long[blocks.length];
////    HashMap<Long, MachineProfile> sguid_map = new HashMap();
//    for (int i = 0; i < blocks.length; ++i) {
//      bit_arr[i] = network_profile.getBlockGUID(blocks[i].getServiceAddress());
////      sguid_map.put(
////              network_profile.getBlockGUID(block_servers[i].getServiceAddress()),
////              block_servers[i]);
//    }
//
//    out.println("Current block availability");
//    out.println("--------------------------");
//
//    // The total number of mappings recorded by the manager,
//    long count = network_profile.getBlockMappingCount();
//    long[] m = network_profile.getBlockMappingRange(0, Long.MAX_VALUE);
//
//    // The ordered list of server guids to display on a line
//    ArrayList<Long> block_line = new ArrayList();
//    long current_block = -1;
//
//    for (int i = 0; i < m.length; i += 2) {
//      long bl = m[i];      // the block id
//      long sg = m[i + 1];  // the server guid
//
//      if (bl > current_block) {
//        // Flush current line,
//        outputBlockLine(current_block, block_line, bit_arr);
//        block_line.clear();
//
////        if (block_line.size() > 0) {
////          out.print(current_block);
////          out.print(" -> ");
////          out.print(block_line.toString());
////          out.println();
////          block_line.clear();
////        }
//
//        current_block = bl;
//      }
//      block_line.add(sg);
//    }
//
//    // Flush current line,
//    outputBlockLine(current_block, block_line, bit_arr);
//    block_line.clear();
////    if (block_line.size() > 0) {
////      out.print(current_block);
////      out.print(" -> ");
////      out.print(block_line.toString());
////      out.println();
////      block_line.clear();
////    }
//
//
////    out.println("Total mappings = " + count);
////    out.println("m.length = " + m.length);
//
//  }

  /**
   * Shows usage of storage resources on the machine node (memory and disk).
   */
  public void showFree() {
    // Refresh
    network_profile.refresh();

    MachineProfile[] machines = network_profile.getAllMachineProfiles();
    if (machines.length == 0) {
      out.println("No machines in the network.");
    }
    else {
      for (MachineProfile m : machines) {
        out.print("+Machine: ");
        if (m.isError()) {
          out.println(" Error: " + m.getProblemMessage());
        }
        else {
          out.println(m.getServiceAddress().displayString());
          out.print(" Used Heap: ");
          out.print(memoryReport(m.getHeapUsed(), m.getHeapTotal()));
          out.print(" Used Storage: ");
          out.println(memoryReport(m.getStorageUsed(), m.getStorageTotal()));
          if (m.getStorageUsed() > ((double)m.getStorageTotal() * 0.85d)) {
            out.println(" WARNING: Node is close to full - used storage within 85% of total");
          }
        }
        out.println();
        out.flush();
      }
      out.println();
    }

  }

//  /**
//   * Add or remove a machine from the network schema.
//   */
//  public void addMachine(String arg) {
//    ServiceAddress address = parseMachineAddress(arg);
//    out.println("Adding machine to schema: " + address.displayString());
//    out.flush();
//
//    // Check this is a valid machine node with the right challenge password,
//    if (!network_profile.isValidMckoiNode(address)) {
//      out.println("Error: This machine is either not a Mckoi machine node or the network");
//      out.println("  challenge password for this node is not what is expected.");
//      throw new IntErrException();
//    }
//    else {
//      network_profile.addMachineNode(address);
//      out.println("Done.");
//
//      try {
//        Writer fout = new FileWriter(local_schema_file);
//        network_profile.writeNetworkSchema(fout);
//        fout.close();
//      }
//      catch (IOException e) {
//        out.println("Error: Writing network schema file: " + local_schema_file);
//        e.printStackTrace(out);
//        throw new IntErrException();
//      }
//    }
//
//  }
//
//  /**
//   * Add or remove a machine from the network schema.
//   */
//  public void removeMachine(String arg) {
//    ServiceAddress address = parseMachineAddress(arg);
//    out.println("Removing machine from schema: " + address.displayString());
//    out.flush();
//
//    // Fetch the MachineProfile for this address,
//    MachineProfile p = network_profile.getMachineProfile(address);
//    if (p == null) {
//      out.println("Error: Machine was not found in the network schema.");
//      throw new IntErrException();
//    }
//    // Check if it's assigned to any roles,
//    if (!p.isNotAssigned()) {
//      out.println("Error: Can not remove machine from schema because it has at least one role.");
//      out.println("  To remove this machine, first relieve it of its role(s).");
//      throw new IntErrException();
//    }
//
//    network_profile.removeMachineNode(address);
//    out.println("Done.");
//
//    try {
//      Writer fout = new FileWriter(local_schema_file);
//      network_profile.writeNetworkSchema(fout);
//      fout.close();
//    }
//    catch (IOException e) {
//      out.println("Error: Writing network schema file: " + local_schema_file);
//      e.printStackTrace(out);
//      throw new IntErrException();
//    }
//  }

  /**
   * Assigns a role to a machine in the network.
   */
  public void startRole(String role, String machine)
                                                throws NetworkAdminException {
    ServiceAddress address = parseMachineAddress(machine);
    out.println("Starting role " + role + " on " + address.displayString());
    out.flush();

    MachineProfile p = network_profile.getMachineProfile(address);
    if (p == null) {
      out.println("Error: Machine was not found in the network schema.");
      throw new IntErrException();
    }

    // Here we have some rules,
    // 1. There must be a manager server assigned before block and roots can be
    //    assigned.

    MachineProfile[] current_managers = network_profile.getManagerServers();
    if (!role.equals("manager")) {
      if (current_managers.length == 0) {
        out.println("Error: Can not assign block or root role when no manager is available on the");
        out.println("  network.");
        throw new IntErrException();
      }
    }

    // Check if the machine is already performing the role,
    boolean already_doing_it = false;
    if (role.equals("block")) {
      already_doing_it = p.isBlock();
    }
    else if (role.equals("manager")) {
      already_doing_it = p.isManager();
    }
    else if (role.equals("root")) {
      already_doing_it = p.isRoot();
    }
    else {
      throw new RuntimeException("Unknown role: " + role);
    }

    if (already_doing_it) {
      out.println("Error: The machine is already assigned to the " + role + " role.");
      throw new IntErrException();
    }

    // Perform the assignment,
    if (role.equals("block")) {
      network_profile.startBlock(address);
      network_profile.registerBlock(address);
    }
    else if (role.equals("manager")) {
      network_profile.startManager(address);
      network_profile.registerManager(address);
    }
    else if (role.equals("root")) {
      network_profile.startRoot(address);
      network_profile.registerRoot(address);
    }
    else {
      throw new RuntimeException("Unknown role: " + role);
    }
    out.println("Done.");

  }

  /**
   * Relieves a role from a machine in the network.
   */
  public void stopRole(String role, String machine)
                                                 throws NetworkAdminException {
    ServiceAddress address = parseMachineAddress(machine);
    out.println("Stopping role " + role + " on " + address.displayString());
    out.flush();

    MachineProfile p = network_profile.getMachineProfile(address);
    if (p == null) {
      out.println("Error: Machine was not found in the network schema.");
      throw new IntErrException();
    }

    // Here we have some rules,
    // 1. The manager can not be relieved until all block and root servers have
    //    been.

    MachineProfile[] current_managers = network_profile.getManagerServers();
    MachineProfile[] current_roots = network_profile.getRootServers();
    MachineProfile[] current_blocks = network_profile.getBlockServers();
    if (role.equals("manager") && current_managers.length == 1) {
      if (current_roots.length > 0 ||
          current_blocks.length > 0) {
        out.println("Error: Can not relieve manager role when there are existing block and root");
        out.println("  assignments.");
        throw new IntErrException();
      }
    }

    // Check that the machine is performing the role,
    boolean is_performing = false;
    if (role.equals("block")) {
      is_performing = p.isBlock();
    }
    else if (role.equals("manager")) {
      is_performing = p.isManager();
    }
    else if (role.equals("root")) {
      is_performing = p.isRoot();
    }
    else {
      throw new RuntimeException("Unknown role: " + role);
    }

    if (!is_performing) {
      out.println("Error: The machine is not assigned to the " + role + " role.");
      throw new IntErrException();
    }

    // Perform the assignment,
    if (role.equals("block")) {
      network_profile.deregisterBlock(address);
      network_profile.stopBlock(address);
    }
    else if (role.equals("manager")) {
      network_profile.deregisterManager(address);
      network_profile.stopManager(address);
    }
    else if (role.equals("root")) {
      network_profile.deregisterRoot(address);
      network_profile.stopRoot(address);
    }
    else {
      throw new RuntimeException("Unknown role: " + role);
    }
    out.println("Done.");
  }

  /**
   * Adds a new path to the network.
   */
  public void addPath(String consensus_fun, String path_name, String machine)
                                                 throws NetworkAdminException {

    ServiceAddress[] addresses = parseMachineAddressList(machine);

    // Check no duplicates in the list,
    boolean duplicate_found = false;
    for (int i = 0; i < addresses.length; ++i) {
      for (int n = i + 1; n < addresses.length; ++n) {
        if (addresses[i].equals(addresses[n])) {
          duplicate_found = true;
        }
      }
    }

    if (duplicate_found) {
      out.println("Error: Duplicate root server in definition");
      throw new IntErrException();
    }

    out.println("Adding path " + consensus_fun + " " + path_name + ".");
    out.print("Path Info ");
    out.print("Leader: " + addresses[0].displayString());
    out.print(" Replicas: ");
    for (int i = 1; i < addresses.length; ++i) {
      out.print(addresses[i]);
      out.print(" ");
    }
    out.println();
    out.flush();

    for (int i = 0; i < addresses.length; ++i) {
      MachineProfile p = network_profile.getMachineProfile(addresses[i]);
      if (p == null) {
        out.println("Error: Machine was not found in the network schema.");
        throw new IntErrException();
      }
      if(!p.isRoot()) {
        out.println("Error: Given machine is not a root.");
        throw new IntErrException();
      }
    }

    // Add the path,
    network_profile.addPathToNetwork(
                            path_name, consensus_fun, addresses[0], addresses);
    out.println("Done.");
  }

  /**
   * Removes a path from the given root server.
   */
  public void removePath(String path_name, String machine)
                                                 throws NetworkAdminException {
    ServiceAddress address;
    // If machine is null, we need to find the machine the path is on,
    if (machine == null) {
      PathInfo path_info = network_profile.getPathInfoForPath(path_name);
      if (path_info == null) {
        out.println("The path '" + path_name + "' was not found.");
        throw new IntErrException();
      }
      address = path_info.getRootLeader();
    }
    else {
      address = parseMachineAddress(machine);
    }
    out.println("Removing path " + path_name +
                " from root " + address.displayString());
    out.flush();

    MachineProfile p = network_profile.getMachineProfile(address);
    if (p == null) {
      out.println("Error: Machine was not found in the network schema.");
      throw new IntErrException();
    }
    if(!p.isRoot()) {
      out.println("Error: Given machine is not a root.");
      throw new IntErrException();
    }

    // Remove the path,
    network_profile.removePathFromNetwork(path_name, address);
    out.println("Done.");

  }

  /**
   * Locates the root server that currently represents the given path name.
   */
  public void locatePath(String path_name) throws NetworkAdminException {

    PathInfo path_info = network_profile.getPathInfoForPath(path_name);
    if (path_info == null) {
      out.println("The path '" + path_name + "' was not found.");
      throw new IntErrException();
    }
    ServiceAddress address = path_info.getRootLeader();

    out.print("Root " + address.displayString());
    out.println(" is managing path " + path_name);
    out.flush();
  }



  private void rollbackPathToTime(String path_name, long timestamp)
                                    throws NetworkAdminException, IOException {

    PathInfo path_info = network_profile.getPathInfoForPath(path_name);
    if (path_info == null) {
      out.println("The path '" + path_name + "' was not found.");
      throw new IntErrException();
    }
    ServiceAddress address = path_info.getRootLeader();

    out.println("Reverting path " + path_name + " to " + new Date(timestamp));

    DataAddress[] data_addresses =
         network_profile.getHistoricalPathRoots(
                                           address, path_name, timestamp, 20);

    if (data_addresses.length == 0) {
      out.println("No historical roots found.");
      return;
    }
    else {
      out.println();
      out.println("Found the following roots:");
      for (DataAddress da : data_addresses) {
        out.println("  " + da.formatString());
      }
    }

    out.println();
    out.println("WARNING: Great care must be taken when rolling back a path. This");
    out.println(" operation is only intended as a way to recover from some types");
    out.println(" of corruption or other data inconsistency issues.");
    out.println(" Before agreeing to rollback the path, ensure there are no open");
    out.println(" writable transactions currently active on the path. A commit");
    out.println(" write on this path before this operation completes may undo the");
    out.println(" rollback or worse, put the path back into an inconsistent ");
    out.println(" state.");
    out.println();
    out.print("If you are sure you want to continue type YES (case-sensitive): ");
    out.flush();

    String approval = in.readLine();

    out.println();
    if (approval.equals("YES")) {
      network_profile.setPathRoot(address, path_name, data_addresses[0]);

//      network_profile.setCurrentPathRoot(address, path_name, data_addresses[0]);
      out.println("Done.");
    }
    else {
      out.println("Failed: Confirmation not given.");
    }

    out.flush();
  }



  private void processPathRollback(String lccmd)
                                   throws NetworkAdminException, IOException {
    String path_name = args[0];
    String timestamp = args[2];

    if (match(timestamp, "([0-9]+)\\s+hours")) {
      out.println("To revert " + args[0] + " hours.");

      int num_hours = Integer.parseInt(args[0]);

      long timems = System.currentTimeMillis();
      timems = timems - (num_hours * 60 * 60 * 1000);

      rollbackPathToTime(path_name, timems);

    }
    else {
      DateFormat[] df = new DateFormat[] {
        DateFormat.getDateTimeInstance(DateFormat.FULL, DateFormat.SHORT, Locale.US),
        DateFormat.getDateTimeInstance(DateFormat.LONG, DateFormat.SHORT, Locale.US),
        DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT, Locale.US),
        DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT, Locale.US),
      };

      // Try and parse it
      Date date = null;
      for(DateFormat d : df) {
        try {
          date = d.parse(timestamp);
          break;
        }
        catch (ParseException e) { /* ignore and try next format */ }
      }

      if (date == null) {
        out.print("Unable to parse timestamp '");
        out.print(timestamp);
        out.println("'.");
        out.println("Must be formatted in one of the following standards;");
        for (DateFormat d : df) {
          if (d instanceof SimpleDateFormat) {
            SimpleDateFormat sd = (SimpleDateFormat) d;
            out.print("  ");
            out.println(sd.toPattern());
          }
        }
        out.println("eg. 'feb 25, 2010 2:25am'");
      }
      else {
        rollbackPathToTime(path_name, date.getTime());
      }
    }
  }

  /**
   * Display a simple help screen.
   */
  private void showHelp() {
    out.println("List of commands:");
    out.println();
    out.println("refresh");
    out.println("  Updates the current state of the network on this tool.");
    out.println("check network");
    out.println("  Performs a check on the MckoiDDB network and reports any issues.");
    out.println("fix block availability");
    out.println("  Fixes any block availability issues.");
    out.println("rollback path [path name] to [timestamp]");
    out.println("  Reverts all changes made to the path since the given timestamp.");
    out.println("  'timestamp' may be a date format or 'yesterday', '39 hours', etc.");
    out.println();
    out.println("show network");
    out.println("  Displays the list of machines in the network schema.");
    out.println("show paths");
    out.println("  Displays information on all paths in the system.");
    out.println("show status");
    out.println("  Displays information on all paths in the system.");
    out.println("show free");
    out.println("  Displays an overview of all resources available on all machines in");
    out.println("  the network.");
    out.println();
//    out.println("add machine [address]");
//    out.println("  Adds a machine to the network schema.");
//    out.println("remove machine [address]");
//    out.println("  Removes a machine from the network schema.");
//    out.println();
    out.println("start role [block/manager/root] on [address]");
    out.println("  Starts a service role on the given machine.");
    out.println("stop role [block/manager/root] on [address]");
    out.println("  Stops a service role on the given machine.");
    out.println("move manager to [address]");
    out.println("  Moves the manager service to the given machine.");
    out.println();
    out.println("add path [consensus function] [path name] to [address of roots]");
    out.println("  Adds a new path name and consensus function on the given root ");
    out.println("  servers (the root servers parameter is a comma separated list).");
    out.println("add simple database [path name] to [address of root]");
    out.println("  Adds a simple database (com.mckoi.sdb.SimpleDatabase) path name ");
    out.println("  on the given root server.");
    out.println("remove path [path name]");
    out.println("  Removes a path from the root server that is managing it.");
    out.println("locate path [path name]");
    out.println("  Reports the root server address of the machine currently managing");
    out.println("  the given path.");
    out.println();
  }

  /**
   * Process a single instruction.
   */
  public void processCommand(String command) throws IOException {

    // Make the command lower case (to US locale).
    String lccmd = command;

    try {
      if (match(lccmd, "help")) {
        showHelp();
      }
      else if (match(lccmd, "show\\s+network") ||
               match(lccmd, "show\\s+schema")) {
        showNetwork();
      }
      else if (match(lccmd, "show\\s+paths")) {
        showPaths();
      }
      else if (match(lccmd, "show\\s+status")) {
        // Overview of current status of network
        showStatus();
      }
      else if (match(lccmd, "show\\s+free")) {
        showFree();
      }
      else if (match(lccmd, "show\\s+analytics")) {
        showAnalytics();
      }

      else if (match(lccmd, "show\\s+manager\\s+debug")) {
        showManagerDebug();
      }

      // path rollback operations,
      else if (match(lccmd, "rollback\\s+path\\s+(\\S+)\\s+(to\\s+)?(.*)")) {
        processPathRollback(lccmd);
      }


//      else if (match(lccmd, "show\\s+block_servers") ||
//               match(lccmd, "show\\s+block\\s+status")) {
//        // Diagramatically show all the block_servers stored over all the block
//        // servers
//        showBlocks(Long.MAX_VALUE);
//      }
      else if (match(lccmd, "refresh")) {
        out.println("Refreshing...");
        out.flush();
        network_profile.refresh();
        try {
          network_profile.refreshNetworkConfig();
        }
        catch (IOException e) {
          out.println("Unable to refresh network config due to IO error");
          e.printStackTrace(out);
        }
        out.println("Done.");
      }
//      else if (match(lccmd, "add\\s+machine\\s+(\\S+)")) {
//        addMachine(args[0]);
//      }
//      else if (match(lccmd, "remove\\s+machine\\s+(\\S+)")) {
//        removeMachine(args[0]);
//      }
      else if (match(lccmd, "start\\s+role\\s+(block|manager|root)\\s+(on\\s+)?(\\S+)")) {
        startRole(args[0], args[2]);
      }
      else if (match(lccmd, "stop\\s+role\\s+(block|manager|root)\\s+(on\\s+)?(\\S+)")) {
        stopRole(args[0], args[2]);
      }

      else if (match(lccmd, "fix\\s+block\\s+availability")) {
        fixBlockAvailability();
      }
      else if (match(lccmd, "check\\s+network")) {
        // Check network for incorrect registrations, and fix them.
        checkBlockStatus();
//        out.println("Function pending.");
      }
      else if (match(lccmd, "move\\s+manager\\s+(to\\s+)?(\\S+)")) {
        // PENDING: Moves the network manager role to another machine.
        out.println("Function pending.");
      }
      else if (match(lccmd, "add\\s+path\\s+(\\S+)\\s+(\\S+)\\s+to\\s+(.+)")) {
        // add path [consensus] [path_name] to [server]
        addPath(args[0], args[1], args[2]);
      }
      else if (match(lccmd, "add\\s+simple\\s+database\\s+(\\S+)\\s+to\\s+(.+)")) {
        // add path [consensus] [path_name] to [server]
        addPath("com.mckoi.sdb.SimpleDatabase", args[0], args[1]);
      }
      else if (match(lccmd, "remove\\s+path\\s+(\\S+)\\s+(from\\s+)?(\\S+)")) {
        removePath(args[0], args[2]);
      }
      else if (match(lccmd, "remove\\s+path\\s+(\\S+)")) {
        removePath(args[0], null);
      }
      else if (match(lccmd, "locate\\s+path\\s+(\\S+)")) {
        locatePath(args[0]);
      }

      else {
        out.println("Unknown command (use 'help' for a list of commands).");
      }

    }
    catch (IntErrException e) {
      // Represents a regular error that interrupted a command.
    }
    catch (NetworkAdminException e) {
      out.println("Error: " + e.getMessage());
//      e.printStackTrace(out);
      out.println("Operation aborted.");
    }

    out.flush();
  }

  /**
   * Process the input reader until the end of the stream is reached.
   */
  public void process() {
    try {
      while (true) {
        if (display_prompt) {
          out.print("MckoiDDB> ");
          out.flush();
        }
        String command = in.readLine();

        if (command == null ||
            command.equals("quit") ||
            command.equals("exit") ||
            command.equals("close")) {
          return;
        }
        if (!display_prompt) {
          out.print("MckoiDDB> ");
          out.println(command);
        }

        // Process the command,
        processCommand(command);
      }
    }
    catch (IOException e) {
      out.println("EXITED because of IO Exception: " + e.getMessage());
    }
  }


  static class IntErrException extends RuntimeException {
    IntErrException() {
      super();
    }
    IntErrException(String msg) {
      super(msg);
    }
  }

  interface BlockMapProcess {

    void managerProcess(BlockId block_id,
                        List<Long> manager_queried_servers,
                        List<Long> actual_known_sguids,
                        Map<Long, MachineProfile> sguid_to_address)
                                                 throws NetworkAdminException;

    void process(BlockId block_id,
          List<Long> available_sguids_containing_block_id,
          long[] available_block_servers,
          long min_threshold,
          Map<Long, MachineProfile> sguid_to_address)
                                                 throws NetworkAdminException;

  }

}
