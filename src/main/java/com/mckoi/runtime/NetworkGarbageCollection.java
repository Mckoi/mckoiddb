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

import com.mckoi.data.NodeReference;
import com.mckoi.network.*;
import com.mckoi.util.CommandLine;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.UnknownHostException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A tool that reclaims file system resources by rewriting block files so that
 * they no longer contain nodes that are not connected to the currently
 * available paths. This operation may take a very long time to complete but
 * can be safely performed in the background.
 * <p>
 * When a block is rewritten it may take some time until the original block
 * file is deleted, therefore file system usage may increase temporarily until
 * the old block files are deleted.
 *
 * @author Tobias Downer
 */

public class NetworkGarbageCollection {

  public static void main(String[] args) {
    try {

      // Turn logging off for the GC app (don't worry, the servers will still
      // log events that happen).
      Logger logger = Logger.getLogger("com.mckoi.network.Log");
      logger.setLevel(Level.OFF);
      
      StringWriter str_out = new StringWriter();
      PrintWriter pout = new PrintWriter(str_out, true);

      // Output standard info
      System.out.println(MckoiDDBVerInfo.displayVersionString());
      System.out.println(MckoiDDBVerInfo.license_message);

      String network_conf_arg = null;
      String client_conf_arg = null;

      CommandLine command_line = new CommandLine(args);
      boolean failed = false;
      try {
        network_conf_arg =
                  command_line.switchArgument("-netconfig", "network.conf");
        client_conf_arg =
                  command_line.switchArgument("-clientconfig", "client.conf");
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
      if (client_conf_arg == null) {
        pout.println("Error, no client configuration file given.");
        failed = true;
      }

      if (!failed) {
        
        // Process the network config argument,
        NetworkConfigResource network_conf_resource =
                                NetworkConfigResource.parse(network_conf_arg);

        // Load from client configuration file,
        File client_config_file = new File(client_conf_arg);

        MckoiDDBClient client =
                          MckoiDDBClientUtils.connectTCP(client_config_file);

        // Init the network profile,
        NetworkProfile net_profile = client.getNetworkProfile(null);
        net_profile.setNetworkConfiguration(network_conf_resource);

        // Refresh the network profile,
        net_profile.refresh();

        // Path discovery,
        String[] all_paths = client.queryAllNetworkPaths();

        StringBuilder path_line = new StringBuilder();
        if (all_paths.length > 0) {
          for (int i = 0; i < all_paths.length - 1; ++i) {
            path_line.append(all_paths[i]);
            path_line.append(", ");
          }
          path_line.append(all_paths[all_paths.length - 1]);
        }

        System.out.println();
        System.out.println("Discovered " + all_paths.length + " paths.");
        System.out.println("Paths: " + path_line);
        System.out.println();

//        // Create a transaction for managing GC data,
//        // NOTE: Should we push this data out on a system path so the GC
//        //   process can be incremental?
//        KeyObjectTransaction transaction = client.createEmptyTransaction();
//
//        // Create the discovered node set object,
//        Key nodeset_key = new Key((short) 0, 0, 10);
//        transaction.getDataFile(nodeset_key, 'w');

        // Create a Java heap node set object,
        HeapDiscoveredNodeSet discovered_node_set = new HeapDiscoveredNodeSet();

        // Time now,
        long time_now = System.currentTimeMillis();
        // Past point (4 days ago),
        long time_to_preserve = time_now - (14L * 24L * 60L * 60L * 1000L);

        // Get the historical data address from each path,
        for (String path : all_paths) {

          System.out.println("Processing: " + path);

          // The current path root (make sure to include this)
          DataAddress current_root = client.getCurrentSnapshot(path);
          // Historical roots over the past 4 days to preserve,
          DataAddress[] roots =
             client.getHistoricalSnapshots(path,
                                           time_to_preserve, Long.MAX_VALUE);

          // Make a list of roots with the current root as most recent.
          DataAddress[] roots_copy = new DataAddress[roots.length + 1];
          System.arraycopy(roots, 0, roots_copy, 0, roots.length);
          roots_copy[roots.length] = current_root;
          
          // Sort the roots to preserve,
          System.out.print("#(" + roots_copy.length + ")");
          Arrays.sort(roots_copy);

          // Discover nodes to preserve for each root node in the path,
          for (DataAddress root_node : roots_copy) {
            System.out.print(".");
            client.discoverNodesInSnapshot(pout,
                                           root_node, discovered_node_set);
          }
          System.out.println();

        }

        System.out.println(
                "Preserve node count = " + discovered_node_set.getNodeCount());

        // Blocks being preserved,
        HashMap<BlockId, List<NodeReference>> touched_blocks = new HashMap();
        for (NodeReference node : discovered_node_set.nodes()) {

          // Skip nodes that are special and or not in memory,
          if (!node.isSpecial() && !node.isInMemory()) {
            // The block id,
            BlockId block_id = DataAddress.getBlockIdFrom(node);

            List<NodeReference> block_nodes = touched_blocks.get(block_id);
            // Put the association in the map
            if (block_nodes == null) {
              block_nodes = new ArrayList();
              touched_blocks.put(block_id, block_nodes);
            }

            // Add the node reference,
            block_nodes.add(node);
          }

        }

        // We now have a set of blocks that need to be preserved, and also the
        // node references in the blocks that we wish to preserve.

        // Dispatch the cleanup action to the block servers,
        for (BlockId block_id : touched_blocks.keySet()) {

          // The block servers that manage this block_id,
          ServiceAddress[] block_servers =
                                      net_profile.getBlockServerList(block_id);

          // Tell these block servers to preserve only the given nodes in
          // the block,
          List<NodeReference> nodes_to_preserve = touched_blocks.get(block_id);
          for (ServiceAddress block_server : block_servers) {
            net_profile.preserveNodesInBlock(block_server,
                                             block_id, nodes_to_preserve);
          }

        }

//        // Blocks touched,
//        System.out.println("Touched block count = " + touched_blocks.size());
//
//        for (BlockId block_id : touched_blocks.keySet()) {
//          List<NodeReference> block_nodes = touched_blocks.get(block_id);
//          System.out.println("block_id = " + block_id);
//          System.out.println("block_nodes = " + block_nodes);
//          System.out.println("sz = " + block_nodes.size());
//        }

      }

      if (failed) {
        System.out.println();
        System.out.println("Failed for following reason;");
        System.out.println(str_out.toString());
      }

    }
    catch (UnknownHostException e) {
      e.printStackTrace(System.err);
    }
    catch (IOException e) {
      e.printStackTrace(System.err);
    }
    catch (NetworkAdminException e) {
      e.printStackTrace(System.err);
    }
  }

  
  
  /**
   * An implementation of DiscoveredNodeSet where the node references are
   * stored in a TreeSet.
   */
  private static class HeapDiscoveredNodeSet implements DiscoveredNodeSet {

    private TreeSet<NodeReference> tree_set;

    HeapDiscoveredNodeSet() {
      tree_set = new TreeSet();
    }

    @Override
    public boolean add(NodeReference node_ref) {
      return tree_set.add(node_ref);
    }

    public int getNodeCount() {
      return tree_set.size();
    }

    public SortedSet<NodeReference> nodes() {
      return Collections.unmodifiableSortedSet(tree_set);
    }

  }

}
