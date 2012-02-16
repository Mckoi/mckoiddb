/**
 * com.mckoi.network.PathInfo  Jun 22, 2010
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

import java.io.IOException;

/**
 * Encapsulates information about a path in the system, including the service
 * that is the root leader, the root servers, the name of the path, the
 * consensus function, and the version number (for caching).
 *
 * @author Tobias Downer
 */

public class PathInfo {

  private final String path_name;
  private final String consensus_function;
  private final int version_number;
  private final ServiceAddress root_leader;
  private final ServiceAddress[] root_servers;

  PathInfo(String path_name, String consensus_function, int version_number,
           ServiceAddress root_leader, ServiceAddress[] root_servers) {
    this.path_name = path_name;
    this.consensus_function = consensus_function;
    this.version_number = version_number;
    this.root_leader = root_leader;
    this.root_servers = root_servers;

    // Check the root leader in the root servers list,
    boolean found = false;
    for (int i = 0; i < root_servers.length; ++i) {
      if (root_servers[i].equals(root_leader)) {
        found = true;
        break;
      }
    }
    // Error if not found,
    if (!found) {
      throw new RuntimeException("Leader not found in root servers list");
    }
  }

  public String getPathName() {
    return path_name;
  }

  public String getConsensusFunction() {
    return consensus_function;
  }

  public int getVersionNumber() {
    if (version_number < 0) {
      throw new RuntimeException("Negative version number");
    }
    return version_number;
  }

  public ServiceAddress getRootLeader() {
    return root_leader;
  }

  public ServiceAddress[] getRootServers() {
    return root_servers.clone();
  }

  /**
   * Returns a parsable string of this object, minus the path name.
   */
  public String formatString() {
    StringBuilder b = new StringBuilder();
    b.append(consensus_function);
    b.append(",");
    b.append(version_number);
    // Output the root servers list
    int sz = root_servers.length;
    for (int i = 0; i < sz; ++i) {
      b.append(",");
      ServiceAddress addr = root_servers[i];
      // The root leader has a "*" prefix
      if (addr.equals(root_leader)) {
        b.append("*");
      }
      b.append(addr.formatString());
    }
    return b.toString();
  }

  /**
   * Parses a formated path info string.
   */
  public static PathInfo parseString(String path_name, String str_format) {
    String[] parts = str_format.split(",");

    try {
      String consensus_function = parts[0];
      int version_number = Integer.parseInt(parts[1]);
      int sz = parts.length - 2;
      ServiceAddress root_leader = null;
      ServiceAddress[] servers = new ServiceAddress[sz];

      for (int i = 0; i < sz; ++i) {
        boolean is_leader = false;
        String item = parts[i + 2];
        if (item.startsWith("*")) {
          item = item.substring(1);
          is_leader = true;
        }
        ServiceAddress addr = ServiceAddress.parseString(item);
        servers[i] = addr;
        if (is_leader) {
          root_leader = addr;
        }
      }

      // Return the PathInfo object,
      return new PathInfo(path_name, consensus_function, version_number,
                          root_leader, servers);

    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Comparison.
   */
  private static boolean listsEqual(
                             ServiceAddress[] list1, ServiceAddress[] list2) {

    if (list1 == null && list2 == null) {
      return true;
    }
    if (list1 == null || list2 == null) {
      return false;
    }
    // Both non-null
    int sz = list1.length;
    if (sz != list2.length) {
      return false;
    }
    for (int i = 0; i < sz; ++i) {
      ServiceAddress addr = list1[i];
      boolean found = false;
      for (int n = 0; n < sz; ++n) {
        if (list2[n].equals(addr)) {
          found = true;
          break;
        }
      }
      if (!found) {
        return false;
      }
    }
    for (int i = 0; i < sz; ++i) {
      ServiceAddress addr = list2[i];
      boolean found = false;
      for (int n = 0; n < sz; ++n) {
        if (list1[n].equals(addr)) {
          found = true;
          break;
        }
      }
      if (!found) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean equals(Object ob) {
    if (ob == null) {
      return false;
    }
    if (!(ob instanceof PathInfo)) {
      return false;
    }
    PathInfo that_path_info = (PathInfo) ob;
    if (path_name.equals(that_path_info.path_name) &&
        consensus_function.equals(that_path_info.consensus_function) &&
        version_number == that_path_info.version_number &&
        root_leader.equals(that_path_info.root_leader) &&
        listsEqual(root_servers, that_path_info.root_servers)) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int servers_hashcode = 0;
    for (ServiceAddress addr : root_servers) {
      servers_hashcode += addr.hashCode();
    }
    return path_name.hashCode() + consensus_function.hashCode() + version_number + root_leader.hashCode() + servers_hashcode;
  }

}
