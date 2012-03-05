/**
 * com.mckoi.network.MckoiNetworkPermission  Mar 4, 2012
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

import java.security.BasicPermission;

/**
 * A security permission for various network actions.
 *
 * @author Tobias Downer
 */

public class MckoiNetworkPermission extends BasicPermission {

  public MckoiNetworkPermission(String name) {
    super(name);
  }

  public MckoiNetworkPermission(String name, String actions) {
    super(name, actions);
  }

  // ----- Statics -----
  
  final static MckoiNetworkPermission CREATE_TCP_CONNECTOR =
                 new MckoiNetworkPermission("create_tcp_connector");

  final static MckoiNetworkPermission CREATE_PROXY_CONNECTOR =
                 new MckoiNetworkPermission("create_proxy_connector");


  //
  final static MckoiNetworkPermission CREATE_MCKOIDDB_CLIENT =
              new MckoiNetworkPermission("mckoiddbclient.create");
          
  //
  final static MckoiNetworkPermission CREATE_NETWORK_PROFILE =
              new MckoiNetworkPermission("networkprofile.create");

  //
  final static MckoiNetworkPermission QUERY_ALL_NETWORK_PATHS = 
              new MckoiNetworkPermission("mckoiddbclient.query_all_network_paths");

  
  final static MckoiNetworkPermission CREATE_NETWORK_CONFIG_RESOURCE =
              new MckoiNetworkPermission("networkconfigresource.create");
          


  final static MckoiNetworkPermission PARSE_DATA_ADDRESS =
              new MckoiNetworkPermission("dataaddress.parse");

}
