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
