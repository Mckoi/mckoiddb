/*
 * Mckoi Software ( http://www.mckoi.com/ )
 * Copyright (C) 2000 - 2016  Tobias Downer.
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
 
package com.mckoi.odb;

import java.security.BasicPermission;

/**
 * Various permissions for access to sensitive information within the
 * Object Database model.
 *
 * @author Tobias Downer
 */

public class MckoiODBPermission extends BasicPermission {

  public MckoiODBPermission(String name) {
    super(name);
  }

  public MckoiODBPermission(String name, String actions) {
    super(name, actions);
  }

  // ----- Statics -----

  final static MckoiODBPermission ACCESS_DATA_ADDRESS =
                  new MckoiODBPermission("access_data_address");

  final static MckoiODBPermission CREATE_ROOT_ADDRESS =
                  new MckoiODBPermission("create_root_address");

}
