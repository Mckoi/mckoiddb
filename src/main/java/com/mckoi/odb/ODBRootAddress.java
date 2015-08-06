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

package com.mckoi.odb;

import com.mckoi.network.DataAddress;

/**
 * An object that encapsulates a root node address of a transaction from an
 * ODBSession.
 *
 * @author Tobias Downer
 */

public class ODBRootAddress {

  private final ODBSession session;
  private final DataAddress data_address;

  ODBRootAddress(ODBSession session, DataAddress data_address) {
    this.data_address = data_address;
    this.session = session;
  }

  /**
   * Returns the ODBSession object for this root node address.
   */
  ODBSession getSession() {
    return session;
  }

  DataAddress getDataAddress() {
    return data_address;
  }

}

