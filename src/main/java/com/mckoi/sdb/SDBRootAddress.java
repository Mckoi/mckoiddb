/**
 * com.mckoi.sdb.SDBRootAddress  Jul 26, 2009
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

package com.mckoi.sdb;

import com.mckoi.network.DataAddress;

/**
 * An object that encapsulates a root node address of a transaction from an
 * SDBSession.
 *
 * @author Tobias Downer
 */

// NOT SDBTrustedObject. Exposes SDBSession.
public class SDBRootAddress {

  private final SDBSession session;
  private final DataAddress data_address;

  SDBRootAddress(SDBSession session, DataAddress data_address) {
    this.data_address = data_address;
    this.session = session;
  }

  /**
   * Returns the SDBSession object for this root node address.
   */
  SDBSession getSession() {
    return session;
  }

  DataAddress getDataAddress() {
    return data_address;
  }

}
