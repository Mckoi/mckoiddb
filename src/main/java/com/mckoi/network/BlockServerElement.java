/**
 * com.mckoi.network.BlockServerElement  Dec 13, 2008
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

/**
 * A status element of a block server in the network.
 *
 * @author Tobias Downer
 */

public class BlockServerElement {

  /**
   * The address of the block server.
   */
  private final ServiceAddress address;

  /**
   * The status of the block server.
   */
  private final String status;

  /**
   * Constructs the element.
   */
  public BlockServerElement(ServiceAddress address, String status) {
    this.address = address;
    this.status = status;
  }

  /**
   * Returns the address of the block server.
   */
  public ServiceAddress getAddress() {
    return address;
  }

  /**
   * Returns true if the status report of the server from this element says
   * that the server is UP.
   */
  public boolean isStatusUp() {
    return status.startsWith("U");
  }

}
