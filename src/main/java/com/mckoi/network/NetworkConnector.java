/**
 * com.mckoi.network.NetworkConnector  Nov 25, 2008
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
 * An object used by clients to connect to services in the Mckoi network.
 *
 * @author Tobias Downer
 */

public interface NetworkConnector {

  /**
   * Stops this connector, invalidating it and putting any resources it
   * used up for GC.
   */
  public void stop();
  
  /**
   * Connects to the instance administration component of the given address.
   */
  public MessageProcessor connectInstanceAdmin(ServiceAddress address);
  
  /**
   * Connects to a block server at the given address.
   */
  public MessageProcessor connectBlockServer(ServiceAddress address);

  /**
   * Connects to a manager server at the given address.
   */
  public MessageProcessor connectManagerServer(ServiceAddress address);

  /**
   * Connects to a root server at the given address.
   */
  public MessageProcessor connectRootServer(ServiceAddress address);
  
}
