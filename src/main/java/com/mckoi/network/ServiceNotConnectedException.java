/**
 * com.mckoi.network.ServiceNotConnectedException  Jun 21, 2010
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

/**
 * An exception indicating a function on a service failed because the
 * service has not yet completed connection to the network (the service either
 * is going through the initialization process or not enough services are
 * available on the network).
 *
 * @author Tobias Downer
 */

public class ServiceNotConnectedException extends RuntimeException {

  public ServiceNotConnectedException() {
    super();
  }

  public ServiceNotConnectedException(String message) {
    super(message);
  }

  public ServiceNotConnectedException(Throwable cause) {
    super(cause);
  }

  public ServiceNotConnectedException(String message, Throwable cause) {
    super(message, cause);
  }

}
