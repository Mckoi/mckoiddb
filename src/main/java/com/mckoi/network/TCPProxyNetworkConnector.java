/**
 * com.mckoi.network.TCPProxyNetworkConnector  Jul 18, 2009
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
import java.net.InetAddress;
import java.net.Socket;

/**
 * A connector that enables communication to a MckoiDDB network via a proxy
 * server. This is useful for setting up a system where a client can connect
 * to a network of MckoiDDB nodes remotely. Using this connector, when the
 * client requests information from a server the request is sent to the proxy
 * which performs the command on the the clients behalf on the network, and
 * the result is returned to the client.
 * <p>
 * Note that all network communication using this connector is not encrypted
 * so it is not appropriate for use over a public network or the internet.
 * This type of connection could be made secure if tunnelled through a secure
 * channel (such as ssh).
 *
 * @author Tobias Downer
 */

class TCPProxyNetworkConnector extends ProxyNetworkConnector {

  /**
   * Constructs the connector.
   */
  public TCPProxyNetworkConnector(String net_password) {
    super(net_password);
  }

  /**
   * Attempts to connect to the proxy server.
   */
  public void connect(InetAddress proxy_address, int proxy_port) {
    try {
      Socket socket = new Socket(proxy_address, proxy_port);
      super.connect(socket.getInputStream(), socket.getOutputStream());
    }
    catch (IOException e) {
      throw new RuntimeException("IO Error", e);
    }
  }

}
